const crypto = require('crypto');
const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { normalizeSignal, normalizeUrl } = require('./_topic-engine-workflow');

function cleanText(value, max = 1000) {
  return String(value || '').trim().slice(0, max);
}

function normalizeText(value, max = 500) {
  return cleanText(value, max)
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function getSixHourBucket(date = new Date()) {
  const bucket = new Date(date);
  bucket.setUTCMinutes(0, 0, 0);
  const hour = bucket.getUTCHours();
  bucket.setUTCHours(hour - (hour % 6));
  return bucket.toISOString();
}

function buildTopicSignalDedupeKey({
  personaId,
  sourceType,
  sourceUrl,
  externalId,
  title,
  sectionHint,
  bucketIso
}) {
  const canonical = [
    normalizeText(personaId, 255),
    normalizeText(sourceType, 30),
    normalizeUrl(sourceUrl) || normalizeText(externalId, 300),
    normalizeText(title, 320),
    normalizeText(sectionHint, 80),
    cleanText(bucketIso, 40)
  ].join('::');
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

function getRequestOrigin(req) {
  const host = cleanText(req?.headers?.['x-forwarded-host'] || req?.headers?.host || '', 1000);
  if (!host) return '';
  const forwardedProto = cleanText(req?.headers?.['x-forwarded-proto'] || '', 50).toLowerCase();
  const protocol = forwardedProto || (host.startsWith('localhost') || host.startsWith('127.0.0.1') ? 'http' : 'https');
  return `${protocol}://${host}`;
}

function resolveInngestEventEndpoint(req) {
  const configured = cleanText(process.env.INNGEST_EVENT_URL || '', 1200);
  if (configured) return configured;
  const origin = getRequestOrigin(req);
  if (!origin) return '';
  return `${origin}/api/inngest`;
}

async function emitSignalReceivedEvent(signal, req) {
  const endpoint = resolveInngestEventEndpoint(req);
  if (!endpoint) return { attempted: false, sent: false, reason: 'missing_inngest_event_url' };

  const key = cleanText(process.env.INNGEST_EVENT_KEY || '', 600);
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(key ? { Authorization: `Bearer ${key}` } : {})
      },
      body: JSON.stringify({
        name: 'signal.received',
        data: {
          signalId: Number(signal?.id || 0),
          personaId: cleanText(signal?.personaId || '', 255),
          sourceType: cleanText(signal?.sourceType || '', 30),
          trigger: 'topic_engine_trigger'
        }
      })
    });
    return {
      attempted: true,
      sent: response.ok,
      status: response.status
    };
  } catch (error) {
    return {
      attempted: true,
      sent: false,
      reason: cleanText(error?.message || 'event_send_failed', 500)
    };
  }
}

async function persistSignalDispatchStatus(sql, signalId, eventResult) {
  const attempted = eventResult?.attempted === true;
  const sent = eventResult?.sent === true;
  const status = Number.isFinite(Number(eventResult?.status)) ? Number(eventResult.status) : null;
  const reason = cleanText(eventResult?.reason || '', 500) || null;
  const lastAttemptAt = new Date().toISOString();
  const needsRetry = !sent;

  await sql`
    UPDATE topic_signals
    SET
      metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{eventDispatch}',
        jsonb_build_object(
          'attempted', ${attempted},
          'sent', ${sent},
          'status', ${status},
          'reason', ${reason},
          'needsRetry', ${needsRetry},
          'lastAttemptAt', ${lastAttemptAt},
          'retryCount',
            (
              CASE
                WHEN COALESCE(metadata->'eventDispatch'->>'retryCount', '') ~ '^[0-9]+$'
                  THEN (metadata->'eventDispatch'->>'retryCount')::int
                ELSE 0
              END
              +
              CASE WHEN ${needsRetry} THEN 1 ELSE 0 END
            )
        ),
        true
      ),
      action = CASE
        WHEN ${needsRetry} THEN 'pending'
        ELSE action
      END,
      review_decision = CASE
        WHEN ${needsRetry} THEN 'pending_review'
        ELSE review_decision
      END,
      updated_at = NOW()
    WHERE id = ${signalId}
  `;
}

async function ensureTopicSignalTables(sql) {
  const requirements = [
    { table: 'personas', columns: ['id'] },
    {
      table: 'topic_signals',
      columns: ['persona_id', 'source_type', 'source_name', 'source_url', 'external_id', 'title', 'snippet', 'section_hint', 'metadata', 'dedupe_key', 'action', 'next_step', 'review_decision']
    }
  ];

  for (const requirement of requirements) {
    const tableRows = await sql`SELECT to_regclass(${`public.${requirement.table}`}) as name`;
    if (!tableRows[0]?.name) {
      const error = new Error(`Schema not ready: missing table ${requirement.table}`);
      error.statusCode = 503;
      throw error;
    }

    const columnRows = await sql`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = ${requirement.table}
    `;
    const existingColumns = new Set(columnRows.map((row) => String(row.column_name || '')));
    const missingColumns = requirement.columns.filter((columnName) => !existingColumns.has(columnName));
    if (missingColumns.length) {
      const error = new Error(`Schema not ready: missing columns on ${requirement.table}: ${missingColumns.join(', ')}`);
      error.statusCode = 503;
      throw error;
    }
  }
}

function requireWebhookOrAdmin(req, res) {
  const expectedSecret = String(process.env.TOPIC_ENGINE_WEBHOOK_SECRET || '').trim();
  if (expectedSecret) {
    const provided = String(
      req.headers['x-topic-engine-secret'] ||
      req.query.secret ||
      (req.body && req.body.secret) ||
      ''
    ).trim();
    if (!provided || provided !== expectedSecret) {
      res.status(401).json({ error: 'Unauthorized' });
      return false;
    }
    return true;
  }
  return requireAdmin(req, res);
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }
  if (!requireWebhookOrAdmin(req, res)) return;

  const personaId = String(req.body?.personaId || req.body?.engineId || '').trim();
  if (!personaId) {
    return res.status(400).json({ error: 'personaId is required' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureTopicSignalTables(sql);
    const signal = normalizeSignal(req.body?.signal || req.body || {});
    if (!signal.title) {
      return res.status(400).json({ ok: false, error: 'missing_signal_title' });
    }
    const sourceType = 'webhook';
    const sourceUrl = normalizeUrl(signal.url || signal.sourceUrl || '');
    const externalId = cleanText(req.body?.externalId || sourceUrl || '', 500);
    const sectionHint = cleanText(req.body?.sectionHint || req.body?.section || 'local', 80).toLowerCase();
    const bucketIso = getSixHourBucket();
    const dedupeKey = buildTopicSignalDedupeKey({
      personaId,
      sourceType,
      sourceUrl,
      externalId,
      title: signal.title,
      sectionHint,
      bucketIso
    });
    const metadata = signal.metadata && typeof signal.metadata === 'object'
      ? { ...signal.metadata, triggerMode: 'event' }
      : { triggerMode: 'event' };
    const inserted = await sql`
      INSERT INTO topic_signals (
        persona_id,
        source_type,
        source_name,
        source_url,
        external_id,
        title,
        snippet,
        section_hint,
        metadata,
        dedupe_key,
        action,
        next_step,
        review_decision,
        created_at,
        updated_at
      )
      VALUES (
        ${personaId},
        ${sourceType},
        ${cleanText(signal.sourceName || req.body?.sourceName || '', 240) || null},
        ${sourceUrl || null},
        ${externalId || null},
        ${cleanText(signal.title || '', 500)},
        ${cleanText(signal.snippet || '', 4000) || null},
        ${sectionHint || null},
        ${metadata}::jsonb,
        ${dedupeKey},
        'pending',
        'none',
        'pending_review',
        NOW(),
        NOW()
      )
      ON CONFLICT (persona_id, dedupe_key) DO NOTHING
      RETURNING id, persona_id as "personaId", source_type as "sourceType", title, dedupe_key as "dedupeKey"
    `;

    let result;
    if (inserted[0]) {
      const eventResult = await emitSignalReceivedEvent(inserted[0], req);
      await persistSignalDispatchStatus(sql, inserted[0].id, eventResult);
      if (!eventResult.sent) {
        result = {
          ok: false,
          inserted: true,
          deduped: false,
          reason: 'signal_received_event_failed',
          signal: inserted[0],
          event: eventResult
        };
      } else {
        result = {
          ok: true,
          inserted: true,
          deduped: false,
          signal: inserted[0],
          event: eventResult
        };
      }
    } else {
      const existing = await sql`
        SELECT
          id,
          persona_id as "personaId",
          source_type as "sourceType",
          title,
          dedupe_key as "dedupeKey",
          created_at as "createdAt"
        FROM topic_signals
        WHERE persona_id = ${personaId}
          AND dedupe_key = ${dedupeKey}
        LIMIT 1
      `;
      result = {
        ok: true,
        inserted: false,
        deduped: true,
        reason: 'duplicate_signal',
        signal: existing[0] || null
      };
    }

    if (!result.ok) {
      return res.status(400).json({ ok: false, error: result.reason || 'trigger_failed', details: result });
    }
    return res.status(200).json({
      ok: true,
      triggerMode: 'event',
      result
    });
  } catch (error) {
    console.error('Topic engine event trigger error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Failed to process topic engine event trigger' });
  }
};
