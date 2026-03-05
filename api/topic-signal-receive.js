const crypto = require('crypto');
const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const VALID_SOURCE_TYPES = new Set(['rss', 'webhook', 'chat_yes', 'chat_specify']);

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

function normalizeUrl(value) {
  const raw = cleanText(value, 2000);
  if (!raw) return '';
  try {
    const url = new URL(raw);
    url.hash = '';
    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content'].forEach((k) => url.searchParams.delete(k));
    return url.toString();
  } catch (_) {
    return raw;
  }
}

function getSixHourBucket(date = new Date()) {
  const bucket = new Date(date);
  bucket.setUTCMinutes(0, 0, 0);
  const hour = bucket.getUTCHours();
  bucket.setUTCHours(hour - (hour % 6));
  return bucket.toISOString();
}

function buildDedupeKey({
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

async function emitSignalReceivedEvent(signal) {
  const endpoint = cleanText(process.env.INNGEST_EVENT_URL || '', 1200);
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
          trigger: 'topic_signal_receive'
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

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') {
    res.setHeader('Allow', ['POST']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const personaId = cleanText(req.body?.personaId || req.body?.engineId || '', 255);
  const sourceType = cleanText(req.body?.sourceType || '', 30).toLowerCase();
  const sourceName = cleanText(req.body?.sourceName || '', 240);
  const sourceUrl = normalizeUrl(req.body?.sourceUrl || req.body?.url || '');
  const externalId = cleanText(req.body?.externalId || '', 500);
  const title = cleanText(req.body?.title || '', 500);
  const snippet = cleanText(req.body?.snippet || req.body?.description || '', 4000);
  const sectionHint = cleanText(req.body?.sectionHint || req.body?.section || '', 80).toLowerCase();
  const sessionHash = cleanText(req.body?.sessionHash || '', 128);
  const metadata = req.body?.metadata && typeof req.body.metadata === 'object' ? req.body.metadata : {};

  if (!personaId) return res.status(400).json({ error: 'personaId is required' });
  if (!VALID_SOURCE_TYPES.has(sourceType)) {
    return res.status(400).json({ error: `sourceType must be one of: ${Array.from(VALID_SOURCE_TYPES).join(', ')}` });
  }
  if (!title) return res.status(400).json({ error: 'title is required' });

  const bucketIso = getSixHourBucket();
  const dedupeKey = buildDedupeKey({
    personaId,
    sourceType,
    sourceUrl,
    externalId,
    title,
    sectionHint,
    bucketIso
  });

  try {
    const sql = neon(process.env.DATABASE_URL);
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
        session_hash,
        created_at,
        updated_at
      )
      VALUES (
        ${personaId},
        ${sourceType},
        ${sourceName || null},
        ${sourceUrl || null},
        ${externalId || null},
        ${title},
        ${snippet || null},
        ${sectionHint || null},
        ${metadata}::jsonb,
        ${dedupeKey},
        'pending',
        'none',
        'pending_review',
        ${sessionHash || null},
        NOW(),
        NOW()
      )
      ON CONFLICT (persona_id, dedupe_key) DO NOTHING
      RETURNING
        id,
        persona_id as "personaId",
        source_type as "sourceType",
        title,
        action,
        next_step as "nextStep",
        dedupe_key as "dedupeKey",
        created_at as "createdAt"
    `;

    if (inserted[0]) {
      const eventResult = await emitSignalReceivedEvent(inserted[0]);
      return res.status(200).json({
        ok: true,
        inserted: true,
        signal: inserted[0],
        event: eventResult
      });
    }

    const existing = await sql`
      SELECT
        id,
        persona_id as "personaId",
        source_type as "sourceType",
        title,
        action,
        next_step as "nextStep",
        dedupe_key as "dedupeKey",
        created_at as "createdAt"
      FROM topic_signals
      WHERE persona_id = ${personaId}
        AND dedupe_key = ${dedupeKey}
      LIMIT 1
    `;

    return res.status(200).json({
      ok: true,
      inserted: false,
      deduped: true,
      signal: existing[0] || null
    });
  } catch (error) {
    console.error('Topic signal receive error:', error);
    if (String(error?.message || '').toLowerCase().includes('topic_signals')) {
      return res.status(500).json({
        error: 'Topic signal table missing',
        details: 'Run migration 20260304_05_gatekeeper_signals.sql first.'
      });
    }
    return res.status(500).json({ error: 'Failed to receive topic signal' });
  }
};
