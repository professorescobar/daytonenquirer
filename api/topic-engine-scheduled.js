const crypto = require('crypto');
const { neon } = require('@neondatabase/serverless');
const Parser = require('rss-parser');
const { requireAdmin } = require('./_admin-auth');
const { ensureTopicEngineTables, normalizeUrl } = require('./_topic-engine-workflow');

const parser = new Parser({
  timeout: 12000,
  headers: {
    'User-Agent': 'DaytonEnquirerTopicEngineBot/1.0 (+https://thedaytonenquirer.com)'
  }
});

const DEFAULT_FEEDS_BY_SECTION = {
  local: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  national: [
    'https://news.google.com/rss/search?q=national+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  world: [
    'https://news.google.com/rss/search?q=world+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  business: [
    'https://news.google.com/rss/search?q=stock+market+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  sports: [
    'https://news.google.com/rss/search?q=dayton+ohio+sports+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  health: [
    'https://news.google.com/rss/search?q=health+news+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  entertainment: [
    'https://news.google.com/rss/search?q=dayton+entertainment+events+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  technology: [
    'https://news.google.com/rss/search?q=technology+innovation+when:2d&hl=en-US&gl=US&ceid=US:en'
  ]
};

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
          trigger: 'topic_engine_scheduled'
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

async function ingestScheduledSignal(sql, req, {
  personaId,
  section,
  signal
}) {
  const sourceType = 'rss';
  const sourceUrl = normalizeUrl(signal.url || '');
  const externalId = cleanText(signal.externalId || sourceUrl || '', 500);
  const sectionHint = cleanText(section || 'local', 80).toLowerCase();
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
    ? { ...signal.metadata, triggerMode: 'scheduled' }
    : { triggerMode: 'scheduled' };

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
      ${cleanText(signal.sourceName || '', 240) || null},
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
    RETURNING
      id,
      persona_id as "personaId",
      source_type as "sourceType"
  `;

  if (!inserted[0]) {
    return { ok: true, inserted: false, deduped: true };
  }

  const eventResult = await emitSignalReceivedEvent(inserted[0], req);
  await persistSignalDispatchStatus(sql, inserted[0].id, eventResult);
  if (!eventResult.sent) {
    return {
      ok: false,
      inserted: true,
      deduped: false,
      reason: 'signal_received_event_failed',
      event: eventResult
    };
  }
  return {
    ok: true,
    inserted: true,
    deduped: false,
    event: eventResult
  };
}

async function loadRetryableScheduledSignals(sql, limit) {
  const rows = await sql`
    SELECT
      id,
      persona_id as "personaId",
      source_type as "sourceType"
    FROM topic_signals
    WHERE source_type = 'rss'
      AND (
        CASE
          WHEN lower(COALESCE(metadata->'eventDispatch'->>'needsRetry', '')) IN ('true', 'false')
            THEN (metadata->'eventDispatch'->>'needsRetry')::boolean
          ELSE false
        END
      ) = true
      AND action = 'pending'
      AND review_decision = 'pending_review'
    ORDER BY updated_at ASC, id ASC
    LIMIT ${limit}
  `;
  return rows;
}

async function replayRetryableScheduledSignals(sql, req, replayLimit) {
  const retryRows = await loadRetryableScheduledSignals(sql, replayLimit);
  let replayAttempted = 0;
  let replaySucceeded = 0;
  let replayFailed = 0;

  for (const row of retryRows) {
    const payload = {
      id: Number(row.id || 0),
      personaId: cleanText(row.personaId || '', 255),
      sourceType: cleanText(row.sourceType || '', 30)
    };
    if (!payload.id || !payload.personaId) continue;
    replayAttempted += 1;
    const eventResult = await emitSignalReceivedEvent(payload, req);
    await persistSignalDispatchStatus(sql, payload.id, eventResult);
    if (eventResult.sent) replaySucceeded += 1;
    else replayFailed += 1;
  }

  return {
    scanned: retryRows.length,
    attempted: replayAttempted,
    succeeded: replaySucceeded,
    failed: replayFailed
  };
}

function parsePositiveInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

async function getScheduledPersonas(sql, maxEngines) {
  const rows = await sql`
    SELECT
      id,
      COALESCE(NULLIF(trim(activation_mode), ''), 'both') as "activationMode",
      COALESCE(NULLIF(trim(section), ''), 'local') as section
    FROM personas
    WHERE COALESCE(NULLIF(trim(activation_mode), ''), 'both') IN ('scheduled', 'both')
    ORDER BY id ASC
    LIMIT ${maxEngines}
  `;
  return rows;
}

async function getFeedsForPersona(sql, personaId, section) {
  const configured = await sql`
    SELECT feed_url as "feedUrl", COALESCE(source_name, '') as "sourceName"
    FROM topic_engine_feeds
    WHERE persona_id = ${personaId}
      AND enabled = true
    ORDER BY priority ASC, id ASC
    LIMIT 50
  `;
  if (configured.length) {
    return configured.map((row) => ({
      feedUrl: normalizeUrl(row.feedUrl),
      sourceName: row.sourceName || ''
    })).filter((item) => item.feedUrl);
  }
  const fallbacks = DEFAULT_FEEDS_BY_SECTION[section] || [];
  return fallbacks.map((feedUrl) => ({ feedUrl: normalizeUrl(feedUrl), sourceName: section }));
}

function itemToSignal(item, sourceName, feedUrl) {
  return {
    title: String(item?.title || '').trim(),
    url: normalizeUrl(item?.link || item?.guid || ''),
    snippet: String(item?.contentSnippet || item?.summary || item?.content || '').trim().slice(0, 2000),
    sourceName: sourceName || '',
    sourceUrl: feedUrl || '',
    publishedAt: item?.isoDate || item?.pubDate || null,
    metadata: {
      feedTitle: item?.creator || '',
      categories: Array.isArray(item?.categories) ? item.categories.slice(0, 12) : []
    }
  };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (!['POST', 'GET'].includes(req.method)) {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const input = req.method === 'POST' ? (req.body || {}) : (req.query || {});
  const maxEngines = parsePositiveInt(input.maxEngines, 50, 1, 500);
  const maxFeedsPerEngine = parsePositiveInt(input.maxFeedsPerEngine, 5, 1, 50);
  const maxItemsPerFeed = parsePositiveInt(input.maxItemsPerFeed, 5, 1, 50);
  const replayRetryLimit = parsePositiveInt(input.replayRetryLimit, 100, 0, 2000);

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureTopicEngineTables(sql, { profile: 'ingestion_scheduled' });

    const personas = await getScheduledPersonas(sql, maxEngines);
    const summary = {
      enginesScanned: personas.length,
      feedsScanned: 0,
      signalsSeen: 0,
      inserted: 0,
      deduped: 0,
      skipped: 0,
      errors: 0,
      eventDispatchFailed: 0,
      replayRetryScanned: 0,
      replayRetryAttempted: 0,
      replayRetrySucceeded: 0,
      replayRetryFailed: 0
    };

    const engineResults = [];

    for (const persona of personas) {
      const personaId = String(persona.id || '').trim();
      const section = String(persona.section || 'local').trim().toLowerCase() || 'local';
      const feeds = (await getFeedsForPersona(sql, personaId, section)).slice(0, maxFeedsPerEngine);
      let engineSeen = 0;
      let engineInserted = 0;
      let engineDeduped = 0;
      let engineSkipped = 0;
      let engineErrors = 0;
      let engineEventDispatchFailed = 0;

      for (const feed of feeds) {
        summary.feedsScanned += 1;
        let parsed;
        try {
          parsed = await parser.parseURL(feed.feedUrl);
        } catch (error) {
          engineErrors += 1;
          summary.errors += 1;
          continue;
        }

        const items = Array.isArray(parsed?.items) ? parsed.items.slice(0, maxItemsPerFeed) : [];
        for (const item of items) {
          const signal = itemToSignal(item, feed.sourceName || parsed?.title || section, feed.feedUrl);
          if (!signal.title) continue;
          summary.signalsSeen += 1;
          engineSeen += 1;
          const result = await ingestScheduledSignal(sql, req, {
            personaId,
            section,
            signal
          });
          if (!result.ok) {
            engineErrors += 1;
            summary.errors += 1;
            if (result.reason === 'signal_received_event_failed') {
              engineEventDispatchFailed += 1;
              summary.eventDispatchFailed += 1;
            }
          } else if (result.skipped) {
            engineSkipped += 1;
            summary.skipped += 1;
          } else if (result.deduped) {
            engineDeduped += 1;
            summary.deduped += 1;
          } else {
            engineInserted += 1;
            summary.inserted += 1;
          }
        }
      }

      engineResults.push({
        personaId,
        section,
        feedsScanned: feeds.length,
        signalsSeen: engineSeen,
        inserted: engineInserted,
        deduped: engineDeduped,
        skipped: engineSkipped,
        errors: engineErrors,
        eventDispatchFailed: engineEventDispatchFailed
      });
    }

    if (replayRetryLimit > 0) {
      const replay = await replayRetryableScheduledSignals(sql, req, replayRetryLimit);
      summary.replayRetryScanned = replay.scanned;
      summary.replayRetryAttempted = replay.attempted;
      summary.replayRetrySucceeded = replay.succeeded;
      summary.replayRetryFailed = replay.failed;
    }

    return res.status(200).json({
      ok: true,
      triggerMode: 'scheduled',
      params: { maxEngines, maxFeedsPerEngine, maxItemsPerFeed, replayRetryLimit },
      summary,
      engines: engineResults
    });
  } catch (error) {
    console.error('Topic engine scheduled trigger error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    if (String(error?.message || '').toLowerCase().includes('topic_signals')) {
      return res.status(500).json({
        error: 'Topic signal table missing',
        details: 'Run migration 20260304_05_gatekeeper_signals.sql first.'
      });
    }
    return res.status(500).json({ error: 'Failed to run scheduled topic discovery' });
  }
};
