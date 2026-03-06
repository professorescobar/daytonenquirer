const crypto = require('crypto');

const VALID_ACTIVATION_MODES = new Set(['event', 'scheduled', 'both']);
const TOPIC_ENGINE_STAGES = [
  'topic_qualification',
  'research_discovery',
  'evidence_extraction',
  'story_planning',
  'draft_writing',
  'final_review'
];

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const url = new URL(raw);
    url.hash = '';
    url.searchParams.delete('utm_source');
    url.searchParams.delete('utm_medium');
    url.searchParams.delete('utm_campaign');
    url.searchParams.delete('utm_term');
    url.searchParams.delete('utm_content');
    return url.toString();
  } catch (_) {
    return raw;
  }
}

function toIsoOrNull(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function buildDedupeKey(personaId, title, url) {
  const normalizedTitle = normalizeText(title).slice(0, 220);
  const normalizedUrl = normalizeUrl(url);
  const canonical = `${String(personaId || '').trim().toLowerCase()}::${normalizedTitle}::${normalizedUrl}`;
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

function normalizeSignal(signal = {}) {
  const title = String(signal.title || '').trim();
  const url = normalizeUrl(signal.url || signal.link || '');
  const snippet = String(signal.snippet || signal.description || signal.summary || '').trim();
  const sourceName = String(signal.sourceName || signal.source || '').trim();
  const sourceUrl = normalizeUrl(signal.sourceUrl || '');
  const publishedAt = toIsoOrNull(signal.publishedAt || signal.pubDate || signal.published_at || null);
  const metadata = signal.metadata && typeof signal.metadata === 'object' ? signal.metadata : {};
  return { title, url, snippet, sourceName, sourceUrl, publishedAt, metadata };
}

async function ensureTopicEngineTables(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS personas (
      id VARCHAR(255) PRIMARY KEY,
      avatar_url TEXT,
      disclosure TEXT,
      activation_mode TEXT DEFAULT 'both',
      updated_at TIMESTAMPTZ DEFAULT now()
    )
  `;
  await sql`
    ALTER TABLE personas
    ADD COLUMN IF NOT EXISTS activation_mode TEXT DEFAULT 'both'
  `;
  await sql`
    ALTER TABLE personas
    ADD COLUMN IF NOT EXISTS display_name TEXT
  `;
  await sql`
    ALTER TABLE personas
    ADD COLUMN IF NOT EXISTS section TEXT DEFAULT 'local'
  `;
  await sql`
    ALTER TABLE personas
    ADD COLUMN IF NOT EXISTS beat TEXT DEFAULT 'general-local'
  `;
  await sql`
    UPDATE personas
    SET activation_mode = 'both'
    WHERE activation_mode IS NULL OR trim(activation_mode) = ''
  `;
  await sql`
    UPDATE personas
    SET section = 'local'
    WHERE section IS NULL OR trim(section) = ''
  `;
  await sql`
    UPDATE personas
    SET beat = 'general-local'
    WHERE beat IS NULL OR trim(beat) = ''
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS topic_engines (
      persona_id VARCHAR(255) PRIMARY KEY,
      is_auto_promote_enabled BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
  await sql`
    ALTER TABLE topic_engines
    ADD COLUMN IF NOT EXISTS is_auto_promote_enabled BOOLEAN NOT NULL DEFAULT false
  `;
  await sql`
    ALTER TABLE topic_engines
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  `;
  await sql`
    ALTER TABLE topic_engines
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  `;
  await sql`
    INSERT INTO topic_engines (persona_id, is_auto_promote_enabled, created_at, updated_at)
    SELECT p.id, false, NOW(), NOW()
    FROM personas p
    LEFT JOIN topic_engines te ON te.persona_id = p.id
    WHERE te.persona_id IS NULL
    ON CONFLICT (persona_id) DO NOTHING
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS idx_topic_engines_auto_promote
    ON topic_engines (is_auto_promote_enabled, updated_at DESC)
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS topic_engine_feeds (
      id SERIAL PRIMARY KEY,
      persona_id VARCHAR(255) NOT NULL,
      feed_url TEXT NOT NULL,
      source_name TEXT,
      priority INTEGER DEFAULT 100,
      enabled BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(persona_id, feed_url)
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS idx_topic_engine_feeds_persona_enabled
    ON topic_engine_feeds(persona_id, enabled, priority)
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS topic_engine_candidates (
      id SERIAL PRIMARY KEY,
      persona_id VARCHAR(255) NOT NULL,
      trigger_mode TEXT NOT NULL,
      dedupe_key TEXT NOT NULL,
      title TEXT NOT NULL,
      url TEXT,
      snippet TEXT,
      source_name TEXT,
      source_url TEXT,
      published_at TIMESTAMPTZ,
      status TEXT NOT NULL DEFAULT 'discovered',
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(persona_id, dedupe_key)
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS idx_topic_engine_candidates_persona_status_created
    ON topic_engine_candidates(persona_id, status, created_at DESC)
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS topic_engine_stage_configs (
      id SERIAL PRIMARY KEY,
      persona_id VARCHAR(255) NOT NULL,
      stage_name TEXT NOT NULL,
      runner_type TEXT NOT NULL DEFAULT 'llm',
      provider TEXT NOT NULL DEFAULT '',
      model_or_endpoint TEXT NOT NULL DEFAULT '',
      enabled BOOLEAN NOT NULL DEFAULT true,
      prompt_template TEXT NOT NULL DEFAULT '',
      workflow_config JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(persona_id, stage_name)
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS idx_topic_engine_stage_configs_persona
    ON topic_engine_stage_configs(persona_id, stage_name)
  `;
}

async function getPersonaConfig(sql, personaId) {
  const rows = await sql`
    SELECT
      id,
      COALESCE(NULLIF(trim(activation_mode), ''), 'both') AS "activationMode"
    FROM personas
    WHERE id = ${personaId}
    LIMIT 1
  `;
  if (!rows[0]) return null;
  const mode = String(rows[0].activationMode || 'both').toLowerCase();
  return {
    id: rows[0].id,
    activationMode: VALID_ACTIVATION_MODES.has(mode) ? mode : 'both'
  };
}

function isTriggerAllowed(activationMode, triggerMode) {
  if (activationMode === 'both') return true;
  return activationMode === triggerMode;
}

async function runTopicEngineWorkflow(sql, payload) {
  const personaId = String(payload.personaId || '').trim();
  const triggerMode = String(payload.triggerMode || 'event').trim().toLowerCase();
  const signal = normalizeSignal(payload.signal || {});

  if (!personaId) {
    return { ok: false, reason: 'missing_persona_id' };
  }
  if (!VALID_ACTIVATION_MODES.has(triggerMode)) {
    return { ok: false, reason: 'invalid_trigger_mode' };
  }
  if (!signal.title) {
    return { ok: false, reason: 'missing_signal_title' };
  }

  await ensureTopicEngineTables(sql);
  const persona = await getPersonaConfig(sql, personaId);
  if (!persona) {
    return { ok: false, reason: 'persona_not_found', personaId };
  }
  if (!isTriggerAllowed(persona.activationMode, triggerMode)) {
    return {
      ok: true,
      skipped: true,
      reason: 'activation_mode_blocked',
      activationMode: persona.activationMode
    };
  }

  const dedupeKey = buildDedupeKey(personaId, signal.title, signal.url);
  const rows = await sql`
    INSERT INTO topic_engine_candidates (
      persona_id,
      trigger_mode,
      dedupe_key,
      title,
      url,
      snippet,
      source_name,
      source_url,
      published_at,
      status,
      metadata,
      updated_at
    )
    VALUES (
      ${personaId},
      ${triggerMode},
      ${dedupeKey},
      ${signal.title},
      ${signal.url || null},
      ${signal.snippet || null},
      ${signal.sourceName || null},
      ${signal.sourceUrl || null},
      ${signal.publishedAt},
      'discovered',
      ${signal.metadata}::jsonb,
      NOW()
    )
    ON CONFLICT (persona_id, dedupe_key) DO NOTHING
    RETURNING id, persona_id as "personaId", title, url, status, created_at as "createdAt"
  `;

  if (rows[0]) {
    return {
      ok: true,
      inserted: true,
      deduped: false,
      candidate: rows[0]
    };
  }

  return {
    ok: true,
    inserted: false,
    deduped: true,
    reason: 'duplicate_candidate'
  };
}

module.exports = {
  ensureTopicEngineTables,
  runTopicEngineWorkflow,
  normalizeSignal,
  normalizeUrl,
  TOPIC_ENGINE_STAGES
};
