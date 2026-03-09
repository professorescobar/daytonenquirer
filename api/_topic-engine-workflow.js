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

async function ensureTopicEngineTables(sql, options = {}) {
  const profile = String(options.profile || 'full').trim().toLowerCase();
  const requirementsByName = {
    personas: { table: 'personas', columns: ['id', 'activation_mode', 'section', 'beat'] },
    topic_engines: { table: 'topic_engines', columns: ['persona_id', 'is_auto_promote_enabled', 'updated_at'] },
    topic_engine_feeds: { table: 'topic_engine_feeds', columns: ['persona_id', 'feed_url', 'source_name', 'priority', 'enabled'] },
    topic_engine_candidates: {
      table: 'topic_engine_candidates',
      columns: ['persona_id', 'trigger_mode', 'dedupe_key', 'title', 'url', 'snippet', 'source_name', 'source_url', 'published_at', 'status', 'metadata']
    },
    topic_engine_stage_configs: {
      table: 'topic_engine_stage_configs',
      columns: ['persona_id', 'stage_name', 'runner_type', 'provider', 'model_or_endpoint', 'enabled', 'prompt_template', 'workflow_config']
    }
  };
  const profiles = {
    full: ['personas', 'topic_engines', 'topic_engine_feeds', 'topic_engine_candidates', 'topic_engine_stage_configs'],
    ingestion_event: ['personas', 'topic_engine_candidates'],
    ingestion_scheduled: ['personas', 'topic_engine_feeds', 'topic_engine_candidates']
  };
  const requirementNames = profiles[profile] || profiles.full;
  const requirements = requirementNames.map((name) => requirementsByName[name]).filter(Boolean);

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

  await ensureTopicEngineTables(sql, { profile: 'ingestion_event' });
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
