const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { TOPIC_ENGINE_STAGES } = require('./_topic-engine-workflow');
const HARD_CODED_STAGE_STACK = {
  topic_qualification: { runnerType: 'llm', provider: 'google', modelOrEndpoint: 'gemini-1.5-flash' },
  research_discovery: { runnerType: 'api_workflow', provider: 'tavily', modelOrEndpoint: 'https://api.tavily.com/search' },
  evidence_extraction: { runnerType: 'llm', provider: 'google', modelOrEndpoint: 'gemini-1.5-pro' },
  story_planning: { runnerType: 'llm', provider: 'openai', modelOrEndpoint: 'gpt-4o-mini' },
  draft_writing: { runnerType: 'llm', provider: 'anthropic', modelOrEndpoint: 'claude-3-5-sonnet' },
  final_review: { runnerType: 'llm', provider: 'openai', modelOrEndpoint: 'gpt-4o' }
};
const BEAT_OPTIONS_BY_SECTION = {
  local: ['general-local', 'government', 'crime', 'education'],
  national: ['general-national', 'politics', 'social-issues'],
  world: ['general-world', 'conflict', 'diplomacy'],
  business: ['general-business', 'local-business', 'markets', 'real-estate'],
  sports: ['general-sports', 'high-school', 'college', 'professional'],
  health: ['general-health', 'local-health', 'wellness', 'medical-research'],
  entertainment: ['general-entertainment', 'local-entertainment', 'movies', 'music', 'gaming'],
  technology: ['general-technology', 'local-tech', 'ai', 'consumer-tech']
};
const DEFAULT_SECTION = 'local';
const DEFAULT_BEAT = 'general-local';
const VALID_SECTION_SET = new Set(Object.keys(BEAT_OPTIONS_BY_SECTION));

async function ensurePersonasTable(sql) {
  const requiredTables = [
    'personas',
    'topic_engines',
    'topic_engine_feeds',
    'topic_engine_stage_configs'
  ];
  for (const tableName of requiredTables) {
    const rows = await sql`SELECT to_regclass(${`public.${tableName}`}) as name`;
    if (!rows[0]?.name) {
      const error = new Error(`Schema not ready: missing table ${tableName}`);
      error.statusCode = 503;
      throw error;
    }
  }
  const requiredPersonaColumns = [
    'activation_mode',
    'display_name',
    'section',
    'beat',
    'image_db_enabled',
    'image_sourcing_enabled',
    'image_generation_enabled',
    'image_mode',
    'image_profile',
    'image_fallback_asset_url',
    'image_fallback_cloudinary_public_id',
    'quota_postgres_image_daily',
    'quota_sourced_image_daily',
    'quota_generated_image_daily',
    'quota_text_only_daily',
    'layer6_timeout_seconds',
    'layer6_budget_usd',
    'exa_max_attempts',
    'generation_max_attempts'
  ];
  const columnRows = await sql`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'personas'
  `;
  const existingColumns = new Set(columnRows.map((row) => String(row.column_name || '')));
  const missingColumns = requiredPersonaColumns.filter((columnName) => !existingColumns.has(columnName));
  if (missingColumns.length) {
    const error = new Error(`Schema not ready: missing personas columns: ${missingColumns.join(', ')}`);
    error.statusCode = 503;
    throw error;
  }
}

function normalizeActivationMode(value) {
  const mode = String(value || 'both').trim().toLowerCase();
  if (mode === 'event' || mode === 'scheduled' || mode === 'both') return mode;
  return 'both';
}

function normalizeRunnerType(value) {
  const runner = String(value || 'llm').trim().toLowerCase();
  if (runner === 'llm' || runner === 'api_workflow' || runner === 'tool' || runner === 'script') return runner;
  return 'llm';
}

function cleanText(value, max = 5000) {
  return String(value || '').trim().slice(0, max);
}

function cleanPersonaId(value) {
  return cleanText(value, 255).toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-+|-+$/g, '');
}

function normalizeSection(value) {
  const section = cleanText(value, 120).toLowerCase();
  return VALID_SECTION_SET.has(section) ? section : DEFAULT_SECTION;
}

function normalizeBeat(section, beatValue) {
  const beats = BEAT_OPTIONS_BY_SECTION[section] || BEAT_OPTIONS_BY_SECTION[DEFAULT_SECTION];
  const beat = cleanText(beatValue, 120)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return beat || beats[0];
}

function normalizePostingDays(value) {
  if (!Array.isArray(value) || value.length !== 7) return [true, true, true, true, true, true, true];
  return value.map((v) => Boolean(v));
}

function normalizeDaypart(value) {
  const daypart = String(value || '').trim().toLowerCase();
  if (daypart === 'morning' || daypart === 'midday' || daypart === 'afternoon' || daypart === 'evening') {
    return daypart;
  }
  return '';
}

function normalizePacingConfig(value) {
  const raw = value && typeof value === 'object' ? value : {};
  const postsPerActiveDay = Number.parseInt(String(raw.postsPerActiveDay ?? 1), 10);
  const minSpacingMinutes = Number.parseInt(String(raw.minSpacingMinutes ?? 90), 10);
  const maxBacklog = Number.parseInt(String(raw.maxBacklog ?? 200), 10);
  const maxRetries = Number.parseInt(String(raw.maxRetries ?? 3), 10);

  return {
    enabled: raw.enabled === true,
    postingDays: normalizePostingDays(raw.postingDays),
    postsPerActiveDay: Number.isFinite(postsPerActiveDay) ? Math.min(Math.max(postsPerActiveDay, 0), 24) : 1,
    windowStartLocal: cleanText(raw.windowStartLocal || '06:00:00', 20) || '06:00:00',
    windowEndLocal: cleanText(raw.windowEndLocal || '22:00:00', 20) || '22:00:00',
    cadenceEnabled: raw.cadenceEnabled !== false,
    singlePostTimeLocal: cleanText(raw.singlePostTimeLocal || '', 20) || null,
    singlePostDaypart: normalizeDaypart(raw.singlePostDaypart) || null,
    minSpacingMinutes: Number.isFinite(minSpacingMinutes) ? Math.min(Math.max(minSpacingMinutes, 0), 1440) : 90,
    maxBacklog: Number.isFinite(maxBacklog) ? Math.min(Math.max(maxBacklog, 1), 5000) : 200,
    maxRetries: Number.isFinite(maxRetries) ? Math.min(Math.max(maxRetries, 0), 20) : 3
  };
}

function toIntBounded(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? fallback), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function toNumberBounded(value, fallback, min, max) {
  const parsed = Number(value ?? fallback);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function normalizeImageConfig(value) {
  const raw = value && typeof value === 'object' ? value : {};
  const mode = cleanText(raw.imageMode || raw.image_mode || 'manual', 40).toLowerCase();
  const profile = cleanText(raw.imageProfile || raw.image_profile || 'professional', 40).toLowerCase();

  return {
    imageDbEnabled: raw.imageDbEnabled === undefined && raw.image_db_enabled === undefined
      ? true
      : (raw.imageDbEnabled === true || raw.image_db_enabled === true),
    imageSourcingEnabled: raw.imageSourcingEnabled === undefined && raw.image_sourcing_enabled === undefined
      ? true
      : (raw.imageSourcingEnabled === true || raw.image_sourcing_enabled === true),
    imageGenerationEnabled: raw.imageGenerationEnabled === true || raw.image_generation_enabled === true,
    imageMode: mode === 'auto' ? 'auto' : 'manual',
    imageProfile: mode && (profile === 'professional' || profile === 'creative' || profile === 'cheap') ? profile : 'professional',
    imageFallbackAssetUrl: cleanText(raw.imageFallbackAssetUrl || raw.image_fallback_asset_url || '', 5000) || null,
    imageFallbackCloudinaryPublicId: cleanText(raw.imageFallbackCloudinaryPublicId || raw.image_fallback_cloudinary_public_id || '', 500) || null,
    quotaPostgresImageDaily: toIntBounded(raw.quotaPostgresImageDaily ?? raw.quota_postgres_image_daily, 200, 0, 5000),
    quotaSourcedImageDaily: toIntBounded(raw.quotaSourcedImageDaily ?? raw.quota_sourced_image_daily, 120, 0, 5000),
    quotaGeneratedImageDaily: toIntBounded(raw.quotaGeneratedImageDaily ?? raw.quota_generated_image_daily, 30, 0, 5000),
    quotaTextOnlyDaily: toIntBounded(raw.quotaTextOnlyDaily ?? raw.quota_text_only_daily, 400, 0, 5000),
    layer6TimeoutSeconds: toIntBounded(raw.layer6TimeoutSeconds ?? raw.layer6_timeout_seconds, 90, 15, 600),
    layer6BudgetUsd: toNumberBounded(raw.layer6BudgetUsd ?? raw.layer6_budget_usd, 0.20, 0, 50),
    exaMaxAttempts: toIntBounded(raw.exaMaxAttempts ?? raw.exa_max_attempts, 3, 1, 20),
    generationMaxAttempts: toIntBounded(raw.generationMaxAttempts ?? raw.generation_max_attempts, 2, 1, 20)
  };
}

function normalizeFeedEntry(entry) {
  const value = typeof entry === 'string' ? entry : String(entry?.feedUrl || '').trim();
  if (!value) return null;
  const sourceName = cleanText(typeof entry === 'object' ? entry?.sourceName : '', 240);
  const priorityRaw = Number.parseInt(String(typeof entry === 'object' ? entry?.priority : ''), 10);
  const priority = Number.isFinite(priorityRaw) ? Math.min(Math.max(priorityRaw, 1), 10000) : 100;
  return { feedUrl: value, sourceName, priority };
}

async function fetchPersonaWorkflow(sql, personaId) {
  const feeds = await sql`
    SELECT
      feed_url as "feedUrl",
      COALESCE(source_name, '') as "sourceName",
      COALESCE(priority, 100) as priority,
      enabled
    FROM topic_engine_feeds
    WHERE persona_id = ${personaId}
    ORDER BY priority ASC, id ASC
  `;

  const stageRows = await sql`
    SELECT
      stage_name as "stageName",
      runner_type as "runnerType",
      provider,
      model_or_endpoint as "modelOrEndpoint",
      enabled,
      prompt_template as "promptTemplate",
      workflow_config as "workflowConfig"
    FROM topic_engine_stage_configs
    WHERE persona_id = ${personaId}
  `;

  const stageMap = new Map();
  for (const row of stageRows) {
    stageMap.set(String(row.stageName || ''), row);
  }

  const stageConfigs = {};
  for (const stageName of TOPIC_ENGINE_STAGES) {
    const current = stageMap.get(stageName);
    stageConfigs[stageName] = current ? {
      runnerType: normalizeRunnerType(current.runnerType),
      provider: cleanText(current.provider, 240),
      modelOrEndpoint: cleanText(current.modelOrEndpoint, 500),
      enabled: Boolean(current.enabled),
      promptTemplate: cleanText(current.promptTemplate, 5000),
      workflowConfig: current.workflowConfig && typeof current.workflowConfig === 'object' ? current.workflowConfig : {}
    } : {
      runnerType: 'llm',
      provider: '',
      modelOrEndpoint: '',
      enabled: true,
      promptTemplate: '',
      workflowConfig: {}
    };
  }

  const engineRows = await sql`
    SELECT is_auto_promote_enabled as "isAutoPromoteEnabled"
    FROM topic_engines
    WHERE persona_id = ${personaId}
    LIMIT 1
  `;
  const isAutoPromoteEnabled = Boolean(engineRows[0]?.isAutoPromoteEnabled);

  let pacingConfig = normalizePacingConfig({});
  try {
    const pacingRows = await sql`
      SELECT
        enabled,
        posting_days as "postingDays",
        posts_per_active_day as "postsPerActiveDay",
        window_start_local::text as "windowStartLocal",
        window_end_local::text as "windowEndLocal",
        cadence_enabled as "cadenceEnabled",
        single_post_time_local::text as "singlePostTimeLocal",
        single_post_daypart as "singlePostDaypart",
        min_spacing_minutes as "minSpacingMinutes",
        max_backlog as "maxBacklog",
        max_retries as "maxRetries"
      FROM topic_engine_pacing
      WHERE persona_id = ${personaId}
      LIMIT 1
    `;
    if (pacingRows[0]) pacingConfig = normalizePacingConfig(pacingRows[0]);
  } catch (error) {
    if (!String(error?.message || '').toLowerCase().includes('topic_engine_pacing')) throw error;
  }

  return { feeds, stageConfigs, isAutoPromoteEnabled, pacingConfig };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  const sql = neon(process.env.DATABASE_URL);

  if (req.method === 'GET') {
    try {
      await ensurePersonasTable(sql);
      const rows = await sql`
        SELECT
          id,
          display_name as "displayName",
          avatar_url as "avatarUrl",
          disclosure,
          COALESCE(NULLIF(trim(activation_mode), ''), 'both') as "activationMode",
          COALESCE(NULLIF(trim(section), ''), ${DEFAULT_SECTION}) as section,
          COALESCE(NULLIF(trim(beat), ''), ${DEFAULT_BEAT}) as beat,
          COALESCE(image_db_enabled, TRUE) as "imageDbEnabled",
          COALESCE(image_sourcing_enabled, TRUE) as "imageSourcingEnabled",
          COALESCE(image_generation_enabled, FALSE) as "imageGenerationEnabled",
          COALESCE(NULLIF(trim(image_mode), ''), 'manual') as "imageMode",
          COALESCE(NULLIF(trim(image_profile), ''), 'professional') as "imageProfile",
          image_fallback_asset_url as "imageFallbackAssetUrl",
          image_fallback_cloudinary_public_id as "imageFallbackCloudinaryPublicId",
          COALESCE(quota_postgres_image_daily, 200) as "quotaPostgresImageDaily",
          COALESCE(quota_sourced_image_daily, 120) as "quotaSourcedImageDaily",
          COALESCE(quota_generated_image_daily, 30) as "quotaGeneratedImageDaily",
          COALESCE(quota_text_only_daily, 400) as "quotaTextOnlyDaily",
          COALESCE(layer6_timeout_seconds, 90) as "layer6TimeoutSeconds",
          COALESCE(layer6_budget_usd, 0.20) as "layer6BudgetUsd",
          COALESCE(exa_max_attempts, 3) as "exaMaxAttempts",
          COALESCE(generation_max_attempts, 2) as "generationMaxAttempts"
        FROM personas
      `;
      const personas = [];
      for (const row of rows) {
        const workflow = await fetchPersonaWorkflow(sql, row.id);
        personas.push({
          ...row,
          feeds: workflow.feeds,
          stageConfigs: workflow.stageConfigs,
          isAutoPromoteEnabled: workflow.isAutoPromoteEnabled,
          pacingConfig: workflow.pacingConfig
        });
      }
      return res.status(200).json({ personas });
    } catch (error) {
      console.error('Error fetching personas:', error);
      if (Number(error?.statusCode || 0) === 503) {
        return res.status(503).json({ error: error.message });
      }
      return res.status(500).json({ error: 'Failed to fetch personas' });
    }
  }

  if (req.method === 'PUT') {
    try {
      await ensurePersonasTable(sql);
      const {
        id,
        displayName,
        avatarUrl,
        disclosure,
        activationMode,
        section,
        beat,
        feeds,
        stageConfigs,
        isAutoPromoteEnabled,
        pacingConfig,
        imageConfig
      } = req.body;
      const normalizedId = cleanPersonaId(id);
      if (!normalizedId) {
        return res.status(400).json({ error: 'Persona ID is required' });
      }
      const normalizedActivationMode = normalizeActivationMode(activationMode);
      const normalizedDisplayName = cleanText(displayName, 160) || null;
      const normalizedSection = normalizeSection(section);
      const normalizedBeat = normalizeBeat(normalizedSection, beat);
      const normalizedImageConfig = normalizeImageConfig(imageConfig || req.body);

      const rows = await sql`
        INSERT INTO personas (
          id,
          display_name,
          avatar_url,
          disclosure,
          activation_mode,
          section,
          beat,
          image_db_enabled,
          image_sourcing_enabled,
          image_generation_enabled,
          image_mode,
          image_profile,
          image_fallback_asset_url,
          image_fallback_cloudinary_public_id,
          quota_postgres_image_daily,
          quota_sourced_image_daily,
          quota_generated_image_daily,
          quota_text_only_daily,
          layer6_timeout_seconds,
          layer6_budget_usd,
          exa_max_attempts,
          generation_max_attempts
        )
        VALUES (
          ${normalizedId},
          ${normalizedDisplayName},
          ${avatarUrl},
          ${disclosure},
          ${normalizedActivationMode},
          ${normalizedSection},
          ${normalizedBeat},
          ${normalizedImageConfig.imageDbEnabled},
          ${normalizedImageConfig.imageSourcingEnabled},
          ${normalizedImageConfig.imageGenerationEnabled},
          ${normalizedImageConfig.imageMode},
          ${normalizedImageConfig.imageProfile},
          ${normalizedImageConfig.imageFallbackAssetUrl},
          ${normalizedImageConfig.imageFallbackCloudinaryPublicId},
          ${normalizedImageConfig.quotaPostgresImageDaily},
          ${normalizedImageConfig.quotaSourcedImageDaily},
          ${normalizedImageConfig.quotaGeneratedImageDaily},
          ${normalizedImageConfig.quotaTextOnlyDaily},
          ${normalizedImageConfig.layer6TimeoutSeconds},
          ${normalizedImageConfig.layer6BudgetUsd},
          ${normalizedImageConfig.exaMaxAttempts},
          ${normalizedImageConfig.generationMaxAttempts}
        )
        ON CONFLICT (id) DO UPDATE
        SET display_name = EXCLUDED.display_name,
            avatar_url = EXCLUDED.avatar_url,
            disclosure = EXCLUDED.disclosure,
            activation_mode = EXCLUDED.activation_mode,
            section = EXCLUDED.section,
            beat = EXCLUDED.beat,
            image_db_enabled = EXCLUDED.image_db_enabled,
            image_sourcing_enabled = EXCLUDED.image_sourcing_enabled,
            image_generation_enabled = EXCLUDED.image_generation_enabled,
            image_mode = EXCLUDED.image_mode,
            image_profile = EXCLUDED.image_profile,
            image_fallback_asset_url = EXCLUDED.image_fallback_asset_url,
            image_fallback_cloudinary_public_id = EXCLUDED.image_fallback_cloudinary_public_id,
            quota_postgres_image_daily = EXCLUDED.quota_postgres_image_daily,
            quota_sourced_image_daily = EXCLUDED.quota_sourced_image_daily,
            quota_generated_image_daily = EXCLUDED.quota_generated_image_daily,
            quota_text_only_daily = EXCLUDED.quota_text_only_daily,
            layer6_timeout_seconds = EXCLUDED.layer6_timeout_seconds,
            layer6_budget_usd = EXCLUDED.layer6_budget_usd,
            exa_max_attempts = EXCLUDED.exa_max_attempts,
            generation_max_attempts = EXCLUDED.generation_max_attempts,
            updated_at = now()
        RETURNING
          id,
          display_name as "displayName",
          avatar_url as "avatarUrl",
          disclosure,
          COALESCE(NULLIF(trim(activation_mode), ''), 'both') as "activationMode",
          COALESCE(NULLIF(trim(section), ''), ${DEFAULT_SECTION}) as section,
          COALESCE(NULLIF(trim(beat), ''), ${DEFAULT_BEAT}) as beat,
          COALESCE(image_db_enabled, TRUE) as "imageDbEnabled",
          COALESCE(image_sourcing_enabled, TRUE) as "imageSourcingEnabled",
          COALESCE(image_generation_enabled, FALSE) as "imageGenerationEnabled",
          COALESCE(NULLIF(trim(image_mode), ''), 'manual') as "imageMode",
          COALESCE(NULLIF(trim(image_profile), ''), 'professional') as "imageProfile",
          image_fallback_asset_url as "imageFallbackAssetUrl",
          image_fallback_cloudinary_public_id as "imageFallbackCloudinaryPublicId",
          COALESCE(quota_postgres_image_daily, 200) as "quotaPostgresImageDaily",
          COALESCE(quota_sourced_image_daily, 120) as "quotaSourcedImageDaily",
          COALESCE(quota_generated_image_daily, 30) as "quotaGeneratedImageDaily",
          COALESCE(quota_text_only_daily, 400) as "quotaTextOnlyDaily",
          COALESCE(layer6_timeout_seconds, 90) as "layer6TimeoutSeconds",
          COALESCE(layer6_budget_usd, 0.20) as "layer6BudgetUsd",
          COALESCE(exa_max_attempts, 3) as "exaMaxAttempts",
          COALESCE(generation_max_attempts, 2) as "generationMaxAttempts";
      `;

      if (Array.isArray(feeds)) {
        const normalizedFeeds = feeds
          .map(normalizeFeedEntry)
          .filter(Boolean)
          .filter((item, index, arr) => arr.findIndex((x) => x.feedUrl === item.feedUrl) === index);
        await sql`DELETE FROM topic_engine_feeds WHERE persona_id = ${normalizedId}`;
        for (const feed of normalizedFeeds) {
          await sql`
            INSERT INTO topic_engine_feeds (persona_id, feed_url, source_name, priority, enabled, updated_at)
            VALUES (${normalizedId}, ${feed.feedUrl}, ${feed.sourceName || null}, ${feed.priority}, true, NOW())
          `;
        }
      }

      if (stageConfigs && typeof stageConfigs === 'object') {
        for (const stageName of TOPIC_ENGINE_STAGES) {
          const raw = stageConfigs[stageName];
          if (!raw || typeof raw !== 'object') continue;
          const fixed = HARD_CODED_STAGE_STACK[stageName] || { runnerType: 'llm', provider: 'google', modelOrEndpoint: '' };
          const runnerType = normalizeRunnerType(fixed.runnerType);
          const provider = cleanText(fixed.provider, 240);
          const modelOrEndpoint = cleanText(fixed.modelOrEndpoint, 500);
          const enabled = raw.enabled !== false;
          const promptTemplate = cleanText(raw.promptTemplate, 5000);
          const workflowConfig = raw.workflowConfig && typeof raw.workflowConfig === 'object'
            ? raw.workflowConfig
            : {};
          await sql`
            INSERT INTO topic_engine_stage_configs (
              persona_id,
              stage_name,
              runner_type,
              provider,
              model_or_endpoint,
              enabled,
              prompt_template,
              workflow_config,
              updated_at
            )
            VALUES (
              ${normalizedId},
              ${stageName},
              ${runnerType},
              ${provider},
              ${modelOrEndpoint},
              ${enabled},
              ${promptTemplate},
              ${workflowConfig}::jsonb,
              NOW()
            )
            ON CONFLICT (persona_id, stage_name) DO UPDATE
            SET
              runner_type = EXCLUDED.runner_type,
              provider = EXCLUDED.provider,
              model_or_endpoint = EXCLUDED.model_or_endpoint,
              enabled = EXCLUDED.enabled,
              prompt_template = EXCLUDED.prompt_template,
              workflow_config = EXCLUDED.workflow_config,
              updated_at = NOW()
          `;
        }
      }

      if (typeof isAutoPromoteEnabled === 'boolean') {
        await sql`
          INSERT INTO topic_engines (persona_id, is_auto_promote_enabled, updated_at)
          VALUES (${normalizedId}, ${isAutoPromoteEnabled}, NOW())
          ON CONFLICT (persona_id) DO UPDATE
          SET
            is_auto_promote_enabled = EXCLUDED.is_auto_promote_enabled,
            updated_at = NOW()
        `;
      }

      if (pacingConfig && typeof pacingConfig === 'object') {
        const pacing = normalizePacingConfig(pacingConfig);
        await sql`
          INSERT INTO topic_engine_pacing (
            persona_id,
            enabled,
            posting_days,
            posts_per_active_day,
            window_start_local,
            window_end_local,
            cadence_enabled,
            single_post_time_local,
            single_post_daypart,
            min_spacing_minutes,
            max_backlog,
            max_retries,
            updated_at
          )
          VALUES (
            ${normalizedId},
            ${pacing.enabled},
            ${pacing.postingDays},
            ${pacing.postsPerActiveDay},
            ${pacing.windowStartLocal}::time,
            ${pacing.windowEndLocal}::time,
            ${pacing.cadenceEnabled},
            ${pacing.singlePostTimeLocal}::time,
            ${pacing.singlePostDaypart},
            ${pacing.minSpacingMinutes},
            ${pacing.maxBacklog},
            ${pacing.maxRetries},
            NOW()
          )
          ON CONFLICT (persona_id) DO UPDATE
          SET
            enabled = EXCLUDED.enabled,
            posting_days = EXCLUDED.posting_days,
            posts_per_active_day = EXCLUDED.posts_per_active_day,
            window_start_local = EXCLUDED.window_start_local,
            window_end_local = EXCLUDED.window_end_local,
            cadence_enabled = EXCLUDED.cadence_enabled,
            single_post_time_local = EXCLUDED.single_post_time_local,
            single_post_daypart = EXCLUDED.single_post_daypart,
            min_spacing_minutes = EXCLUDED.min_spacing_minutes,
            max_backlog = EXCLUDED.max_backlog,
            max_retries = EXCLUDED.max_retries,
            updated_at = NOW()
        `;
      }

      const workflow = await fetchPersonaWorkflow(sql, normalizedId);
      return res.status(200).json({
        persona: {
          ...rows[0],
          feeds: workflow.feeds,
          stageConfigs: workflow.stageConfigs,
          isAutoPromoteEnabled: workflow.isAutoPromoteEnabled,
          pacingConfig: workflow.pacingConfig
        }
      });
    } catch (error) {
      console.error('Error saving persona:', error);
      if (Number(error?.statusCode || 0) === 503) {
        return res.status(503).json({ error: error.message });
      }
      return res.status(500).json({ error: 'Failed to save persona' });
    }
  }

  res.setHeader('Allow', ['GET', 'PUT']);
  return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
};
