const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { ensureTopicEngineTables, TOPIC_ENGINE_STAGES } = require('./_topic-engine-workflow');

async function ensurePersonasTable(sql) {
  await ensureTopicEngineTables(sql);
  await sql`
    ALTER TABLE personas
    ADD COLUMN IF NOT EXISTS activation_mode TEXT DEFAULT 'both'
  `;
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

  return { feeds, stageConfigs, isAutoPromoteEnabled };
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
          avatar_url as "avatarUrl",
          disclosure,
          COALESCE(NULLIF(trim(activation_mode), ''), 'both') as "activationMode"
        FROM personas
      `;
      const personas = [];
      for (const row of rows) {
        const workflow = await fetchPersonaWorkflow(sql, row.id);
        personas.push({
          ...row,
          feeds: workflow.feeds,
          stageConfigs: workflow.stageConfigs
        });
      }
      return res.status(200).json({ personas });
    } catch (error) {
      console.error('Error fetching personas:', error);
      // If table doesn't exist yet, return empty list instead of crashing
      if (error.message.includes('does not exist')) {
         return res.status(200).json({ personas: [] });
      }
      return res.status(500).json({ error: 'Failed to fetch personas' });
    }
  }

  if (req.method === 'PUT') {
    try {
      await ensurePersonasTable(sql);
      const { id, avatarUrl, disclosure, activationMode, feeds, stageConfigs, isAutoPromoteEnabled } = req.body;
      if (!id) {
        return res.status(400).json({ error: 'Persona ID is required' });
      }
      const normalizedActivationMode = normalizeActivationMode(activationMode);

      const rows = await sql`
        INSERT INTO personas (id, avatar_url, disclosure, activation_mode)
        VALUES (${id}, ${avatarUrl}, ${disclosure}, ${normalizedActivationMode})
        ON CONFLICT (id) DO UPDATE
        SET avatar_url = EXCLUDED.avatar_url,
            disclosure = EXCLUDED.disclosure,
            activation_mode = EXCLUDED.activation_mode,
            updated_at = now()
        RETURNING
          id,
          avatar_url as "avatarUrl",
          disclosure,
          COALESCE(NULLIF(trim(activation_mode), ''), 'both') as "activationMode";
      `;

      if (Array.isArray(feeds)) {
        const normalizedFeeds = feeds
          .map(normalizeFeedEntry)
          .filter(Boolean)
          .filter((item, index, arr) => arr.findIndex((x) => x.feedUrl === item.feedUrl) === index);
        await sql`DELETE FROM topic_engine_feeds WHERE persona_id = ${id}`;
        for (const feed of normalizedFeeds) {
          await sql`
            INSERT INTO topic_engine_feeds (persona_id, feed_url, source_name, priority, enabled, updated_at)
            VALUES (${id}, ${feed.feedUrl}, ${feed.sourceName || null}, ${feed.priority}, true, NOW())
          `;
        }
      }

      if (stageConfigs && typeof stageConfigs === 'object') {
        for (const stageName of TOPIC_ENGINE_STAGES) {
          const raw = stageConfigs[stageName];
          if (!raw || typeof raw !== 'object') continue;
          const runnerType = normalizeRunnerType(raw.runnerType);
          const provider = cleanText(raw.provider, 240);
          const modelOrEndpoint = cleanText(raw.modelOrEndpoint, 500);
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
              ${id},
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
          VALUES (${id}, ${isAutoPromoteEnabled}, NOW())
          ON CONFLICT (persona_id) DO UPDATE
          SET
            is_auto_promote_enabled = EXCLUDED.is_auto_promote_enabled,
            updated_at = NOW()
        `;
      }

      const workflow = await fetchPersonaWorkflow(sql, id);
      return res.status(200).json({
        persona: {
          ...rows[0],
          feeds: workflow.feeds,
          stageConfigs: workflow.stageConfigs
        }
      });
    } catch (error) {
      console.error('Error saving persona:', error);
      return res.status(500).json({ error: 'Failed to save persona' });
    }
  }

  res.setHeader('Allow', ['GET', 'PUT']);
  return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
};
