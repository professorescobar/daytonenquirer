const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const {
  normalizeStageName,
  compileStagePrompt
} = require('../lib/topic-engine-prompts');

function cleanText(value, max = 5000) {
  return String(value || '').trim().slice(0, max);
}

async function assertPromptLayersTable(sql) {
  const rows = await sql`
    SELECT to_regclass('public.topic_engine_prompt_layers') as "tableName"
  `;
  return Boolean(rows?.[0]?.tableName);
}

async function loadPersona(sql, personaId) {
  const rows = await sql`
    SELECT
      id,
      COALESCE(NULLIF(trim(section), ''), 'local') as section
    FROM personas
    WHERE id = ${personaId}
    LIMIT 1
  `;
  return rows[0] || null;
}

async function loadPersonaStagePrompt(sql, personaId, stageName) {
  try {
    const rows = await sql`
      SELECT
        prompt_template as "promptTemplate",
        NULL::int as version,
        updated_at as "updatedAt"
      FROM topic_engine_stage_configs
      WHERE persona_id = ${personaId}
        AND stage_name = ${stageName}
      LIMIT 1
    `;
    return rows[0] || null;
  } catch (error) {
    const message = String(error?.message || '').toLowerCase();
    if (message.includes('topic_engine_stage_configs') || message.includes('does not exist')) {
      return null;
    }
    throw error;
  }
}

async function loadLayerPrompt(sql, stageName, scopeType, section) {
  if (scopeType === 'global') {
    const rows = await sql`
      SELECT
        prompt_template as "promptTemplate",
        version,
        updated_at as "updatedAt"
      FROM topic_engine_prompt_layers
      WHERE stage_name = ${stageName}
        AND scope_type = 'global'
      LIMIT 1
    `;
    return rows[0] || null;
  }
  const rows = await sql`
    SELECT
      prompt_template as "promptTemplate",
      version,
      updated_at as "updatedAt"
    FROM topic_engine_prompt_layers
    WHERE stage_name = ${stageName}
      AND scope_type = 'section'
      AND section = ${section}
    LIMIT 1
  `;
  return rows[0] || null;
}

function redactLayerBreakdown(layers) {
  return (Array.isArray(layers) ? layers : []).map((item) => ({
    layer: cleanText(item?.layer, 60),
    text: cleanText(item?.text, 50000)
  }));
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'GET') {
    res.setHeader('Allow', ['GET']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    const personaId = cleanText(req.query?.personaId || req.query?.persona || '', 255);
    const stageName = normalizeStageName(req.query?.stageName || req.query?.stage || '');
    if (!personaId) return res.status(400).json({ error: 'personaId is required' });
    if (!stageName) return res.status(400).json({ error: 'valid stageName is required' });

    const sql = neon(process.env.DATABASE_URL);
    const tableReady = await assertPromptLayersTable(sql);
    if (!tableReady) {
      return res.status(503).json({
        error: 'Prompt layers table not found. Run migration 20260307_12_prompt_layers.sql first.'
      });
    }

    const persona = await loadPersona(sql, personaId);
    if (!persona) return res.status(404).json({ error: 'Persona not found' });

    const section = cleanText(persona.section || 'local', 120).toLowerCase() || 'local';
    const [globalLayer, sectionLayer, personaStage] = await Promise.all([
      loadLayerPrompt(sql, stageName, 'global'),
      loadLayerPrompt(sql, stageName, 'section', section),
      loadPersonaStagePrompt(sql, personaId, stageName)
    ]);

    const compiled = compileStagePrompt({
      stageName,
      section,
      globalPrompt: globalLayer?.promptTemplate || '',
      sectionPrompt: sectionLayer?.promptTemplate || '',
      personaPrompt: personaStage?.promptTemplate || '',
      sourceVersions: {
        global: globalLayer?.version || null,
        section: sectionLayer?.version || null,
        persona: personaStage?.version || null
      },
      runtimeContext: {
        note: '[redacted runtime context in preview]'
      }
    });

    if (!compiled.ok) {
      return res.status(400).json({
        error: 'Failed to compile prompt',
        warnings: compiled.warnings || ['compile_failed']
      });
    }

    return res.status(200).json({
      personaId,
      stageName,
      section,
      compiledPrompt: cleanText(compiled.compiledPrompt, 200000),
      layerBreakdown: redactLayerBreakdown(compiled.layerBreakdown),
      warnings: Array.isArray(compiled.warnings) ? compiled.warnings : [],
      hasEditableGuidance: Boolean(compiled.hasEditableGuidance),
      promptHash: cleanText(compiled.promptHash, 120),
      promptSourceVersion: cleanText(compiled.promptSourceVersion, 120),
      sourceVersions: {
        global: globalLayer?.version || null,
        section: sectionLayer?.version || null,
        persona: personaStage?.version || null
      },
      sourceUpdatedAt: {
        global: globalLayer?.updatedAt || null,
        section: sectionLayer?.updatedAt || null,
        persona: personaStage?.updatedAt || null
      },
      runtimePreview: {
        context: '[redacted]'
      }
    });
  } catch (error) {
    console.error('Admin prompt preview API error:', error);
    return res.status(500).json({ error: 'Failed to generate prompt preview' });
  }
};
