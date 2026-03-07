const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const {
  TOPIC_ENGINE_STAGES,
  SECTION_OPTIONS,
  normalizeStageName,
  normalizeSection,
  normalizeScopeType,
  normalizeGuidance
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

function parseVersion(value) {
  const raw = String(value ?? '').trim();
  if (!/^[1-9]\d*$/.test(raw)) return null;
  const num = Number(raw);
  return Number.isFinite(num) && num >= 1 ? num : null;
}

function normalizeLayerRow(row) {
  return {
    id: Number(row.id),
    stageName: cleanText(row.stageName, 120),
    scopeType: cleanText(row.scopeType, 40),
    section: cleanText(row.section || '', 120) || null,
    promptTemplate: cleanText(row.promptTemplate || '', 50000),
    version: Number(row.version || 1),
    createdAt: row.createdAt || null,
    updatedAt: row.updatedAt || null
  };
}

async function getLayerRow(sql, stageName, scopeType, section) {
  if (scopeType === 'global') {
    const rows = await sql`
      SELECT
        id,
        stage_name as "stageName",
        scope_type as "scopeType",
        section,
        prompt_template as "promptTemplate",
        version,
        created_at as "createdAt",
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
      id,
      stage_name as "stageName",
      scope_type as "scopeType",
      section,
      prompt_template as "promptTemplate",
      version,
      created_at as "createdAt",
      updated_at as "updatedAt"
    FROM topic_engine_prompt_layers
    WHERE stage_name = ${stageName}
      AND scope_type = 'section'
      AND section = ${section}
    LIMIT 1
  `;
  return rows[0] || null;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (!['GET', 'PUT'].includes(req.method)) {
    res.setHeader('Allow', ['GET', 'PUT']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const tableReady = await assertPromptLayersTable(sql);
    if (!tableReady) {
      return res.status(503).json({
        error: 'Prompt layers table not found. Run migration 20260307_12_prompt_layers.sql first.'
      });
    }

    if (req.method === 'GET') {
      const stageFilter = normalizeStageName(req.query?.stageName || req.query?.stage || '');
      const scopeFilter = normalizeScopeType(req.query?.scopeType || req.query?.scope || '');
      const sectionFilter = normalizeSection(req.query?.section || '');

      const rows = await sql`
        SELECT
          id,
          stage_name as "stageName",
          scope_type as "scopeType",
          section,
          prompt_template as "promptTemplate",
          version,
          created_at as "createdAt",
          updated_at as "updatedAt"
        FROM topic_engine_prompt_layers
        WHERE (${stageFilter || null}::text IS NULL OR stage_name = ${stageFilter || null})
          AND (${scopeFilter || null}::text IS NULL OR scope_type = ${scopeFilter || null})
          AND (${sectionFilter || null}::text IS NULL OR section = ${sectionFilter || null})
        ORDER BY stage_name ASC, scope_type ASC, section ASC NULLS FIRST
      `;
      return res.status(200).json({
        stages: TOPIC_ENGINE_STAGES,
        sections: SECTION_OPTIONS,
        layers: rows.map(normalizeLayerRow)
      });
    }

    const stageName = normalizeStageName(req.body?.stageName || req.body?.stage || '');
    if (!stageName) return res.status(400).json({ error: 'Valid stageName is required' });

    const scopeType = normalizeScopeType(req.body?.scopeType || req.body?.scope || '');
    if (scopeType !== 'global' && scopeType !== 'section') {
      return res.status(400).json({ error: 'scopeType must be global or section' });
    }

    const section = scopeType === 'section'
      ? normalizeSection(req.body?.section || '')
      : '';
    if (scopeType === 'section' && !section) {
      return res.status(400).json({ error: 'Valid section is required when scopeType=section' });
    }
    if (scopeType === 'global' && cleanText(req.body?.section || '', 120)) {
      return res.status(400).json({ error: 'section must be empty for global scopeType' });
    }

    const promptTemplate = normalizeGuidance(req.body?.promptTemplate || '');
    const expectedVersion = parseVersion(req.body?.expectedVersion ?? req.body?.version ?? null);

    if (!expectedVersion) {
      let inserted = [];
      if (scopeType === 'global') {
        inserted = await sql`
          INSERT INTO topic_engine_prompt_layers (
            stage_name,
            scope_type,
            section,
            prompt_template,
            version,
            created_at,
            updated_at
          )
          VALUES (
            ${stageName},
            'global',
            NULL,
            ${promptTemplate},
            1,
            NOW(),
            NOW()
          )
          ON CONFLICT (stage_name) WHERE scope_type = 'global' DO NOTHING
          RETURNING
            id,
            stage_name as "stageName",
            scope_type as "scopeType",
            section,
            prompt_template as "promptTemplate",
            version,
            created_at as "createdAt",
            updated_at as "updatedAt"
        `;
      } else {
        inserted = await sql`
          INSERT INTO topic_engine_prompt_layers (
            stage_name,
            scope_type,
            section,
            prompt_template,
            version,
            created_at,
            updated_at
          )
          VALUES (
            ${stageName},
            'section',
            ${section},
            ${promptTemplate},
            1,
            NOW(),
            NOW()
          )
          ON CONFLICT (stage_name, section) WHERE scope_type = 'section' DO NOTHING
          RETURNING
            id,
            stage_name as "stageName",
            scope_type as "scopeType",
            section,
            prompt_template as "promptTemplate",
            version,
            created_at as "createdAt",
            updated_at as "updatedAt"
        `;
      }

      if (inserted[0]) {
        return res.status(200).json({ layer: normalizeLayerRow(inserted[0]), created: true });
      }

      const current = await getLayerRow(sql, stageName, scopeType, section || null);
      return res.status(409).json({
        error: 'Layer already exists. expectedVersion is required for updates.',
        current: current ? normalizeLayerRow(current) : null
      });
    }

    let updated = [];
    if (scopeType === 'global') {
      updated = await sql`
        UPDATE topic_engine_prompt_layers
        SET prompt_template = ${promptTemplate}
        WHERE stage_name = ${stageName}
          AND scope_type = 'global'
          AND version = ${expectedVersion}
        RETURNING
          id,
          stage_name as "stageName",
          scope_type as "scopeType",
          section,
          prompt_template as "promptTemplate",
          version,
          created_at as "createdAt",
          updated_at as "updatedAt"
      `;
    } else {
      updated = await sql`
        UPDATE topic_engine_prompt_layers
        SET prompt_template = ${promptTemplate}
        WHERE stage_name = ${stageName}
          AND scope_type = 'section'
          AND section = ${section}
          AND version = ${expectedVersion}
        RETURNING
          id,
          stage_name as "stageName",
          scope_type as "scopeType",
          section,
          prompt_template as "promptTemplate",
          version,
          created_at as "createdAt",
          updated_at as "updatedAt"
      `;
    }

    if (updated[0]) return res.status(200).json({ layer: normalizeLayerRow(updated[0]), created: false });

    const latest = await getLayerRow(sql, stageName, scopeType, section || null);
    return res.status(409).json({
      error: latest ? 'Version conflict. Refresh and retry.' : 'Layer not found for update.',
      current: latest ? normalizeLayerRow(latest) : null
    });
  } catch (error) {
    console.error('Admin prompt layers API error:', error);
    return res.status(500).json({ error: 'Failed to update prompt layers' });
  }
};
