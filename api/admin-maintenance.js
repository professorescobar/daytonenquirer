const { neon } = require('@neondatabase/serverless');

function getAdminToken(req) {
  const authHeader = String(req.headers.authorization || '');
  const bearer = authHeader.startsWith('Bearer ')
    ? authHeader.slice('Bearer '.length).trim()
    : '';
  return bearer || req.headers['x-admin-token'] || req.query.token || '';
}

function requireAdminApiKey(req, res) {
  const expected = String(process.env.ADMIN_API_TOKEN || '').trim();
  if (!expected) {
    res.status(500).json({ error: 'Missing ADMIN_API_TOKEN env var' });
    return false;
  }
  const token = String(getAdminToken(req) || '').trim();
  if (!token || token !== expected) {
    res.status(401).json({ error: 'Unauthorized' });
    return false;
  }
  return true;
}

function parseBool(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function parsePositiveInt(value, fallback, min = 1, max = 1000000) {
  const num = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(Math.max(num, min), max);
}

async function tableExists(sql, tableName) {
  const rows = await sql`SELECT to_regclass(${`public.${tableName}`}) AS reg`;
  return Boolean(rows?.[0]?.reg);
}

async function ensureArchiveTables(sql) {
  const requiredTables = ['research_artifacts_archive', 'pipeline_steps_archive'];
  for (const tableName of requiredTables) {
    const rows = await sql`SELECT to_regclass(${`public.${tableName}`}) AS reg`;
    if (!rows?.[0]?.reg) {
      const error = new Error(`Schema not ready: missing table ${tableName}. Apply migration 20260309_27.`);
      error.statusCode = 503;
      throw error;
    }
  }
}

async function countEligibleResearchArtifacts(sql, daysOld) {
  const rows = await sql`
    SELECT COUNT(*)::int AS count
    FROM research_artifacts
    WHERE created_at < NOW() - make_interval(days => ${daysOld})
  `;
  return Number(rows?.[0]?.count || 0);
}

async function countEligiblePipelineSteps(sql, daysOld) {
  const rows = await sql`
    SELECT COUNT(*)::int AS count
    FROM pipeline_steps
    WHERE created_at < NOW() - make_interval(days => ${daysOld})
  `;
  return Number(rows?.[0]?.count || 0);
}

async function countEligibleApiRateLimits(sql, daysOld) {
  const rows = await sql`
    SELECT COUNT(*)::int AS count
    FROM api_rate_limits
    WHERE window_start < NOW() - make_interval(days => ${daysOld})
  `;
  return Number(rows?.[0]?.count || 0);
}

async function archiveResearchArtifactsChunk(sql, daysOld, batchSize) {
  const rows = await sql`
    WITH target AS (
      SELECT id
      FROM research_artifacts
      WHERE created_at < NOW() - make_interval(days => ${daysOld})
      ORDER BY created_at
      LIMIT ${batchSize}
      FOR UPDATE SKIP LOCKED
    ),
    moved AS (
      INSERT INTO research_artifacts_archive (
        id, run_id, engine_id, candidate_id, stage, artifact_type,
        source_url, source_domain, title, published_at, content, metadata, created_at
      )
      SELECT
        ra.id, ra.run_id, ra.engine_id, ra.candidate_id, ra.stage::text, ra.artifact_type,
        ra.source_url, ra.source_domain, ra.title, ra.published_at, ra.content, ra.metadata, ra.created_at
      FROM research_artifacts ra
      JOIN target t ON t.id = ra.id
      ON CONFLICT (id) DO NOTHING
      RETURNING id
    )
    DELETE FROM research_artifacts ra
    USING moved
    WHERE ra.id = moved.id
    RETURNING ra.id
  `;
  return rows.length;
}

async function archivePipelineStepsChunk(sql, daysOld, batchSize) {
  const rows = await sql`
    WITH target AS (
      SELECT id
      FROM pipeline_steps
      WHERE created_at < NOW() - make_interval(days => ${daysOld})
      ORDER BY created_at
      LIMIT ${batchSize}
      FOR UPDATE SKIP LOCKED
    ),
    moved AS (
      INSERT INTO pipeline_steps_archive (
        id, run_id, stage, attempt, status, runner, provider, model_or_endpoint,
        input_payload, output_payload, metrics, error, started_at, ended_at, created_at
      )
      SELECT
        ps.id, ps.run_id, ps.stage::text, ps.attempt, ps.status::text, ps.runner::text, ps.provider, ps.model_or_endpoint,
        ps.input_payload, ps.output_payload, ps.metrics, ps.error, ps.started_at, ps.ended_at, ps.created_at
      FROM pipeline_steps ps
      JOIN target t ON t.id = ps.id
      ON CONFLICT (id) DO NOTHING
      RETURNING id
    )
    DELETE FROM pipeline_steps ps
    USING moved
    WHERE ps.id = moved.id
    RETURNING ps.id
  `;
  return rows.length;
}

async function pruneApiRateLimitsChunk(sql, daysOld, batchSize) {
  const rows = await sql`
    WITH target AS (
      SELECT limiter_key, window_start
      FROM api_rate_limits
      WHERE window_start < NOW() - make_interval(days => ${daysOld})
      ORDER BY window_start ASC
      LIMIT ${batchSize}
      FOR UPDATE SKIP LOCKED
    )
    DELETE FROM api_rate_limits arl
    USING target t
    WHERE arl.limiter_key = t.limiter_key
      AND arl.window_start = t.window_start
    RETURNING arl.limiter_key
  `;
  return rows.length;
}

module.exports = async (req, res) => {
  if (!requireAdminApiKey(req, res)) return;
  if (!['GET', 'POST'].includes(req.method)) {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const input = req.method === 'POST' ? (req.body || {}) : (req.query || {});
  const dryRun = parseBool(input.dryRun, true);
  const researchDays = parsePositiveInt(input.researchDays, 30, 1, 3650);
  const stepsDays = parsePositiveInt(input.stepsDays, 90, 1, 3650);
  const researchBatchSize = parsePositiveInt(input.researchBatchSize, 500, 1, 10000);
  const stepsBatchSize = parsePositiveInt(input.stepsBatchSize, 1000, 1, 10000);
  const apiRateLimitDays = parsePositiveInt(input.apiRateLimitDays, 14, 1, 3650);
  const apiRateLimitBatchSize = parsePositiveInt(input.apiRateLimitBatchSize, 5000, 1, 50000);
  const refreshAudit = parseBool(input.refreshAudit, !dryRun);
  const auditBatchSize = parsePositiveInt(input.auditBatchSize, 200, 1, 5000);

  try {
    const sql = neon(process.env.DATABASE_URL);
    const warnings = [];

    const hasResearchArtifacts = await tableExists(sql, 'research_artifacts');
    const hasPipelineSteps = await tableExists(sql, 'pipeline_steps');
    const hasPipelineRuns = await tableExists(sql, 'pipeline_runs');
    const hasApiRateLimits = await tableExists(sql, 'api_rate_limits');

    let eligibleResearchArtifacts = 0;
    let eligiblePipelineSteps = 0;
    let eligibleApiRateLimits = 0;

    if (hasResearchArtifacts) {
      eligibleResearchArtifacts = await countEligibleResearchArtifacts(sql, researchDays);
    } else {
      warnings.push('Table research_artifacts not found; skipping artifact eligibility count.');
    }

    if (hasPipelineSteps) {
      eligiblePipelineSteps = await countEligiblePipelineSteps(sql, stepsDays);
    } else {
      warnings.push('Table pipeline_steps not found; skipping step eligibility count.');
    }

    if (hasApiRateLimits) {
      eligibleApiRateLimits = await countEligibleApiRateLimits(sql, apiRateLimitDays);
    } else {
      warnings.push('Table api_rate_limits not found; skipping rate-limit cleanup eligibility count.');
    }

    let archivedResearchArtifacts = 0;
    let archivedPipelineSteps = 0;
    let prunedApiRateLimits = 0;
    let refreshedPipelineRunAudit = 0;
    let auditRefreshAttempted = false;

    if (!dryRun) {
      await ensureArchiveTables(sql);
      if (hasResearchArtifacts) {
        archivedResearchArtifacts = await archiveResearchArtifactsChunk(sql, researchDays, researchBatchSize);
      }
      if (hasPipelineSteps) {
        archivedPipelineSteps = await archivePipelineStepsChunk(sql, stepsDays, stepsBatchSize);
      }
      if (hasApiRateLimits) {
        prunedApiRateLimits = await pruneApiRateLimitsChunk(sql, apiRateLimitDays, apiRateLimitBatchSize);
      }

      if (refreshAudit) {
        auditRefreshAttempted = true;
        if (hasPipelineRuns && hasPipelineSteps) {
          try {
            const rows = await sql`
              SELECT refresh_pipeline_run_audit_batch(${auditBatchSize})::int as "refreshedCount"
            `;
            refreshedPipelineRunAudit = Number(rows?.[0]?.refreshedCount || 0);
          } catch (error) {
            warnings.push('Pipeline run audit refresh function missing or failed; run migration 20260308_15_pipeline_run_audit_hardening_and_usage.sql.');
          }
        } else {
          warnings.push('Tables pipeline_runs/pipeline_steps missing; skipping pipeline run audit refresh.');
        }
      }
    }

    return res.status(200).json({
      ok: true,
      dryRun,
      params: {
        researchDays,
        stepsDays,
        apiRateLimitDays,
        researchBatchSize,
        stepsBatchSize,
        apiRateLimitBatchSize,
        refreshAudit,
        auditBatchSize
      },
      eligibility: {
        researchArtifacts: eligibleResearchArtifacts,
        pipelineSteps: eligiblePipelineSteps,
        apiRateLimits: eligibleApiRateLimits
      },
      archived: {
        researchArtifacts: archivedResearchArtifacts,
        pipelineSteps: archivedPipelineSteps,
        apiRateLimits: prunedApiRateLimits
      },
      audit: {
        attempted: auditRefreshAttempted,
        refreshedRuns: refreshedPipelineRunAudit
      },
      warnings
    });
  } catch (error) {
    console.error('Admin maintenance error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    return res.status(500).json({
      error: 'Failed to run maintenance',
      details: error.message
    });
  }
};
