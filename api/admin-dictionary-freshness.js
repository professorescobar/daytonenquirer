const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const PHASE_F_SCAN_EVENT = 'dictionary.substrate.freshness.scan';

function cleanText(value, max = 1000) {
  return String(value || '').trim().slice(0, max);
}

function parseBool(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function parsePositiveInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
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

async function dictionarySchemaReady(sql) {
  const rows = await sql`SELECT to_regclass('dictionary.dictionary_review_queue') AS reg`;
  return Boolean(rows?.[0]?.reg);
}

async function listReviewItems(sql, { limit, itemType, rootSourceId, openOnly }) {
  return sql`
    SELECT
      rq.id,
      rq.item_type as "itemType",
      rq.severity,
      rq.root_source_id as "rootSourceId",
      rs.source_name as "rootSourceName",
      rs.root_url as "rootSourceUrl",
      rq.crawl_artifact_id as "crawlArtifactId",
      rq.pipeline_run_id as "pipelineRunId",
      rq.affected_record_type as "affectedRecordType",
      rq.affected_record_id as "affectedRecordId",
      rq.retry_count as "retryCount",
      rq.last_error as "lastError",
      rq.suggested_action as "suggestedAction",
      rq.first_failed_at as "firstFailedAt",
      rq.last_failed_at as "lastFailedAt",
      rq.resolved_at as "resolvedAt",
      rq.created_at as "createdAt",
      rq.updated_at as "updatedAt"
    FROM dictionary.dictionary_review_queue rq
    LEFT JOIN dictionary.dictionary_root_sources rs
      ON rs.id = rq.root_source_id
    WHERE (${openOnly}::boolean = false OR rq.resolved_at IS NULL)
      AND (${itemType || null}::text IS NULL OR rq.item_type = ${itemType || null}::dictionary.review_queue_item_type)
      AND (${rootSourceId || null}::uuid IS NULL OR rq.root_source_id = ${rootSourceId || null}::uuid)
    ORDER BY
      rq.resolved_at NULLS FIRST,
      rq.last_failed_at DESC,
      rq.created_at DESC
    LIMIT ${limit}
  `;
}

async function buildReviewSummary(sql) {
  const rows = await sql`
    SELECT
      COUNT(*) FILTER (WHERE resolved_at IS NULL) as "openItemCount",
      COUNT(*) FILTER (WHERE resolved_at IS NULL AND severity = 'critical') as "criticalOpenCount",
      COUNT(*) FILTER (WHERE resolved_at IS NULL AND severity = 'high') as "highOpenCount",
      COUNT(*) FILTER (WHERE resolved_at IS NULL AND item_type = 'freshness_overdue') as "freshnessOverdueOpenCount",
      COUNT(*) FILTER (WHERE resolved_at IS NULL AND item_type = 'expired_high_impact_assertion') as "expiredHighImpactOpenCount"
    FROM dictionary.dictionary_review_queue
  `;
  return rows[0] || {
    openItemCount: 0,
    criticalOpenCount: 0,
    highOpenCount: 0,
    freshnessOverdueOpenCount: 0,
    expiredHighImpactOpenCount: 0
  };
}

async function listAttentionRootSources(sql, limit) {
  return sql`
    SELECT
      root_source_id as "rootSourceId",
      source_name as "sourceName",
      source_type as "sourceType",
      source_domain as "sourceDomain",
      root_url as "rootUrl",
      trust_tier as "trustTier",
      freshness_sla_days as "freshnessSlaDays",
      failure_threshold as "failureThreshold",
      last_successful_crawl_at as "lastSuccessfulCrawlAt",
      days_since_last_success as "daysSinceLastSuccess",
      due_by_cadence as "dueByCadence",
      overdue_by_freshness_sla as "overdueByFreshnessSla",
      open_blocking_failure_count as "openBlockingFailureCount",
      blocking_failure_retry_count as "blockingFailureRetryCount",
      blocking_item_types as "blockingItemTypes",
      open_extraction_failure_count as "openExtractionFailureCount",
      extraction_failure_retry_count as "extractionFailureRetryCount",
      is_blocked as "isBlocked",
      attention_reason as "attentionReason",
      should_dispatch_refresh as "shouldDispatchRefresh"
    FROM dictionary.phase_f_root_sources_requiring_attention()
    ORDER BY
      is_blocked DESC,
      overdue_by_freshness_sla DESC,
      should_dispatch_refresh DESC,
      source_name ASC
    LIMIT ${limit}
  `;
}

async function listRecentPhaseFRuns(sql, limit) {
  return sql`
    SELECT
      id,
      status,
      trigger_type as "triggerType",
      input_payload as "inputPayload",
      output_payload as "outputPayload",
      error_payload as "errorPayload",
      created_at as "createdAt",
      started_at as "startedAt",
      ended_at as "endedAt"
    FROM dictionary.dictionary_pipeline_runs
    WHERE lower(stage_name) = 'phase_f_freshness_review'
    ORDER BY created_at DESC, id DESC
    LIMIT ${limit}
  `;
}

async function emitPhaseFScanEvent(req, payload) {
  const endpoint = resolveInngestEventEndpoint(req);
  if (!endpoint) {
    return { attempted: false, sent: false, reason: 'missing_inngest_event_url' };
  }

  const key = cleanText(process.env.INNGEST_EVENT_KEY || '', 600);
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(key ? { Authorization: `Bearer ${key}` } : {})
      },
      body: JSON.stringify({
        name: PHASE_F_SCAN_EVENT,
        data: payload
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

  if (!['GET', 'POST'].includes(req.method)) {
    res.setHeader('Allow', ['GET', 'POST']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const schemaReady = await dictionarySchemaReady(sql);
    if (!schemaReady) {
      return res.status(503).json({
        error: 'Dictionary substrate schema missing',
        details: 'Run the dictionary Phase A migrations first.'
      });
    }

    if (req.method === 'GET') {
      const limit = parsePositiveInt(req.query?.limit, 25, 1, 100);
      const rootLimit = parsePositiveInt(req.query?.rootLimit, 10, 1, 50);
      const runLimit = parsePositiveInt(req.query?.runLimit, 10, 1, 25);
      const openOnly = parseBool(req.query?.openOnly ?? req.query?.open_only, true);
      const itemType = cleanText(req.query?.itemType || req.query?.item_type || '', 80) || null;
      const rootSourceId = cleanText(req.query?.rootSourceId || req.query?.root_source_id || '', 80) || null;

      if (rootSourceId && !UUID_RE.test(rootSourceId)) {
        return res.status(400).json({ error: 'rootSourceId must be a valid UUID' });
      }

      const [summary, reviewItems, attentionRoots, recentRuns] = await Promise.all([
        buildReviewSummary(sql),
        listReviewItems(sql, { limit, itemType, rootSourceId, openOnly }),
        listAttentionRootSources(sql, rootLimit),
        listRecentPhaseFRuns(sql, runLimit)
      ]);

      return res.status(200).json({
        filters: {
          limit,
          rootLimit,
          runLimit,
          openOnly,
          itemType,
          rootSourceId
        },
        summary,
        reviewItems,
        attentionRoots,
        recentRuns
      });
    }

    const dispatchRefreshes = parseBool(req.body?.dispatchRefreshes ?? req.body?.dispatch_refreshes, true);
    const refreshLimit = parsePositiveInt(req.body?.refreshLimit ?? req.body?.refresh_limit, 10, 1, 50);
    const refreshCooldownHours = parsePositiveInt(
      req.body?.refreshCooldownHours ?? req.body?.refresh_cooldown_hours,
      24,
      1,
      168
    );

    const eventPayload = {
      trigger: 'manual',
      dispatchRefreshes,
      refreshLimit,
      refreshCooldownHours
    };

    const eventResult = await emitPhaseFScanEvent(req, eventPayload);
    if (!eventResult.sent) {
      return res.status(400).json({
        ok: false,
        error: eventResult.reason || 'dictionary_freshness_scan_dispatch_failed',
        event: eventResult
      });
    }

    return res.status(200).json({
      ok: true,
      triggerMode: 'event',
      event: eventResult,
      payload: eventPayload
    });
  } catch (error) {
    console.error('Admin dictionary freshness error:', error);
    const message = String(error?.message || '');
    if (message.includes('valid UUID')) {
      return res.status(400).json({ error: message });
    }
    if (
      message.includes('dictionary.phase_f_root_sources_requiring_attention')
      || message.includes('dictionary.phase_f_assertions_due_for_review')
    ) {
      return res.status(503).json({
        error: 'Dictionary Phase F helpers missing',
        details: 'Run migration 20260313_47_dictionary_phase_f_chunk1_freshness_helpers.sql first.'
      });
    }
    return res.status(500).json({ error: 'Failed to manage dictionary freshness review state' });
  }
};
