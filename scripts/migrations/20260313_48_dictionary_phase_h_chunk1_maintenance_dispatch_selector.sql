-- Phase H / Chunk 1: maintenance-mode dispatch selector
-- Safe to run multiple times.

CREATE INDEX IF NOT EXISTS idx_dictionary_pipeline_runs_phase_b_root_active
  ON dictionary.dictionary_pipeline_runs (root_source_id, created_at DESC)
  WHERE root_source_id IS NOT NULL
    AND lower(stage_name) = 'phase_b_root_ingestion'
    AND status = 'running';

CREATE OR REPLACE FUNCTION dictionary.phase_h_maintenance_dispatch_candidates(
  p_as_of TIMESTAMPTZ DEFAULT NOW(),
  p_recent_run_cooldown INTERVAL DEFAULT INTERVAL '24 hours'
)
RETURNS TABLE (
  root_source_id UUID,
  source_name TEXT,
  source_type TEXT,
  source_domain TEXT,
  root_url TEXT,
  trust_tier dictionary.source_trust_tier,
  enabled BOOLEAN,
  crawl_cadence_days INTEGER,
  freshness_sla_days INTEGER,
  failure_threshold INTEGER,
  due_by_cadence BOOLEAN,
  overdue_by_freshness_sla BOOLEAN,
  due_selection_reason TEXT,
  should_dispatch BOOLEAN,
  skip_reason TEXT,
  active_run_id UUID,
  active_run_created_at TIMESTAMPTZ,
  recent_run_id UUID,
  recent_run_status dictionary.pipeline_run_status,
  recent_run_created_at TIMESTAMPTZ,
  blocking_failure_retry_count INTEGER,
  blocking_failure_item_types TEXT[],
  extraction_failure_retry_count INTEGER,
  extraction_failure_open_count BIGINT,
  review_blocker_open_count BIGINT,
  review_blocker_item_types TEXT[]
)
LANGUAGE sql
STABLE
AS $$
  WITH due AS (
    SELECT *
    FROM dictionary.root_sources_due_for_ingestion(p_as_of)
  ),
  source_health AS (
    SELECT *
    FROM dictionary.phase_f_source_health(p_as_of)
  ),
  active_runs AS (
    SELECT DISTINCT ON (pr.root_source_id)
      pr.root_source_id,
      pr.id,
      pr.created_at
    FROM dictionary.dictionary_pipeline_runs pr
    WHERE pr.root_source_id IS NOT NULL
      AND lower(pr.stage_name) = 'phase_b_root_ingestion'
      AND pr.status = 'running'
    ORDER BY pr.root_source_id, pr.created_at DESC, pr.id DESC
  ),
  recent_runs AS (
    SELECT DISTINCT ON (pr.root_source_id)
      pr.root_source_id,
      pr.id,
      pr.status,
      pr.created_at
    FROM dictionary.dictionary_pipeline_runs pr
    WHERE pr.root_source_id IS NOT NULL
      AND lower(pr.stage_name) = 'phase_b_root_ingestion'
      AND pr.created_at >= p_as_of - p_recent_run_cooldown
    ORDER BY pr.root_source_id, pr.created_at DESC, pr.id DESC
  ),
  extraction_review AS (
    SELECT
      rq.root_source_id,
      COUNT(*) AS extraction_failure_open_count,
      MAX(rq.retry_count) AS extraction_failure_retry_count
    FROM dictionary.dictionary_review_queue rq
    WHERE rq.resolved_at IS NULL
      AND rq.root_source_id IS NOT NULL
      AND rq.item_type = 'extraction_contract_failure'
    GROUP BY rq.root_source_id
  ),
  review_blockers AS (
    SELECT
      rq.root_source_id,
      COUNT(*) AS review_blocker_open_count,
      COALESCE(array_agg(DISTINCT rq.item_type::TEXT), ARRAY[]::TEXT[]) AS review_blocker_item_types
    FROM dictionary.dictionary_review_queue rq
    WHERE rq.resolved_at IS NULL
      AND rq.root_source_id IS NOT NULL
      AND rq.item_type IN ('merge_ambiguity', 'validation_failure', 'promotion_blocked')
    GROUP BY rq.root_source_id
  )
  SELECT
    rs.id AS root_source_id,
    rs.source_name,
    rs.source_type,
    rs.source_domain,
    rs.root_url,
    rs.trust_tier,
    rs.enabled,
    rs.crawl_cadence_days,
    rs.freshness_sla_days,
    rs.failure_threshold,
    COALESCE(due.due_by_cadence, false) AS due_by_cadence,
    COALESCE(due.overdue_by_freshness_sla, false) AS overdue_by_freshness_sla,
    COALESCE(due.selection_reason, 'not_due') AS due_selection_reason,
    CASE
      WHEN NOT rs.enabled THEN false
      WHEN due.root_source_id IS NULL THEN false
      WHEN ar.id IS NOT NULL THEN false
      WHEN COALESCE(sh.is_blocked, false) THEN false
      WHEN COALESCE(er.extraction_failure_retry_count, 0) >= rs.failure_threshold THEN false
      WHEN COALESCE(rb.review_blocker_open_count, 0) > 0 THEN false
      WHEN rr.id IS NOT NULL THEN false
      ELSE true
    END AS should_dispatch,
    CASE
      WHEN NOT rs.enabled THEN 'disabled'
      WHEN due.root_source_id IS NULL THEN 'not_due'
      WHEN ar.id IS NOT NULL THEN 'active_run'
      WHEN COALESCE(sh.is_blocked, false) THEN 'failure_blocked'
      WHEN COALESCE(er.extraction_failure_retry_count, 0) >= rs.failure_threshold THEN 'failure_blocked'
      WHEN COALESCE(rb.review_blocker_open_count, 0) > 0 THEN 'review_blocked'
      WHEN rr.id IS NOT NULL THEN 'recent_run_cooldown'
      ELSE NULL
    END AS skip_reason,
    ar.id AS active_run_id,
    ar.created_at AS active_run_created_at,
    rr.id AS recent_run_id,
    rr.status AS recent_run_status,
    rr.created_at AS recent_run_created_at,
    COALESCE(sh.blocking_failure_retry_count, 0) AS blocking_failure_retry_count,
    COALESCE(sh.blocking_item_types, ARRAY[]::TEXT[]) AS blocking_failure_item_types,
    COALESCE(er.extraction_failure_retry_count, 0) AS extraction_failure_retry_count,
    COALESCE(er.extraction_failure_open_count, 0) AS extraction_failure_open_count,
    COALESCE(rb.review_blocker_open_count, 0) AS review_blocker_open_count,
    COALESCE(rb.review_blocker_item_types, ARRAY[]::TEXT[]) AS review_blocker_item_types
  FROM dictionary.dictionary_root_sources rs
  LEFT JOIN due
    ON due.root_source_id = rs.id
  LEFT JOIN source_health sh
    ON sh.root_source_id = rs.id
  LEFT JOIN active_runs ar
    ON ar.root_source_id = rs.id
  LEFT JOIN recent_runs rr
    ON rr.root_source_id = rs.id
  LEFT JOIN extraction_review er
    ON er.root_source_id = rs.id
  LEFT JOIN review_blockers rb
    ON rb.root_source_id = rs.id
  ORDER BY
    should_dispatch DESC,
    COALESCE(due.overdue_by_freshness_sla, false) DESC,
    COALESCE(due.due_by_cadence, false) DESC,
    rs.source_name ASC;
$$;

COMMENT ON FUNCTION dictionary.phase_h_maintenance_dispatch_candidates(TIMESTAMPTZ, INTERVAL) IS
  'Phase H maintenance-mode root selection. Returns machine-usable dispatch eligibility and narrow skip reasons without changing earlier substrate policy contracts. Active phase_b_root_ingestion runs are hard skips; extraction contract failures suppress automation only after thresholded repeated open failures.';
