-- Phase F / Chunk 1: freshness helpers + source-health selectors
-- Safe to run multiple times.

CREATE INDEX IF NOT EXISTS idx_dictionary_assertions_phase_f_next_review
  ON dictionary.dictionary_assertions (next_review_at, review_status, validity_status, updated_at DESC)
  WHERE next_review_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dictionary_assertions_phase_f_end_dates
  ON dictionary.dictionary_assertions (term_end_at, effective_end_at, validity_status, updated_at DESC)
  WHERE term_end_at IS NOT NULL
     OR effective_end_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dictionary_provenance_assertion_latest
  ON dictionary.dictionary_provenance (record_id, captured_at DESC, created_at DESC)
  WHERE record_type = 'assertion';

CREATE INDEX IF NOT EXISTS idx_dictionary_review_queue_open_source_health
  ON dictionary.dictionary_review_queue (root_source_id, item_type, retry_count DESC, last_failed_at DESC)
  WHERE resolved_at IS NULL
    AND root_source_id IS NOT NULL
    AND item_type IN ('fetch_failure', 'artifact_parse_failure', 'extraction_contract_failure', 'freshness_overdue');

CREATE OR REPLACE FUNCTION dictionary.phase_f_assertion_review_due_at(
  p_last_verified_at TIMESTAMPTZ,
  p_freshness_sla_days INTEGER,
  p_next_review_at TIMESTAMPTZ
)
RETURNS TIMESTAMPTZ
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    p_next_review_at,
    CASE
      WHEN p_last_verified_at IS NOT NULL AND p_freshness_sla_days IS NOT NULL
        THEN p_last_verified_at + make_interval(days => p_freshness_sla_days)
      ELSE NULL
    END
  );
$$;

CREATE OR REPLACE FUNCTION dictionary.phase_f_source_health(
  p_as_of TIMESTAMPTZ DEFAULT NOW()
)
RETURNS TABLE (
  root_source_id UUID,
  source_name TEXT,
  root_url TEXT,
  trust_tier dictionary.source_trust_tier,
  failure_threshold INTEGER,
  last_successful_crawl_at TIMESTAMPTZ,
  open_blocking_failure_count BIGINT,
  blocking_failure_retry_count INTEGER,
  latest_blocking_failed_at TIMESTAMPTZ,
  blocking_item_types TEXT[],
  open_extraction_failure_count BIGINT,
  extraction_failure_retry_count INTEGER,
  latest_extraction_failed_at TIMESTAMPTZ,
  has_blocking_failures BOOLEAN,
  is_blocked BOOLEAN
)
LANGUAGE sql
STABLE
AS $$
  WITH latest_artifacts AS (
    SELECT
      ca.root_source_id,
      MAX(ca.fetched_at) AS last_successful_crawl_at
    FROM dictionary.dictionary_crawl_artifacts ca
    WHERE ca.http_status IS NOT NULL
      AND ca.http_status >= 200
      AND ca.http_status < 300
      AND length(trim(ca.content_hash)) > 0
    GROUP BY ca.root_source_id
  ),
  queue_health AS (
    SELECT
      rq.root_source_id,
      COUNT(*) FILTER (
        WHERE rq.item_type IN ('fetch_failure', 'artifact_parse_failure')
      ) AS open_blocking_failure_count,
      MAX(rq.retry_count) FILTER (
        WHERE rq.item_type IN ('fetch_failure', 'artifact_parse_failure')
      ) AS blocking_failure_retry_count,
      MAX(rq.last_failed_at) FILTER (
        WHERE rq.item_type IN ('fetch_failure', 'artifact_parse_failure')
      ) AS latest_blocking_failed_at,
      COALESCE(
        array_agg(DISTINCT rq.item_type::TEXT) FILTER (
          WHERE rq.item_type IN ('fetch_failure', 'artifact_parse_failure')
        ),
        ARRAY[]::TEXT[]
      ) AS blocking_item_types,
      COUNT(*) FILTER (
        WHERE rq.item_type = 'extraction_contract_failure'
      ) AS open_extraction_failure_count,
      MAX(rq.retry_count) FILTER (
        WHERE rq.item_type = 'extraction_contract_failure'
      ) AS extraction_failure_retry_count,
      MAX(rq.last_failed_at) FILTER (
        WHERE rq.item_type = 'extraction_contract_failure'
      ) AS latest_extraction_failed_at
    FROM dictionary.dictionary_review_queue rq
    WHERE rq.resolved_at IS NULL
      AND rq.root_source_id IS NOT NULL
    GROUP BY rq.root_source_id
  )
  SELECT
    rs.id AS root_source_id,
    rs.source_name,
    rs.root_url,
    rs.trust_tier,
    rs.failure_threshold,
    CASE
      WHEN rs.last_crawled_at IS NULL THEN la.last_successful_crawl_at
      WHEN la.last_successful_crawl_at IS NULL THEN rs.last_crawled_at
      ELSE GREATEST(rs.last_crawled_at, la.last_successful_crawl_at)
    END AS last_successful_crawl_at,
    COALESCE(qh.open_blocking_failure_count, 0) AS open_blocking_failure_count,
    COALESCE(qh.blocking_failure_retry_count, 0) AS blocking_failure_retry_count,
    qh.latest_blocking_failed_at,
    COALESCE(qh.blocking_item_types, ARRAY[]::TEXT[]) AS blocking_item_types,
    COALESCE(qh.open_extraction_failure_count, 0) AS open_extraction_failure_count,
    COALESCE(qh.extraction_failure_retry_count, 0) AS extraction_failure_retry_count,
    qh.latest_extraction_failed_at,
    COALESCE(qh.open_blocking_failure_count, 0) > 0 AS has_blocking_failures,
    COALESCE(qh.blocking_failure_retry_count, 0) >= rs.failure_threshold AS is_blocked
  FROM dictionary.dictionary_root_sources rs
  LEFT JOIN latest_artifacts la
    ON la.root_source_id = rs.id
  LEFT JOIN queue_health qh
    ON qh.root_source_id = rs.id
  WHERE rs.enabled = true;
$$;

CREATE OR REPLACE FUNCTION dictionary.phase_f_assertions_due_for_review(
  p_as_of TIMESTAMPTZ DEFAULT NOW(),
  p_pending_refresh_window INTERVAL DEFAULT INTERVAL '7 days'
)
RETURNS TABLE (
  assertion_id UUID,
  root_source_id UUID,
  crawl_artifact_id UUID,
  source_url TEXT,
  source_domain TEXT,
  trust_tier dictionary.source_trust_tier,
  failure_threshold INTEGER,
  blocking_failure_retry_count INTEGER,
  blocking_item_types TEXT[],
  subject_entity_id UUID,
  object_entity_id UUID,
  role_id UUID,
  assertion_type TEXT,
  validity_status dictionary.assertion_validity_status,
  review_status dictionary.assertion_review_status,
  computed_validity_status dictionary.assertion_validity_status,
  computed_review_status dictionary.assertion_review_status,
  last_verified_at TIMESTAMPTZ,
  freshness_sla_days INTEGER,
  next_review_at TIMESTAMPTZ,
  review_due_at TIMESTAMPTZ,
  pending_refresh_at TIMESTAMPTZ,
  effective_end_at TIMESTAMPTZ,
  term_end_at TIMESTAMPTZ,
  superseded_by_assertion_id UUID,
  latest_provenance_captured_at TIMESTAMPTZ,
  is_high_impact BOOLEAN,
  is_pending_refresh BOOLEAN,
  is_overdue BOOLEAN,
  is_blocked BOOLEAN,
  expires_without_successor BOOLEAN,
  overdue_by INTERVAL
)
LANGUAGE sql
STABLE
AS $$
  WITH latest_provenance AS (
    SELECT DISTINCT ON (p.record_id)
      p.record_id AS assertion_id,
      p.root_source_id,
      p.crawl_artifact_id,
      p.source_url,
      p.source_domain,
      p.trust_tier,
      p.captured_at
    FROM dictionary.dictionary_provenance p
    WHERE p.record_type = 'assertion'
    ORDER BY p.record_id, p.captured_at DESC, p.created_at DESC, p.id DESC
  ),
  base AS (
    SELECT
      a.id AS assertion_id,
      lp.root_source_id,
      lp.crawl_artifact_id,
      lp.source_url,
      lp.source_domain,
      lp.trust_tier,
      sh.failure_threshold,
      sh.blocking_failure_retry_count,
      sh.blocking_item_types,
      COALESCE(sh.is_blocked, false) AS root_source_blocked,
      a.subject_entity_id,
      a.object_entity_id,
      a.role_id,
      a.assertion_type,
      a.validity_status,
      a.review_status,
      a.last_verified_at,
      a.freshness_sla_days,
      a.next_review_at,
      dictionary.phase_f_assertion_review_due_at(
        a.last_verified_at,
        a.freshness_sla_days,
        a.next_review_at
      ) AS review_due_at,
      a.effective_end_at,
      a.term_end_at,
      a.superseded_by_assertion_id,
      lp.captured_at AS latest_provenance_captured_at
    FROM dictionary.dictionary_assertions a
    LEFT JOIN latest_provenance lp
      ON lp.assertion_id = a.id
    LEFT JOIN dictionary.phase_f_source_health(p_as_of) sh
      ON sh.root_source_id = lp.root_source_id
  ),
  evaluated AS (
    SELECT
      b.*,
      (b.role_id IS NOT NULL) AS is_high_impact,
      CASE
        WHEN b.validity_status = 'superseded'
          OR b.superseded_by_assertion_id IS NOT NULL
          THEN 'superseded'::dictionary.assertion_validity_status
        WHEN (
          (b.effective_end_at IS NOT NULL AND b.effective_end_at <= p_as_of)
          OR (b.term_end_at IS NOT NULL AND b.term_end_at <= p_as_of)
        )
          THEN 'expired'::dictionary.assertion_validity_status
        ELSE b.validity_status
      END AS computed_validity_status,
      CASE
        WHEN b.review_due_at IS NOT NULL
          THEN b.review_due_at - p_pending_refresh_window
        ELSE NULL
      END AS pending_refresh_at,
      CASE
        WHEN b.review_due_at IS NOT NULL
         AND b.review_due_at > p_as_of
         AND b.review_due_at <= p_as_of + p_pending_refresh_window
          THEN true
        ELSE false
      END AS is_pending_refresh,
      CASE
        WHEN b.review_due_at IS NOT NULL
         AND b.review_due_at <= p_as_of
          THEN true
        ELSE false
      END AS is_overdue,
      CASE
        WHEN b.review_due_at IS NOT NULL
         AND b.review_due_at <= p_as_of
         AND b.root_source_blocked
          THEN true
        ELSE false
      END AS is_blocked,
      CASE
        WHEN b.role_id IS NOT NULL
         AND b.superseded_by_assertion_id IS NULL
         AND (
           (b.effective_end_at IS NOT NULL AND b.effective_end_at <= p_as_of)
           OR (b.term_end_at IS NOT NULL AND b.term_end_at <= p_as_of)
         )
          THEN true
        ELSE false
      END AS expires_without_successor
    FROM base b
  )
  SELECT
    e.assertion_id,
    e.root_source_id,
    e.crawl_artifact_id,
    e.source_url,
    e.source_domain,
    e.trust_tier,
    e.failure_threshold,
    COALESCE(e.blocking_failure_retry_count, 0) AS blocking_failure_retry_count,
    COALESCE(e.blocking_item_types, ARRAY[]::TEXT[]) AS blocking_item_types,
    e.subject_entity_id,
    e.object_entity_id,
    e.role_id,
    e.assertion_type,
    e.validity_status,
    e.review_status,
    e.computed_validity_status,
    CASE
      WHEN e.is_blocked THEN 'blocked'::dictionary.assertion_review_status
      WHEN e.is_overdue THEN 'needs_review'::dictionary.assertion_review_status
      WHEN e.is_pending_refresh THEN 'pending_refresh'::dictionary.assertion_review_status
      WHEN e.review_due_at IS NOT NULL THEN 'verified'::dictionary.assertion_review_status
      ELSE e.review_status
    END AS computed_review_status,
    e.last_verified_at,
    e.freshness_sla_days,
    e.next_review_at,
    e.review_due_at,
    e.pending_refresh_at,
    e.effective_end_at,
    e.term_end_at,
    e.superseded_by_assertion_id,
    e.latest_provenance_captured_at,
    e.is_high_impact,
    e.is_pending_refresh,
    e.is_overdue,
    e.is_blocked,
    e.expires_without_successor,
    CASE
      WHEN e.review_due_at IS NOT NULL AND e.review_due_at <= p_as_of
        THEN p_as_of - e.review_due_at
      ELSE NULL
    END AS overdue_by
  FROM evaluated e
  WHERE e.review_due_at IS NOT NULL
     OR e.expires_without_successor
  ORDER BY
    e.is_blocked DESC,
    e.is_overdue DESC,
    e.expires_without_successor DESC,
    e.review_due_at NULLS LAST,
    e.assertion_id ASC;
$$;

CREATE OR REPLACE FUNCTION dictionary.phase_f_root_sources_requiring_attention(
  p_as_of TIMESTAMPTZ DEFAULT NOW()
)
RETURNS TABLE (
  root_source_id UUID,
  source_name TEXT,
  source_type TEXT,
  source_domain TEXT,
  root_url TEXT,
  trust_tier dictionary.source_trust_tier,
  crawl_cadence_days INTEGER,
  freshness_sla_days INTEGER,
  failure_threshold INTEGER,
  last_successful_crawl_at TIMESTAMPTZ,
  days_since_last_success NUMERIC,
  due_by_cadence BOOLEAN,
  overdue_by_freshness_sla BOOLEAN,
  open_blocking_failure_count BIGINT,
  blocking_failure_retry_count INTEGER,
  blocking_item_types TEXT[],
  open_extraction_failure_count BIGINT,
  extraction_failure_retry_count INTEGER,
  is_blocked BOOLEAN,
  attention_reason TEXT,
  should_dispatch_refresh BOOLEAN
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
  )
  SELECT
    rs.id AS root_source_id,
    rs.source_name,
    rs.source_type,
    rs.source_domain,
    rs.root_url,
    rs.trust_tier,
    rs.crawl_cadence_days,
    rs.freshness_sla_days,
    rs.failure_threshold,
    COALESCE(due.last_successful_crawl_at, sh.last_successful_crawl_at) AS last_successful_crawl_at,
    due.days_since_last_success,
    COALESCE(due.due_by_cadence, false) AS due_by_cadence,
    COALESCE(due.overdue_by_freshness_sla, false) AS overdue_by_freshness_sla,
    COALESCE(sh.open_blocking_failure_count, 0) AS open_blocking_failure_count,
    COALESCE(sh.blocking_failure_retry_count, 0) AS blocking_failure_retry_count,
    COALESCE(sh.blocking_item_types, ARRAY[]::TEXT[]) AS blocking_item_types,
    COALESCE(sh.open_extraction_failure_count, 0) AS open_extraction_failure_count,
    COALESCE(sh.extraction_failure_retry_count, 0) AS extraction_failure_retry_count,
    COALESCE(sh.is_blocked, false) AS is_blocked,
    CASE
      WHEN COALESCE(sh.is_blocked, false) THEN 'blocking_failures'
      WHEN COALESCE(due.overdue_by_freshness_sla, false) THEN 'freshness_overdue'
      WHEN COALESCE(sh.open_extraction_failure_count, 0) > 0 THEN 'extraction_contract_failure'
      WHEN COALESCE(due.due_by_cadence, false) THEN 'cadence_due'
      ELSE 'attention_required'
    END AS attention_reason,
    CASE
      WHEN due.root_source_id IS NOT NULL AND COALESCE(sh.is_blocked, false) = false THEN true
      ELSE false
    END AS should_dispatch_refresh
  FROM dictionary.dictionary_root_sources rs
  LEFT JOIN due
    ON due.root_source_id = rs.id
  LEFT JOIN source_health sh
    ON sh.root_source_id = rs.id
  WHERE rs.enabled = true
    AND (
      due.root_source_id IS NOT NULL
      OR COALESCE(sh.is_blocked, false)
      OR COALESCE(sh.open_extraction_failure_count, 0) > 0
    )
  ORDER BY
    COALESCE(sh.is_blocked, false) DESC,
    COALESCE(due.overdue_by_freshness_sla, false) DESC,
    COALESCE(due.due_by_cadence, false) DESC,
    rs.source_name ASC;
$$;

COMMENT ON FUNCTION dictionary.phase_f_assertion_review_due_at(TIMESTAMPTZ, INTEGER, TIMESTAMPTZ) IS
  'Phase F helper for the canonical-head review due boundary. It prefers explicit next_review_at and otherwise derives a due timestamp from last_verified_at plus freshness_sla_days.';

COMMENT ON FUNCTION dictionary.phase_f_source_health(TIMESTAMPTZ) IS
  'Summarizes enabled root-source freshness health from existing crawl history and unresolved review-queue failures. Phase F blocked-state semantics are tied to unresolved fetch/artifact-parse failures at or above failure_threshold.';

COMMENT ON FUNCTION dictionary.phase_f_assertions_due_for_review(TIMESTAMPTZ, INTERVAL) IS
  'Evaluates canonical-head assertions for Phase F maintenance. First-pass semantics are canonical-head only, use a fixed 7-day pending_refresh lookahead by default, and do not imply snapshot publish.';

COMMENT ON FUNCTION dictionary.phase_f_root_sources_requiring_attention(TIMESTAMPTZ) IS
  'Returns enabled root sources that are due, freshness-overdue, extraction-failing, or operationally blocked so Phase F can surface review work and safely choose bounded root-source refresh dispatch.';
