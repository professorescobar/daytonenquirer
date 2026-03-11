-- Phase B / Chunk 1: root-source ingestion helpers + contract comments
-- Safe to run multiple times.

CREATE INDEX IF NOT EXISTS idx_dictionary_root_sources_due_selection
  ON dictionary.dictionary_root_sources (
    enabled,
    last_crawled_at,
    crawl_cadence_days,
    freshness_sla_days,
    updated_at DESC
  );

CREATE INDEX IF NOT EXISTS idx_dictionary_crawl_artifacts_root_url_fetched
  ON dictionary.dictionary_crawl_artifacts (
    root_source_id,
    lower(source_url),
    fetched_at DESC,
    created_at DESC
  );

CREATE INDEX IF NOT EXISTS idx_dictionary_pipeline_runs_root_stage_created
  ON dictionary.dictionary_pipeline_runs (
    root_source_id,
    lower(stage_name),
    created_at DESC
  )
  WHERE root_source_id IS NOT NULL;

CREATE OR REPLACE FUNCTION dictionary.latest_root_source_artifact(
  p_root_source_id UUID,
  p_source_url TEXT DEFAULT NULL
)
RETURNS TABLE (
  id UUID,
  root_source_id UUID,
  prior_artifact_id UUID,
  source_url TEXT,
  source_domain TEXT,
  content_hash TEXT,
  fetched_at TIMESTAMPTZ,
  http_status INTEGER,
  content_type TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    ca.id,
    ca.root_source_id,
    ca.prior_artifact_id,
    ca.source_url,
    ca.source_domain,
    ca.content_hash,
    ca.fetched_at,
    ca.http_status,
    ca.content_type,
    ca.metadata,
    ca.created_at
  FROM dictionary.dictionary_crawl_artifacts ca
  WHERE ca.root_source_id = p_root_source_id
    AND (
      p_source_url IS NULL
      OR lower(ca.source_url) = lower(p_source_url)
    )
    AND ca.http_status IS NOT NULL
    AND ca.http_status >= 200
    AND ca.http_status < 300
    AND length(trim(ca.content_hash)) > 0
  ORDER BY
    ca.fetched_at DESC,
    ca.created_at DESC,
    ca.id DESC
  LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION dictionary.root_sources_due_for_ingestion(
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
  selection_reason TEXT
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
  source_state AS (
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
      CASE
        WHEN rs.last_crawled_at IS NULL THEN la.last_successful_crawl_at
        WHEN la.last_successful_crawl_at IS NULL THEN rs.last_crawled_at
        ELSE GREATEST(rs.last_crawled_at, la.last_successful_crawl_at)
      END AS last_successful_crawl_at
    FROM dictionary.dictionary_root_sources rs
    LEFT JOIN latest_artifacts la
      ON la.root_source_id = rs.id
    WHERE rs.enabled = true
  )
  SELECT
    ss.root_source_id,
    ss.source_name,
    ss.source_type,
    ss.source_domain,
    ss.root_url,
    ss.trust_tier,
    ss.crawl_cadence_days,
    ss.freshness_sla_days,
    ss.failure_threshold,
    ss.last_successful_crawl_at,
    CASE
      WHEN ss.last_successful_crawl_at IS NULL THEN NULL
      ELSE ROUND(
        (EXTRACT(EPOCH FROM (p_as_of - ss.last_successful_crawl_at)) / 86400.0)::numeric,
        2
      )
    END AS days_since_last_success,
    CASE
      WHEN ss.crawl_cadence_days IS NULL THEN false
      WHEN ss.last_successful_crawl_at IS NULL THEN true
      ELSE ss.last_successful_crawl_at <= p_as_of - make_interval(days => ss.crawl_cadence_days)
    END AS due_by_cadence,
    CASE
      WHEN ss.freshness_sla_days IS NULL THEN false
      WHEN ss.last_successful_crawl_at IS NULL THEN true
      ELSE ss.last_successful_crawl_at <= p_as_of - make_interval(days => ss.freshness_sla_days)
    END AS overdue_by_freshness_sla,
    CASE
      WHEN ss.last_successful_crawl_at IS NULL THEN 'never_crawled'
      WHEN ss.freshness_sla_days IS NOT NULL
        AND ss.last_successful_crawl_at <= p_as_of - make_interval(days => ss.freshness_sla_days)
        THEN 'freshness_overdue'
      WHEN ss.crawl_cadence_days IS NOT NULL
        AND ss.last_successful_crawl_at <= p_as_of - make_interval(days => ss.crawl_cadence_days)
        THEN 'cadence_due'
      ELSE 'not_due'
    END AS selection_reason
  FROM source_state ss
  WHERE
    ss.last_successful_crawl_at IS NULL
    OR (
      ss.crawl_cadence_days IS NOT NULL
      AND ss.last_successful_crawl_at <= p_as_of - make_interval(days => ss.crawl_cadence_days)
    )
    OR (
      ss.freshness_sla_days IS NOT NULL
      AND ss.last_successful_crawl_at <= p_as_of - make_interval(days => ss.freshness_sla_days)
    )
  ORDER BY
    CASE ss.trust_tier
      WHEN 'authoritative' THEN 0
      WHEN 'corroborative' THEN 1
      ELSE 2
    END,
    CASE
      WHEN ss.last_successful_crawl_at IS NULL THEN 0
      WHEN ss.freshness_sla_days IS NOT NULL
        AND ss.last_successful_crawl_at <= p_as_of - make_interval(days => ss.freshness_sla_days)
        THEN 1
      WHEN ss.crawl_cadence_days IS NOT NULL
        AND ss.last_successful_crawl_at <= p_as_of - make_interval(days => ss.crawl_cadence_days)
        THEN 2
      ELSE 3
    END,
    ss.last_successful_crawl_at NULLS FIRST,
    ss.source_name ASC;
$$;

COMMENT ON FUNCTION dictionary.latest_root_source_artifact(UUID, TEXT) IS
  'Returns the latest successful immutable crawl artifact for an approved root source, optionally scoped to one root URL. Phase B ingestion should link new artifacts to this row through prior_artifact_id.';

COMMENT ON FUNCTION dictionary.root_sources_due_for_ingestion(TIMESTAMPTZ) IS
  'Selects enabled root sources due for Phase B ingestion using successful crawl history plus root-source cadence and freshness policy.';

COMMENT ON COLUMN dictionary.dictionary_root_sources.last_crawled_at IS
  'Most recent successful crawl completion recorded for the root source. Due-source helpers also fall back to latest artifact fetched_at when this is null.';

COMMENT ON COLUMN dictionary.dictionary_crawl_artifacts.prior_artifact_id IS
  'Machine-usable link to the immediately previous immutable artifact for the same approved root URL.';

COMMENT ON COLUMN dictionary.dictionary_crawl_artifacts.content_hash IS
  'Deterministic hash of the normalized artifact comparison basis used by ingestion diff detection, not necessarily the raw transport payload.';

COMMENT ON COLUMN dictionary.dictionary_crawl_artifacts.metadata IS
  'Artifact metadata. Phase B ingestion should populate change_state, hash_algorithm, parser diagnostics, and copied source-policy context.';
