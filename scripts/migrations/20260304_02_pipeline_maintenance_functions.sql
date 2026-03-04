-- Chunked maintenance functions.
-- These are safe to run repeatedly and no-op when source tables are missing.

CREATE OR REPLACE FUNCTION archive_research_artifacts_chunk(days_old INT, batch_size INT DEFAULT 500)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  moved_count BIGINT := 0;
BEGIN
  IF to_regclass('public.research_artifacts') IS NULL THEN
    RETURN 0;
  END IF;

  WITH target AS (
    SELECT ra.id
    FROM research_artifacts ra
    WHERE ra.created_at < NOW() - (days_old || ' days')::interval
    ORDER BY ra.created_at
    LIMIT batch_size
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
  WHERE ra.id = moved.id;

  GET DIAGNOSTICS moved_count = ROW_COUNT;
  RETURN moved_count;
END;
$$;

CREATE OR REPLACE FUNCTION archive_pipeline_steps_chunk(days_old INT, batch_size INT DEFAULT 1000)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  moved_count BIGINT := 0;
BEGIN
  IF to_regclass('public.pipeline_steps') IS NULL THEN
    RETURN 0;
  END IF;

  WITH target AS (
    SELECT ps.id
    FROM pipeline_steps ps
    WHERE ps.created_at < NOW() - (days_old || ' days')::interval
    ORDER BY ps.created_at
    LIMIT batch_size
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
  WHERE ps.id = moved.id;

  GET DIAGNOSTICS moved_count = ROW_COUNT;
  RETURN moved_count;
END;
$$;

CREATE OR REPLACE FUNCTION upsert_pipeline_run_audit(p_run_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  IF to_regclass('public.pipeline_runs') IS NULL OR to_regclass('public.pipeline_steps') IS NULL THEN
    RETURN;
  END IF;

  INSERT INTO pipeline_run_audit (
    run_id, engine_id, candidate_id, final_status, final_stage, publish_decision,
    providers_used, total_cost_usd, total_tokens_in, total_tokens_out,
    started_at, ended_at, created_at, updated_at
  )
  SELECT
    pr.id,
    pr.engine_id,
    pr.candidate_id,
    pr.status::text,
    pr.active_stage::text,
    CASE
      WHEN pr.status = 'succeeded' THEN 'approved'
      WHEN pr.status = 'held' THEN 'held'
      ELSE 'rejected'
    END,
    COALESCE(
      jsonb_agg(DISTINCT jsonb_build_object('provider', ps.provider, 'model', ps.model_or_endpoint))
      FILTER (WHERE ps.id IS NOT NULL),
      '[]'::jsonb
    ),
    COALESCE(SUM(COALESCE((ps.metrics->>'cost_usd')::numeric, 0)), 0),
    COALESCE(SUM(COALESCE((ps.metrics->>'tokens_in')::bigint, 0)), 0),
    COALESCE(SUM(COALESCE((ps.metrics->>'tokens_out')::bigint, 0)), 0),
    pr.started_at,
    pr.ended_at,
    pr.created_at,
    NOW()
  FROM pipeline_runs pr
  LEFT JOIN pipeline_steps ps ON ps.run_id = pr.id
  WHERE pr.id = p_run_id
  GROUP BY pr.id, pr.engine_id, pr.candidate_id, pr.status, pr.active_stage, pr.started_at, pr.ended_at, pr.created_at
  ON CONFLICT (run_id) DO UPDATE SET
    final_status = EXCLUDED.final_status,
    final_stage = EXCLUDED.final_stage,
    publish_decision = EXCLUDED.publish_decision,
    providers_used = EXCLUDED.providers_used,
    total_cost_usd = EXCLUDED.total_cost_usd,
    total_tokens_in = EXCLUDED.total_tokens_in,
    total_tokens_out = EXCLUDED.total_tokens_out,
    started_at = EXCLUDED.started_at,
    ended_at = EXCLUDED.ended_at,
    updated_at = NOW();
END;
$$;

