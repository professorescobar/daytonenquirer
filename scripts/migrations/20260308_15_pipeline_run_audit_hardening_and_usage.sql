-- Pipeline run audit hardening + usage helpers.
-- Safe to run multiple times.

CREATE OR REPLACE FUNCTION upsert_pipeline_run_audit(p_run_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  IF to_regclass('public.pipeline_runs') IS NULL
    OR to_regclass('public.pipeline_steps') IS NULL
    OR to_regclass('public.pipeline_run_audit') IS NULL THEN
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
      WHEN pr.status IN ('failed', 'rejected', 'cancelled') THEN 'rejected'
      ELSE 'pending'
    END,
    COALESCE(
      jsonb_agg(DISTINCT jsonb_build_object('provider', COALESCE(ps.provider, ''), 'model', COALESCE(ps.model_or_endpoint, '')))
      FILTER (WHERE ps.id IS NOT NULL),
      '[]'::jsonb
    ),
    COALESCE(
      SUM(
        CASE
          WHEN COALESCE(ps.metrics->>'cost_usd', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN (ps.metrics->>'cost_usd')::numeric
          ELSE 0
        END
      ),
      0
    ),
    COALESCE(
      SUM(
        CASE
          WHEN COALESCE(ps.metrics->>'tokens_in', '') ~ '^[0-9]+$'
            THEN (ps.metrics->>'tokens_in')::bigint
          ELSE 0
        END
      ),
      0
    ),
    COALESCE(
      SUM(
        CASE
          WHEN COALESCE(ps.metrics->>'tokens_out', '') ~ '^[0-9]+$'
            THEN (ps.metrics->>'tokens_out')::bigint
          ELSE 0
        END
      ),
      0
    ),
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

CREATE OR REPLACE FUNCTION refresh_pipeline_run_audit_batch(batch_size INT DEFAULT 200)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  refreshed_count INT := 0;
BEGIN
  IF to_regclass('public.pipeline_runs') IS NULL
    OR to_regclass('public.pipeline_steps') IS NULL
    OR to_regclass('public.pipeline_run_audit') IS NULL THEN
    RETURN 0;
  END IF;

  WITH target_runs AS (
    SELECT pr.id
    FROM pipeline_runs pr
    LEFT JOIN pipeline_run_audit pra ON pra.run_id = pr.id
    WHERE pra.run_id IS NULL
       OR pra.updated_at < COALESCE(pr.ended_at, pr.updated_at, pr.created_at, NOW())
    ORDER BY COALESCE(pr.ended_at, pr.updated_at, pr.created_at, NOW()) DESC
    LIMIT GREATEST(COALESCE(batch_size, 200), 1)
  )
  SELECT COUNT(*)::int INTO refreshed_count
  FROM target_runs;

  PERFORM upsert_pipeline_run_audit(tr.id)
  FROM target_runs tr;

  RETURN refreshed_count;
END;
$$;
