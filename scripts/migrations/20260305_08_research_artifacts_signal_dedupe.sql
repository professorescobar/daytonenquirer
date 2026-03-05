-- Strengthen research artifact dedupe semantics across reruns for the same signal.
-- Safe to run multiple times.

-- Remove exact duplicates while keeping the newest row per key.
WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY signal_id, stage, artifact_type, source_url
      ORDER BY created_at DESC, id DESC
    ) AS rn
  FROM research_artifacts
  WHERE source_url IS NOT NULL
)
DELETE FROM research_artifacts ra
USING ranked r
WHERE ra.id = r.id
  AND r.rn > 1;

DROP INDEX IF EXISTS uq_research_artifacts_run_type_url;

CREATE UNIQUE INDEX IF NOT EXISTS uq_research_artifacts_signal_stage_type_url
  ON research_artifacts(signal_id, stage, artifact_type, source_url);
