-- Phase 4: Story planning versioning + canonical semantics
-- Safe to run multiple times.

-- 0) Remove first-write-wins uniqueness for story plans.
DROP INDEX IF EXISTS uq_research_artifacts_story_plan_signal;

-- 1) Ensure metadata is always an object for story-plan rows.
UPDATE research_artifacts
SET metadata = '{}'::jsonb
WHERE stage = 'story_planning'
  AND artifact_type = 'story_plan'
  AND (metadata IS NULL OR jsonb_typeof(metadata) <> 'object');

-- 2) Backfill version numbers by signal.
WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY signal_id
      ORDER BY created_at ASC, id ASC
    ) AS version_num
  FROM research_artifacts
  WHERE stage = 'story_planning'
    AND artifact_type = 'story_plan'
    AND signal_id IS NOT NULL
)
UPDATE research_artifacts ra
SET metadata = jsonb_set(
  CASE
    WHEN jsonb_typeof(ra.metadata) = 'object' THEN ra.metadata
    ELSE '{}'::jsonb
  END,
  '{version}',
  to_jsonb(r.version_num),
  true
)
FROM ranked r
WHERE ra.id = r.id
  AND COALESCE(ra.metadata->>'version', '') = '';

-- 3) Default legacy rows to validated READY so one latest row can stay canonical until replaced.
UPDATE research_artifacts
SET metadata = jsonb_set(
      jsonb_set(
        CASE
          WHEN jsonb_typeof(metadata) = 'object' THEN metadata
          ELSE '{}'::jsonb
        END,
        '{executionOutcome}',
        '"validated"'::jsonb,
        true
      ),
      '{planningStatus}',
      '"READY"'::jsonb,
      true
    )
WHERE stage = 'story_planning'
  AND artifact_type = 'story_plan'
  AND (
    COALESCE(metadata->>'executionOutcome', '') = ''
    OR COALESCE(metadata->>'planningStatus', '') = ''
  );

-- 4) Clear existing canonical flags, then set only the latest validated READY row canonical per signal.
UPDATE research_artifacts
SET metadata = jsonb_set(
  CASE
    WHEN jsonb_typeof(metadata) = 'object' THEN metadata
    ELSE '{}'::jsonb
  END,
  '{isCanonical}',
  'false'::jsonb,
  true
)
WHERE stage = 'story_planning'
  AND artifact_type = 'story_plan';

WITH latest_ready AS (
  SELECT DISTINCT ON (signal_id)
    id
  FROM research_artifacts
  WHERE stage = 'story_planning'
    AND artifact_type = 'story_plan'
    AND signal_id IS NOT NULL
    AND COALESCE(metadata->>'executionOutcome', '') = 'validated'
    AND COALESCE(metadata->>'planningStatus', '') = 'READY'
  ORDER BY
    signal_id,
    CASE
      WHEN COALESCE(metadata->>'version', '') ~ '^[0-9]+$'
        THEN (metadata->>'version')::int
      ELSE 0
    END DESC,
    created_at DESC,
    id DESC
)
UPDATE research_artifacts ra
SET metadata = jsonb_set(
  CASE
    WHEN jsonb_typeof(ra.metadata) = 'object' THEN ra.metadata
    ELSE '{}'::jsonb
  END,
  '{isCanonical}',
  'true'::jsonb,
  true
)
FROM latest_ready lr
WHERE ra.id = lr.id;

-- 5) Fast lookup indexes.
CREATE INDEX IF NOT EXISTS idx_research_artifacts_story_plan_signal_version
  ON research_artifacts (
    signal_id,
    (
      CASE
        WHEN COALESCE(metadata->>'version', '') ~ '^[0-9]+$'
          THEN (metadata->>'version')::int
        ELSE 0
      END
    ) DESC,
    created_at DESC
  )
  WHERE stage = 'story_planning'
    AND artifact_type = 'story_plan'
    AND signal_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_research_artifacts_story_plan_signal_canonical
  ON research_artifacts (signal_id)
  WHERE stage = 'story_planning'
    AND artifact_type = 'story_plan'
    AND signal_id IS NOT NULL
    AND COALESCE(metadata->>'isCanonical', 'false') = 'true';
