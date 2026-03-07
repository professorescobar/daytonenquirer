-- Phase 4: Story Planning DB contract (draft for review)
-- Safe to run multiple times.

-- 0) Extend topic_signals next_step contract for Layer 4 routing.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'topic_signals_next_step_chk'
  ) THEN
    ALTER TABLE topic_signals
      DROP CONSTRAINT topic_signals_next_step_chk;
  END IF;

  ALTER TABLE topic_signals
    ADD CONSTRAINT topic_signals_next_step_chk
    CHECK (next_step IN ('none', 'research_discovery', 'cluster_update', 'story_planning')) NOT VALID;

  ALTER TABLE topic_signals
    VALIDATE CONSTRAINT topic_signals_next_step_chk;
END $$;

-- 1) Clean up duplicate story-plan rows per signal, keeping the newest.
WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY signal_id
      ORDER BY created_at DESC, id DESC
    ) AS rn
  FROM research_artifacts
  WHERE stage = 'story_planning'
    AND artifact_type = 'story_plan'
    AND signal_id IS NOT NULL
)
DELETE FROM research_artifacts ra
USING ranked r
WHERE ra.id = r.id
  AND r.rn > 1;

-- 2) Ensure one story-plan artifact per signal.
CREATE UNIQUE INDEX IF NOT EXISTS uq_research_artifacts_story_plan_signal
  ON research_artifacts (signal_id)
  WHERE stage = 'story_planning'
    AND artifact_type = 'story_plan'
    AND signal_id IS NOT NULL;

-- 3) Fast retrieval path for latest story plan by signal.
CREATE INDEX IF NOT EXISTS idx_research_artifacts_story_plan_signal_created
  ON research_artifacts (signal_id, created_at DESC)
  WHERE stage = 'story_planning'
    AND artifact_type = 'story_plan'
    AND signal_id IS NOT NULL;

-- 4) Contract check for story-plan rows (source_url + metadata.plan object present).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'research_artifacts_story_plan_contract_chk'
  ) THEN
    ALTER TABLE research_artifacts
      ADD CONSTRAINT research_artifacts_story_plan_contract_chk
      CHECK (
        stage <> 'story_planning'
        OR artifact_type <> 'story_plan'
        OR (
          signal_id IS NOT NULL
          AND
          source_url IS NOT NULL
          AND source_url LIKE 'signal://%/story-plan'
          AND jsonb_typeof(metadata) = 'object'
          AND jsonb_typeof(metadata->'plan') = 'object'
        )
      ) NOT VALID;

    ALTER TABLE research_artifacts
      VALIDATE CONSTRAINT research_artifacts_story_plan_contract_chk;
  END IF;
END $$;
