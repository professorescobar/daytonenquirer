-- Phase 6: image pipeline run/candidate telemetry tables
-- Safe to run multiple times. Non-destructive.

CREATE TABLE IF NOT EXISTS image_pipeline_runs (
  id UUID PRIMARY KEY,
  signal_id BIGINT,
  persona_id TEXT,
  article_draft_artifact_id TEXT,
  started_at TIMESTAMP NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMP,
  status TEXT NOT NULL DEFAULT 'running',
  final_outcome TEXT,
  selected_candidate_id UUID,
  selected_tier TEXT,
  selected_image_url TEXT,
  selected_image_credit TEXT,
  selected_source_url TEXT,
  selected_cloudinary_public_id TEXT,
  selected_cloudinary_secure_url TEXT,
  selected_cloudinary_asset_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  attempts_postgres INTEGER NOT NULL DEFAULT 0,
  attempts_exa INTEGER NOT NULL DEFAULT 0,
  attempts_generation INTEGER NOT NULL DEFAULT 0,
  latency_ms_total INTEGER NOT NULL DEFAULT 0,
  cost_usd_estimated NUMERIC(10,4) NOT NULL DEFAULT 0,
  budget_usd_limit NUMERIC(10,4),
  timeout_seconds_limit INTEGER,
  rejection_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS image_candidates (
  id UUID PRIMARY KEY,
  run_id UUID NOT NULL,
  signal_id BIGINT,
  persona_id TEXT,
  candidate_tier TEXT NOT NULL,
  candidate_source TEXT,
  source_url TEXT,
  image_url TEXT,
  image_title TEXT,
  image_credit TEXT,
  cloudinary_public_id TEXT,
  cloudinary_secure_url TEXT,
  cloudinary_asset_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  context_score NUMERIC(6,3),
  quality_score NUMERIC(6,3),
  trust_score NUMERIC(6,3),
  weighted_score NUMERIC(6,3),
  score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence NUMERIC(6,3),
  is_selected BOOLEAN NOT NULL DEFAULT FALSE,
  selected_rank INTEGER,
  rejected BOOLEAN NOT NULL DEFAULT FALSE,
  rejection_reason TEXT,
  rejection_details JSONB NOT NULL DEFAULT '{}'::jsonb,
  attempt_number INTEGER NOT NULL DEFAULT 1,
  latency_ms INTEGER NOT NULL DEFAULT 0,
  cost_usd_estimated NUMERIC(10,4) NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT image_candidates_run_fk
    FOREIGN KEY (run_id)
    REFERENCES image_pipeline_runs(id)
    ON DELETE CASCADE
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_pipeline_runs_status_chk'
  ) THEN
    ALTER TABLE image_pipeline_runs
      ADD CONSTRAINT image_pipeline_runs_status_chk
      CHECK (status IN ('running', 'completed', 'failed', 'timed_out')) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_pipeline_runs_outcome_chk'
  ) THEN
    ALTER TABLE image_pipeline_runs
      ADD CONSTRAINT image_pipeline_runs_outcome_chk
      CHECK (
        final_outcome IS NULL
        OR final_outcome IN (
          'postgres_selected',
          'exa_selected',
          'generated_selected',
          'persona_fallback',
          'text_only'
        )
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_candidates_tier_chk'
  ) THEN
    ALTER TABLE image_candidates
      ADD CONSTRAINT image_candidates_tier_chk
      CHECK (
        candidate_tier IN ('postgres_pass1', 'postgres_pass2', 'exa', 'generated', 'persona_fallback')
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_candidates_nonnegative_chk'
  ) THEN
    ALTER TABLE image_candidates
      ADD CONSTRAINT image_candidates_nonnegative_chk
      CHECK (
        attempt_number >= 1
        AND latency_ms >= 0
        AND cost_usd_estimated >= 0
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM image_pipeline_runs
    WHERE status NOT IN ('running', 'completed', 'failed', 'timed_out')
       OR (
         final_outcome IS NOT NULL
         AND final_outcome NOT IN ('postgres_selected', 'exa_selected', 'generated_selected', 'persona_fallback', 'text_only')
       )
  ) THEN
    ALTER TABLE image_pipeline_runs VALIDATE CONSTRAINT image_pipeline_runs_status_chk;
    ALTER TABLE image_pipeline_runs VALIDATE CONSTRAINT image_pipeline_runs_outcome_chk;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM image_candidates
    WHERE candidate_tier NOT IN ('postgres_pass1', 'postgres_pass2', 'exa', 'generated', 'persona_fallback')
       OR attempt_number < 1
       OR latency_ms < 0
       OR cost_usd_estimated < 0
  ) THEN
    ALTER TABLE image_candidates VALIDATE CONSTRAINT image_candidates_tier_chk;
    ALTER TABLE image_candidates VALIDATE CONSTRAINT image_candidates_nonnegative_chk;
  END IF;
END $$;
