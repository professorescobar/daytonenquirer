-- Layer 2 research discovery artifacts table.
-- Safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS research_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL,
  engine_id UUID NOT NULL,
  candidate_id UUID NOT NULL,
  signal_id BIGINT,
  persona_id VARCHAR(255),
  stage TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  source_url TEXT,
  source_domain TEXT,
  title TEXT,
  published_at TIMESTAMPTZ,
  content TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT research_artifacts_stage_nonempty_chk
    CHECK (length(trim(stage)) > 0),
  CONSTRAINT research_artifacts_artifact_type_nonempty_chk
    CHECK (length(trim(artifact_type)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_research_artifacts_run_stage_created
  ON research_artifacts(run_id, stage, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_artifacts_engine_candidate_created
  ON research_artifacts(engine_id, candidate_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_artifacts_signal_created
  ON research_artifacts(signal_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_artifacts_persona_created
  ON research_artifacts(persona_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_artifacts_source_domain_published
  ON research_artifacts(source_domain, published_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_research_artifacts_run_type_url
  ON research_artifacts(run_id, artifact_type, source_url)
  WHERE source_url IS NOT NULL;
