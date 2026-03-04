-- Pipeline maintenance archive/audit tables
-- Safe to run multiple times.

CREATE TABLE IF NOT EXISTS research_artifacts_archive (
  id UUID PRIMARY KEY,
  run_id UUID NOT NULL,
  engine_id UUID NOT NULL,
  candidate_id UUID NOT NULL,
  stage TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  source_url TEXT,
  source_domain TEXT,
  title TEXT,
  published_at TIMESTAMPTZ,
  content TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_artifacts_archive_engine_created
  ON research_artifacts_archive(engine_id, created_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_steps_archive (
  id UUID PRIMARY KEY,
  run_id UUID NOT NULL,
  stage TEXT NOT NULL,
  attempt SMALLINT NOT NULL,
  status TEXT NOT NULL,
  runner TEXT NOT NULL,
  provider TEXT NOT NULL,
  model_or_endpoint TEXT NOT NULL,
  input_payload JSONB NOT NULL,
  output_payload JSONB NOT NULL,
  metrics JSONB NOT NULL,
  error TEXT,
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_steps_archive_run_created
  ON pipeline_steps_archive(run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_run_audit (
  run_id UUID PRIMARY KEY,
  engine_id UUID NOT NULL,
  candidate_id UUID NOT NULL,
  final_status TEXT NOT NULL,
  final_stage TEXT,
  publish_decision TEXT,
  providers_used JSONB NOT NULL DEFAULT '[]'::jsonb,
  total_cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
  total_tokens_in BIGINT NOT NULL DEFAULT 0,
  total_tokens_out BIGINT NOT NULL DEFAULT 0,
  reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Optional bridge column for published article lineage.
ALTER TABLE articles
  ADD COLUMN IF NOT EXISTS pipeline_run_id UUID;

CREATE INDEX IF NOT EXISTS idx_articles_pipeline_run_id
  ON articles(pipeline_run_id);

