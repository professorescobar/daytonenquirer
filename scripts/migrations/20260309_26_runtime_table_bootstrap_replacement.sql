DO $$
BEGIN
  CREATE TABLE IF NOT EXISTS topic_engines (
    persona_id VARCHAR(255) PRIMARY KEY,
    is_auto_promote_enabled BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  CREATE TABLE IF NOT EXISTS topic_engine_stage_configs (
    id SERIAL PRIMARY KEY,
    persona_id VARCHAR(255) NOT NULL,
    stage_name TEXT NOT NULL,
    runner_type TEXT NOT NULL DEFAULT 'llm',
    provider TEXT NOT NULL DEFAULT '',
    model_or_endpoint TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT true,
    prompt_template TEXT NOT NULL DEFAULT '',
    workflow_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(persona_id, stage_name)
  );

  CREATE INDEX IF NOT EXISTS idx_topic_engine_stage_configs_persona
    ON topic_engine_stage_configs(persona_id, stage_name);

  ALTER TABLE personas ADD COLUMN IF NOT EXISTS display_name TEXT;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS section TEXT DEFAULT 'local';
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS beat TEXT DEFAULT 'general-local';

  CREATE TABLE IF NOT EXISTS system_settings (
    key TEXT PRIMARY KEY,
    value JSONB,
    updated_at TIMESTAMP DEFAULT NOW()
  );

  CREATE TABLE IF NOT EXISTS media_library (
    id SERIAL PRIMARY KEY,
    section TEXT NOT NULL DEFAULT 'entertainment',
    beat TEXT,
    persona TEXT,
    title TEXT,
    description TEXT,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    entities JSONB NOT NULL DEFAULT '[]'::jsonb,
    tone TEXT,
    image_url TEXT NOT NULL,
    image_public_id TEXT,
    credit TEXT,
    license_type TEXT,
    license_source_url TEXT,
    approved BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
  );

  CREATE INDEX IF NOT EXISTS idx_media_library_section_approved
    ON media_library(section, approved, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_media_library_persona
    ON media_library(persona);
  CREATE INDEX IF NOT EXISTS idx_media_library_beat
    ON media_library(beat);

  CREATE TABLE IF NOT EXISTS draft_generation_runs (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMP DEFAULT NOW(),
    run_status TEXT NOT NULL,
    run_reason TEXT,
    schedule_mode TEXT,
    track TEXT,
    run_mode TEXT,
    created_via TEXT,
    dry_run BOOLEAN DEFAULT false,
    include_sections TEXT,
    exclude_sections TEXT,
    active_sections TEXT,
    et_date TEXT,
    et_time TEXT,
    requested_count INTEGER,
    target_count INTEGER,
    created_count INTEGER DEFAULT 0,
    skipped_count INTEGER DEFAULT 0,
    daily_token_budget INTEGER,
    tokens_used_today INTEGER DEFAULT 0,
    run_tokens_consumed INTEGER DEFAULT 0,
    writer_provider TEXT,
    writer_model TEXT,
    top_skip_reasons JSONB DEFAULT '[]'::jsonb
  );

  ALTER TABLE draft_generation_runs
    ADD COLUMN IF NOT EXISTS writer_provider TEXT;
  ALTER TABLE draft_generation_runs
    ADD COLUMN IF NOT EXISTS writer_model TEXT;
  ALTER TABLE draft_generation_runs
    ADD COLUMN IF NOT EXISTS top_skip_reasons JSONB DEFAULT '[]'::jsonb;

  CREATE INDEX IF NOT EXISTS idx_draft_generation_runs_run_at
    ON draft_generation_runs(run_at DESC);
  CREATE INDEX IF NOT EXISTS idx_draft_generation_runs_status
    ON draft_generation_runs(run_status);
END $$;
