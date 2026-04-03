-- Migration 002: Admin core
-- Restores the admin/operator persistence tables required by the current
-- draft, settings, and run-log surfaces.

CREATE TABLE IF NOT EXISTS article_drafts (
  id SERIAL PRIMARY KEY,
  slug TEXT UNIQUE NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  content TEXT,
  section TEXT NOT NULL,
  image TEXT,
  image_caption TEXT,
  image_credit TEXT,
  source_url TEXT,
  source_title TEXT,
  source_published_at TIMESTAMP,
  pub_date TIMESTAMP,
  model TEXT,
  input_tokens INTEGER,
  output_tokens INTEGER,
  total_tokens INTEGER,
  created_via TEXT DEFAULT 'auto',
  status TEXT DEFAULT 'pending_review',
  published_article_id INTEGER REFERENCES articles(id),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_article_drafts_status_created
ON article_drafts(status, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_article_drafts_source_url_unique
ON article_drafts(source_url)
WHERE source_url IS NOT NULL;

CREATE TABLE IF NOT EXISTS admin_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT NOW()
);

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

CREATE INDEX IF NOT EXISTS idx_draft_generation_runs_run_at
ON draft_generation_runs(run_at DESC);

CREATE INDEX IF NOT EXISTS idx_draft_generation_runs_status
ON draft_generation_runs(run_status);
