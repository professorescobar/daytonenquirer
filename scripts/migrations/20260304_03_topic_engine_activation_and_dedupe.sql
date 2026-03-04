-- Topic engine activation modes + shared dedupe storage for event and scheduled triggers.
-- Safe to run multiple times.

ALTER TABLE personas
  ADD COLUMN IF NOT EXISTS activation_mode TEXT DEFAULT 'both';

UPDATE personas
SET activation_mode = 'both'
WHERE activation_mode IS NULL OR trim(activation_mode) = '';

CREATE TABLE IF NOT EXISTS topic_engine_feeds (
  id SERIAL PRIMARY KEY,
  persona_id VARCHAR(255) NOT NULL,
  feed_url TEXT NOT NULL,
  source_name TEXT,
  priority INTEGER DEFAULT 100,
  enabled BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(persona_id, feed_url)
);

CREATE INDEX IF NOT EXISTS idx_topic_engine_feeds_persona_enabled
  ON topic_engine_feeds(persona_id, enabled, priority);

CREATE TABLE IF NOT EXISTS topic_engine_candidates (
  id SERIAL PRIMARY KEY,
  persona_id VARCHAR(255) NOT NULL,
  trigger_mode TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT,
  snippet TEXT,
  source_name TEXT,
  source_url TEXT,
  published_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'discovered',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(persona_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_topic_engine_candidates_persona_status_created
  ON topic_engine_candidates(persona_id, status, created_at DESC);

