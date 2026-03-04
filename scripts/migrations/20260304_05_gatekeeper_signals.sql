CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS topic_signals (
  id BIGSERIAL PRIMARY KEY,
  persona_id VARCHAR(255) NOT NULL,

  source_type TEXT NOT NULL,
  source_name TEXT,
  source_url TEXT,
  external_id TEXT,

  title TEXT NOT NULL,
  snippet TEXT,
  section_hint TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  dedupe_key TEXT NOT NULL,

  is_newsworthy NUMERIC(4,3),
  is_local BOOLEAN,
  confidence NUMERIC(4,3),
  category TEXT,
  event_key TEXT,
  relation_to_archive TEXT,
  action TEXT NOT NULL DEFAULT 'pending',
  next_step TEXT NOT NULL DEFAULT 'none',
  policy_flags TEXT[] NOT NULL DEFAULT '{}',
  reasoning TEXT,

  review_decision TEXT NOT NULL DEFAULT 'pending_review',
  review_notes TEXT NOT NULL DEFAULT '',
  processed_at TIMESTAMPTZ,

  session_hash TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT topic_signals_source_type_chk
    CHECK (source_type IN ('rss', 'webhook', 'chat_yes', 'chat_specify')),
  CONSTRAINT topic_signals_action_chk
    CHECK (action IN ('pending', 'watch', 'promote', 'reject')),
  CONSTRAINT topic_signals_next_step_chk
    CHECK (next_step IN ('none', 'research_discovery', 'cluster_update')),
  CONSTRAINT topic_signals_relation_chk
    CHECK (relation_to_archive IS NULL OR relation_to_archive IN ('none', 'duplicate', 'update', 'follow_up')),
  CONSTRAINT topic_signals_review_decision_chk
    CHECK (review_decision IN ('pending_review', 'promoted', 'rejected')),
  CONSTRAINT topic_signals_confidence_chk
    CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  CONSTRAINT topic_signals_newsworthy_chk
    CHECK (is_newsworthy IS NULL OR (is_newsworthy >= 0 AND is_newsworthy <= 1))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_signals_persona_dedupe
  ON topic_signals (persona_id, dedupe_key);

CREATE INDEX IF NOT EXISTS idx_topic_signals_persona_action_created
  ON topic_signals (persona_id, action, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_signals_eventkey_created
  ON topic_signals (event_key, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_signals_source_created
  ON topic_signals (source_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_signals_review_created
  ON topic_signals (review_decision, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_signals_pending_watch
  ON topic_signals (created_at DESC)
  WHERE action IN ('pending', 'watch');
