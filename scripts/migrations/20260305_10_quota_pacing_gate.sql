-- Phase 0.5: Quota + Pacing Gate
-- Safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS topic_engine_pacing (
  persona_id VARCHAR(255) PRIMARY KEY,
  enabled BOOLEAN NOT NULL DEFAULT false,
  posting_days BOOLEAN[] NOT NULL DEFAULT ARRAY[true, true, true, true, true, true, true],
  posts_per_active_day INT NOT NULL DEFAULT 1
    CHECK (posts_per_active_day >= 0 AND posts_per_active_day <= 24),
  window_start_local TIME NOT NULL DEFAULT TIME '06:00',
  window_end_local TIME NOT NULL DEFAULT TIME '22:00',
  cadence_enabled BOOLEAN NOT NULL DEFAULT true,
  single_post_time_local TIME,
  single_post_daypart TEXT,
  CONSTRAINT topic_engine_pacing_single_daypart_chk
    CHECK (single_post_daypart IS NULL OR single_post_daypart IN ('morning','midday','afternoon','evening')),
  min_spacing_minutes INT NOT NULL DEFAULT 90
    CHECK (min_spacing_minutes >= 0 AND min_spacing_minutes <= 1440),
  max_backlog INT NOT NULL DEFAULT 200
    CHECK (max_backlog >= 1 AND max_backlog <= 5000),
  max_retries INT NOT NULL DEFAULT 3
    CHECK (max_retries >= 0 AND max_retries <= 20),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT topic_engine_pacing_posting_days_len_chk
    CHECK (array_length(posting_days, 1) = 7)
);

CREATE INDEX IF NOT EXISTS idx_topic_engine_pacing_enabled
  ON topic_engine_pacing (enabled, updated_at DESC);

CREATE TABLE IF NOT EXISTS topic_engine_release_queue (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  signal_id BIGINT NOT NULL,
  persona_id VARCHAR(255) NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  reason_code TEXT NOT NULL DEFAULT '',
  source_event TEXT NOT NULL DEFAULT 'signal.received',
  scheduled_for_utc TIMESTAMPTZ,
  scheduled_day_local DATE,
  released_at TIMESTAMPTZ,
  released_day_local DATE,
  attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT topic_engine_release_queue_status_chk
    CHECK (status IN ('queued','released','deferred','rejected','failed','expired'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_engine_release_queue_signal
  ON topic_engine_release_queue (signal_id);

CREATE INDEX IF NOT EXISTS idx_topic_engine_release_queue_status_sched
  ON topic_engine_release_queue (status, scheduled_for_utc NULLS LAST, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_topic_engine_release_queue_persona_status
  ON topic_engine_release_queue (persona_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_engine_release_queue_persona_local_day
  ON topic_engine_release_queue (persona_id, released_day_local, status);

CREATE INDEX IF NOT EXISTS idx_topic_engine_release_queue_local_day_status
  ON topic_engine_release_queue (released_day_local, status);

INSERT INTO system_settings (key, value, updated_at)
VALUES
  ('topic_engine_admin_timezone', '"America/New_York"'::jsonb, NOW()),
  ('topic_engine_global_daily_cap', '100'::jsonb, NOW()),
  ('topic_engine_kill_switch_enabled', 'false'::jsonb, NOW())
ON CONFLICT (key) DO NOTHING;
