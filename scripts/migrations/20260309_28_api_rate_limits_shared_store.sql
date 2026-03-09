-- Shared API rate limit counters for public endpoints.
-- This enables consistent limits across serverless/multi-instance deployments.

CREATE TABLE IF NOT EXISTS api_rate_limits (
  limiter_key TEXT NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  request_count INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT api_rate_limits_pk PRIMARY KEY (limiter_key, window_start),
  CONSTRAINT api_rate_limits_count_nonnegative_chk CHECK (request_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_api_rate_limits_window_start
  ON api_rate_limits (window_start);
