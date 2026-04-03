-- Migration 003: Newsletters
-- Restores the newsletter campaign and event tables required by the current
-- admin newsletter composer and provider-status flows.

CREATE TABLE IF NOT EXISTS newsletter_campaigns (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  subject TEXT NOT NULL DEFAULT '',
  preview_text TEXT DEFAULT '',
  description TEXT DEFAULT '',
  content_html TEXT DEFAULT '',
  content_text TEXT DEFAULT '',
  segment_ids JSONB DEFAULT '[]'::jsonb,
  tag_ids JSONB DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'draft',
  kit_broadcast_id BIGINT,
  kit_status TEXT,
  kit_progress INTEGER DEFAULT 0,
  send_at TIMESTAMP,
  sent_at TIMESTAMP,
  last_synced_at TIMESTAMP,
  created_by TEXT DEFAULT 'admin_ui',
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_newsletter_campaigns_created_at
ON newsletter_campaigns(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_newsletter_campaigns_status
ON newsletter_campaigns(status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_newsletter_campaigns_kit_broadcast_id
ON newsletter_campaigns(kit_broadcast_id)
WHERE kit_broadcast_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS newsletter_events (
  id SERIAL PRIMARY KEY,
  campaign_id INTEGER REFERENCES newsletter_campaigns(id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_newsletter_events_campaign_created
ON newsletter_events(campaign_id, created_at DESC);
