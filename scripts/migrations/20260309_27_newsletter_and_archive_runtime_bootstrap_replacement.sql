DO $$
BEGIN
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

  CREATE INDEX IF NOT EXISTS idx_research_artifacts_archive_engine_created
    ON research_artifacts_archive(engine_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_pipeline_steps_archive_run_created
    ON pipeline_steps_archive(run_id, created_at DESC);
END $$;
