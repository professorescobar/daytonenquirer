require('dotenv').config();
const { neon } = require('@neondatabase/serverless');

async function setupDatabase() {
  const sql = neon(process.env.DATABASE_URL);
  
  // Create articles table
  await sql`
    CREATE TABLE IF NOT EXISTS articles (
      id SERIAL PRIMARY KEY,
      slug TEXT UNIQUE NOT NULL,
      title TEXT NOT NULL,
      description TEXT,
      content TEXT,
      section TEXT NOT NULL,
      image TEXT,
      image_caption TEXT,
      image_credit TEXT,
      pub_date TIMESTAMP NOT NULL,
      status TEXT DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
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
    )
  `;

  await sql`
    ALTER TABLE article_drafts
    ADD COLUMN IF NOT EXISTS input_tokens INTEGER
  `;

  await sql`
    ALTER TABLE article_drafts
    ADD COLUMN IF NOT EXISTS output_tokens INTEGER
  `;

  await sql`
    ALTER TABLE article_drafts
    ADD COLUMN IF NOT EXISTS total_tokens INTEGER
  `;

  await sql`
    ALTER TABLE article_drafts
    ADD COLUMN IF NOT EXISTS created_via TEXT DEFAULT 'auto'
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_article_drafts_status_created
    ON article_drafts(status, created_at DESC)
  `;

  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_article_drafts_source_url_unique
    ON article_drafts(source_url)
    WHERE source_url IS NOT NULL
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS admin_settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
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
      top_skip_reasons JSONB DEFAULT '[]'::jsonb
    )
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_draft_generation_runs_run_at
    ON draft_generation_runs(run_at DESC)
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_draft_generation_runs_status
    ON draft_generation_runs(run_status)
  `;

  await sql`
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
    )
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_newsletter_campaigns_created_at
    ON newsletter_campaigns(created_at DESC)
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_newsletter_campaigns_status
    ON newsletter_campaigns(status)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS newsletter_events (
      id SERIAL PRIMARY KEY,
      campaign_id INTEGER REFERENCES newsletter_campaigns(id) ON DELETE CASCADE,
      provider TEXT NOT NULL,
      event_type TEXT NOT NULL,
      payload_json JSONB NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_newsletter_events_campaign_created
    ON newsletter_events(campaign_id, created_at DESC)
  `;

  // Legacy moderation memory was removed from the workflow.
  await sql`DROP TABLE IF EXISTS duplicate_reports`;
  await sql`DROP TABLE IF EXISTS editorial_rejections`;
  
  console.log('✅ Database tables created/verified successfully!');
}

setupDatabase().catch(console.error);
