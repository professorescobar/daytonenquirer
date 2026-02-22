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
  
  console.log('✅ Database tables created/verified successfully!');
}

setupDatabase().catch(console.error);
