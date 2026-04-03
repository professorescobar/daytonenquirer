-- Migration 001: Public site core
-- Restores the published articles table required by the public site and
-- published-article admin surfaces.

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
);

-- Public site reads are dominated by section/status/date queries.
CREATE INDEX IF NOT EXISTS idx_articles_section_status_pub_date
ON articles(section, status, pub_date DESC);

CREATE INDEX IF NOT EXISTS idx_articles_status_pub_date
ON articles(status, pub_date DESC);
