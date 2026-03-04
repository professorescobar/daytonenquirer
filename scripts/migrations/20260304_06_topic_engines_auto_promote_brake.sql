CREATE TABLE IF NOT EXISTS topic_engines (
  persona_id VARCHAR(255) PRIMARY KEY,
  is_auto_promote_enabled BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE topic_engines
  ADD COLUMN IF NOT EXISTS is_auto_promote_enabled BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE topic_engines
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE topic_engines
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

INSERT INTO topic_engines (persona_id, is_auto_promote_enabled, created_at, updated_at)
SELECT p.id, false, NOW(), NOW()
FROM personas p
LEFT JOIN topic_engines te ON te.persona_id = p.id
WHERE te.persona_id IS NULL
ON CONFLICT (persona_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_topic_engines_auto_promote
  ON topic_engines (is_auto_promote_enabled, updated_at DESC);
