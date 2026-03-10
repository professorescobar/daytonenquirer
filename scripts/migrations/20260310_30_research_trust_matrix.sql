DO $$
BEGIN
  CREATE TABLE IF NOT EXISTS topic_engine_research_trust (
    id SERIAL PRIMARY KEY,
    persona_id VARCHAR(255),
    section TEXT,
    beat TEXT,
    domain TEXT NOT NULL,
    trust_tier TEXT NOT NULL DEFAULT 'trusted',
    is_official BOOLEAN NOT NULL DEFAULT false,
    priority INTEGER NOT NULL DEFAULT 100,
    enabled BOOLEAN NOT NULL DEFAULT true,
    notes TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT topic_engine_research_trust_domain_nonempty_chk
      CHECK (length(trim(domain)) > 0),
    CONSTRAINT topic_engine_research_trust_priority_chk
      CHECK (priority >= 1 AND priority <= 10000)
  );

  CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_engine_research_trust_persona_domain
    ON topic_engine_research_trust (
      COALESCE(persona_id, ''),
      COALESCE(section, ''),
      COALESCE(beat, ''),
      lower(domain)
    );

  CREATE INDEX IF NOT EXISTS idx_topic_engine_research_trust_lookup
    ON topic_engine_research_trust (persona_id, enabled, priority, updated_at DESC);
END $$;
