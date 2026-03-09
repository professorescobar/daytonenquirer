DO $$
BEGIN
  CREATE TABLE IF NOT EXISTS topic_engine_chat_feedback (
    id BIGSERIAL PRIMARY KEY,
    article_id BIGINT,
    article_slug TEXT NOT NULL,
    section TEXT,
    persona_id TEXT,
    user_query TEXT NOT NULL,
    assistant_answer TEXT NOT NULL,
    out_of_scope BOOLEAN NOT NULL DEFAULT false,
    suggested_topic TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  CREATE INDEX IF NOT EXISTS idx_topic_engine_chat_feedback_slug_created
    ON topic_engine_chat_feedback(article_slug, created_at DESC);
END $$;
