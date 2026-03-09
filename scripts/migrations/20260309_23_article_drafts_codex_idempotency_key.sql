DO $$
BEGIN
  IF to_regclass('public.article_drafts') IS NULL THEN
    RAISE NOTICE 'Skipping 20260309_23: article_drafts table does not exist';
    RETURN;
  END IF;

  ALTER TABLE article_drafts
    ADD COLUMN IF NOT EXISTS codex_idempotency_key text;

  UPDATE article_drafts
  SET codex_idempotency_key = lower(
    regexp_replace(
      substring(source_url FROM '^codex://automation/(.+)$'),
      '[^a-z0-9:_-]+',
      '-',
      'g'
    )
  )
  WHERE created_via = 'codex_automation'
    AND (codex_idempotency_key IS NULL OR btrim(codex_idempotency_key) = '')
    AND source_url ~ '^codex://automation/.+';

  WITH ranked AS (
    SELECT
      id,
      row_number() OVER (
        PARTITION BY lower(trim(codex_idempotency_key))
        ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
      ) AS rn
    FROM article_drafts
    WHERE created_via = 'codex_automation'
      AND codex_idempotency_key IS NOT NULL
      AND btrim(codex_idempotency_key) <> ''
  )
  UPDATE article_drafts d
  SET codex_idempotency_key = NULL
  FROM ranked r
  WHERE d.id = r.id
    AND r.rn > 1;

  CREATE INDEX IF NOT EXISTS idx_article_drafts_codex_idempotency_key
    ON article_drafts(codex_idempotency_key);

  DROP INDEX IF EXISTS uq_article_drafts_codex_idempotency_key;

  CREATE UNIQUE INDEX IF NOT EXISTS uq_article_drafts_codex_idempotency_key
    ON article_drafts((lower(trim(codex_idempotency_key))))
    WHERE created_via = 'codex_automation'
      AND codex_idempotency_key IS NOT NULL
      AND btrim(codex_idempotency_key) <> '';
END $$;
