-- Enforce article slug uniqueness with duplicate-safe cleanup.
-- Non-destructive: only adjusts duplicate slugs by suffixing with article id.
-- Uses CONCURRENTLY for index build to reduce lock contention on busy tables.

DO $$
BEGIN
  IF to_regclass('public.articles') IS NULL THEN
    RETURN;
  END IF;

  -- De-dupe existing non-empty slugs so unique index can be created safely.
  WITH ranked_slugs AS (
    SELECT
      id,
      slug,
      ROW_NUMBER() OVER (
        PARTITION BY lower(trim(slug))
        ORDER BY id ASC
      ) AS rn
    FROM articles
    WHERE slug IS NOT NULL
      AND trim(slug) <> ''
  )
  UPDATE articles a
  SET
    slug = CONCAT(trim(a.slug), '-', a.id::text),
    updated_at = NOW()
  FROM ranked_slugs r
  WHERE a.id = r.id
    AND r.rn > 1;

END $$;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_articles_slug_norm
  ON articles ((lower(trim(slug))))
  WHERE slug IS NOT NULL
    AND trim(slug) <> '';
