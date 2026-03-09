CREATE EXTENSION IF NOT EXISTS pg_trgm;

ALTER TABLE articles
ADD COLUMN IF NOT EXISTS search_document tsvector
GENERATED ALWAYS AS (
  setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
  setweight(to_tsvector('english', coalesce(description, '')), 'B') ||
  setweight(to_tsvector('english', regexp_replace(coalesce(content, ''), '<[^>]+>', ' ', 'g')), 'C')
) STORED;

CREATE INDEX IF NOT EXISTS idx_articles_search_document
ON articles
USING GIN (search_document);

CREATE INDEX IF NOT EXISTS idx_articles_title_trgm
ON articles
USING GIN (title gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_articles_description_trgm
ON articles
USING GIN (description gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_articles_section_pubdate
ON articles (section, pub_date DESC);

CREATE OR REPLACE FUNCTION find_related_articles(target_slug TEXT, user_query TEXT DEFAULT NULL, limit_count INT DEFAULT 3)
RETURNS TABLE (
  id BIGINT,
  slug TEXT,
  title TEXT,
  description TEXT,
  section TEXT,
  pub_date TIMESTAMPTZ,
  score NUMERIC
)
LANGUAGE sql
STABLE
AS $$
  WITH params AS (
    SELECT
      NULLIF(trim(coalesce(user_query, '')), '') AS uq,
      CASE
        WHEN NULLIF(trim(coalesce(user_query, '')), '') IS NULL THEN NULL::tsquery
        ELSE websearch_to_tsquery('english', NULLIF(trim(coalesce(user_query, '')), ''))
      END AS q_user
  ),
  target AS (
    SELECT
      a.id,
      a.slug,
      a.title,
      a.description,
      a.section,
      a.search_document,
      CASE
        WHEN NULLIF(trim(concat_ws(' ', coalesce(a.title, ''), coalesce(a.description, ''))), '') IS NULL
          THEN NULL::tsquery
        ELSE websearch_to_tsquery(
          'english',
          trim(
            concat_ws(
              ' ',
              coalesce(a.title, ''),
              coalesce(a.description, '')
            )
          )
        )
      END AS q_article
    FROM articles a
    WHERE (
      target_slug IS NULL
      OR trim(target_slug) = ''
      OR lower(trim(a.slug)) = lower(trim(target_slug))
    )
      AND COALESCE(a.status, 'published') = 'published'
    ORDER BY
      CASE
        WHEN lower(trim(a.slug)) = lower(trim(target_slug)) THEN 0
        ELSE 1
      END,
      a.pub_date DESC
    LIMIT 1
  ),
  ranked AS (
    SELECT
      c.id,
      c.slug,
      c.title,
      c.description,
      c.section,
      c.pub_date,
      (
        -- User query relevance gets top priority when present.
        (CASE WHEN p.q_user IS NULL THEN 0 ELSE 0.50 * COALESCE(ts_rank_cd(c.search_document, p.q_user), 0) END) +
        (CASE WHEN p.uq IS NULL THEN 0 ELSE 0.15 * GREATEST(
          similarity(coalesce(c.title, ''), p.uq),
          similarity(coalesce(c.description, ''), p.uq)
        ) END) +
        -- Article context relevance is always included as a secondary anchor.
        (CASE WHEN t.q_article IS NULL THEN 0 ELSE 0.22 * COALESCE(ts_rank_cd(c.search_document, t.q_article), 0) END) +
        (0.08 * CASE WHEN c.section = t.section THEN 1 ELSE 0 END) +
        (0.05 * CASE
          WHEN c.pub_date >= now() - interval '14 days' THEN 1
          WHEN c.pub_date >= now() - interval '60 days' THEN 0.5
          ELSE 0
        END)
      )::numeric AS score
    FROM articles c
    CROSS JOIN target t
    CROSS JOIN params p
    WHERE lower(trim(c.slug)) <> lower(trim(t.slug))
      AND COALESCE(c.status, 'published') = 'published'
      AND (
        -- If user query exists, prioritize intent match, fallback to article context.
        (
          p.uq IS NOT NULL AND (
            (p.q_user IS NOT NULL AND c.search_document @@ p.q_user)
            OR similarity(coalesce(c.title, ''), p.uq) > 0.20
            OR similarity(coalesce(c.description, ''), p.uq) > 0.20
            OR (t.q_article IS NOT NULL AND c.search_document @@ t.q_article)
          )
        )
        OR
        -- If no user query, pure article-context retrieval.
        (
          p.uq IS NULL AND (
            (t.q_article IS NOT NULL AND c.search_document @@ t.q_article)
            OR similarity(coalesce(c.title, ''), coalesce(t.title, '')) > 0.20
            OR similarity(coalesce(c.description, ''), coalesce(t.description, '')) > 0.20
          )
        )
      )
  )
  SELECT
    r.id,
    r.slug,
    r.title,
    r.description,
    r.section,
    r.pub_date,
    round(r.score, 6) AS score
  FROM ranked r
  ORDER BY r.score DESC, r.pub_date DESC
  LIMIT GREATEST(COALESCE(limit_count, 3), 1);
$$;

CREATE OR REPLACE FUNCTION find_related_articles(target_slug TEXT, limit_count INT)
RETURNS TABLE (
  id BIGINT,
  slug TEXT,
  title TEXT,
  description TEXT,
  section TEXT,
  pub_date TIMESTAMPTZ,
  score NUMERIC
)
LANGUAGE sql
STABLE
AS $$
  SELECT *
  FROM find_related_articles(target_slug, NULL::TEXT, limit_count);
$$;
