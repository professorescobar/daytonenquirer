-- Forward-fix for environments where 20260304_04 was already applied.
-- Replaces related-article function bodies with normalized slug matching.

DO $$
BEGIN
  IF to_regclass('public.articles') IS NULL THEN
    RETURN;
  END IF;

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
  AS $fn$
    WITH params AS (
      SELECT
        NULLIF(trim(coalesce(user_query, '')), '') AS uq,
        NULLIF(lower(trim(coalesce(target_slug, ''))), '') AS target_slug_norm,
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
      CROSS JOIN params p
      WHERE p.target_slug_norm IS NOT NULL
        AND lower(trim(a.slug)) = p.target_slug_norm
        AND COALESCE(a.status, 'published') = 'published'
      ORDER BY a.pub_date DESC
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
          (CASE WHEN p.q_user IS NULL THEN 0 ELSE 0.50 * COALESCE(ts_rank_cd(c.search_document, p.q_user), 0) END) +
          (CASE WHEN p.uq IS NULL THEN 0 ELSE 0.15 * GREATEST(
            similarity(coalesce(c.title, ''), p.uq),
            similarity(coalesce(c.description, ''), p.uq)
          ) END) +
          (CASE WHEN t.q_article IS NULL THEN 0 ELSE 0.22 * COALESCE(ts_rank_cd(c.search_document, t.q_article), 0) END) +
          (0.08 * CASE WHEN c.section = t.section THEN 1 ELSE 0 END) +
          (0.05 * CASE
            WHEN c.pub_date >= now() - interval '14 days' THEN 1
            WHEN c.pub_date >= now() - interval '60 days' THEN 0.5
            ELSE 0
          END)
        )::numeric AS score
      FROM articles c
      CROSS JOIN params p
      LEFT JOIN target t ON TRUE
      WHERE (t.id IS NULL OR lower(trim(c.slug)) <> lower(trim(t.slug)))
        AND COALESCE(c.status, 'published') = 'published'
        AND (
          (
            p.uq IS NOT NULL AND (
              (p.q_user IS NOT NULL AND c.search_document @@ p.q_user)
              OR similarity(coalesce(c.title, ''), p.uq) > 0.20
              OR similarity(coalesce(c.description, ''), p.uq) > 0.20
              OR (t.id IS NOT NULL AND t.q_article IS NOT NULL AND c.search_document @@ t.q_article)
            )
          )
          OR
          (
            p.uq IS NULL AND t.id IS NOT NULL AND (
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
  $fn$;

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
  AS $fn$
    SELECT *
    FROM find_related_articles(target_slug, NULL::TEXT, limit_count);
  $fn$;
END $$;
