const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const ET_TIME_ZONE = 'America/New_York';

function normalizeComparableTitle(title) {
  return String(title || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function normalizeImageStatus(value) {
  const status = String(value || '').trim();
  return status === 'with_image' || status === 'text_only' ? status : '';
}

function getEtPartsFromDate(date) {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone: ET_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  const parts = dtf.formatToParts(date);
  const out = {};
  for (const part of parts) {
    if (part.type !== 'literal') out[part.type] = part.value;
  }
  return out;
}

function etLocalToUtcIso(localValue) {
  if (!localValue) return null;
  const match = String(localValue).match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/
  );
  if (!match) return null;

  const [, y, mo, d, h, mi] = match;
  const year = Number(y);
  const month = Number(mo);
  const day = Number(d);
  const hour = Number(h);
  const minute = Number(mi);

  const guessUtc = Date.UTC(year, month - 1, day, hour + 5, minute, 0);
  const windowStart = guessUtc - 12 * 60 * 60 * 1000;
  const windowEnd = guessUtc + 12 * 60 * 60 * 1000;

  for (let t = windowStart; t <= windowEnd; t += 60 * 1000) {
    const p = getEtPartsFromDate(new Date(t));
    if (
      Number(p.year) === year &&
      Number(p.month) === month &&
      Number(p.day) === day &&
      Number(p.hour) === hour &&
      Number(p.minute) === minute
    ) {
      return new Date(t).toISOString();
    }
  }

  return null;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const { id, publishAtEt, imageStatus } = req.body || {};
    const requestedImageStatus = normalizeImageStatus(imageStatus);

    if (!id) {
      return res.status(400).json({ error: 'Missing draft id' });
    }

    let publishAtIso = new Date().toISOString();
    const publishAtEtTrimmed = String(publishAtEt || '').trim();
    if (publishAtEtTrimmed) {
      const converted = etLocalToUtcIso(publishAtEtTrimmed);
      if (!converted) {
        return res.status(400).json({ error: 'Invalid ET publish date format' });
      }
      publishAtIso = converted;
    }

    const rows = await sql`
      SELECT
        id,
        slug,
        title,
        description,
        content,
        section,
        beat,
        persona,
        image,
        image_caption as "imageCaption",
        image_credit as "imageCredit",
        status
      FROM article_drafts
      WHERE id = ${id}
      LIMIT 1
    `;
    const draft = rows[0];

    if (!draft) return res.status(404).json({ error: 'Draft not found' });
    if (draft.status === 'published') {
      return res.status(409).json({ error: 'Draft already published' });
    }
    const publishRows = await sql`
      WITH draft_lock AS (
        SELECT
          id,
          slug,
          title,
          description,
          content,
          section,
          beat,
          persona,
          image,
          image_caption as "imageCaption",
          image_credit as "imageCredit"
        FROM article_drafts
        WHERE id = ${id}
          AND status <> 'published'
        FOR UPDATE
      ),
      slug_seed AS (
        SELECT
          *,
          NULLIF(trim(COALESCE(draft_lock.slug, '')), '') as "existingSlug",
          NULLIF(trim(
            regexp_replace(
              regexp_replace(lower(COALESCE(draft_lock.title, '')), '[^a-z0-9]+', '-', 'g'),
              '(^-+|-+$)', '', 'g'
            )
          ), '') as "titleSlug"
        FROM draft_lock
      ),
      slug_choice AS (
        SELECT
          *,
          COALESCE("existingSlug", "titleSlug", 'article') as "baseSlug"
        FROM slug_seed
      ),
      title_lock AS (
        SELECT pg_advisory_xact_lock(
          hashtext(
            lower(
              regexp_replace(
                trim(COALESCE(slug_choice.title, '')),
                '\s+',
                ' ',
                'g'
              )
            )
          )
        ) AS locked
        FROM slug_choice
      ),
      inserted_primary AS (
        INSERT INTO articles (
          slug,
          title,
          description,
          content,
          section,
          beat,
          persona,
          image,
          image_caption,
          image_credit,
          image_status,
          image_status_changed_at,
          render_class,
          placement_eligible,
          pub_date,
          status
        )
        SELECT
          slug_choice."baseSlug",
          COALESCE(slug_choice.title, ''),
          COALESCE(slug_choice.description, ''),
          COALESCE(slug_choice.content, ''),
          slug_choice.section,
          slug_choice.beat,
          slug_choice.persona,
          CASE
            WHEN ${requestedImageStatus} = 'text_only' THEN ''
            ELSE COALESCE(slug_choice.image, '')
          END,
          CASE
            WHEN ${requestedImageStatus} = 'text_only' THEN ''
            ELSE COALESCE(slug_choice."imageCaption", '')
          END,
          CASE
            WHEN ${requestedImageStatus} = 'text_only' THEN ''
            ELSE COALESCE(slug_choice."imageCredit", '')
          END,
          CASE
            WHEN ${requestedImageStatus} = 'with_image' THEN 'with_image'
            WHEN ${requestedImageStatus} = 'text_only' THEN 'text_only'
            WHEN trim(COALESCE(slug_choice.image, '')) <> '' THEN 'with_image'
            ELSE 'text_only'
          END,
          NOW(),
          CASE
            WHEN ${requestedImageStatus} = 'with_image' THEN 'with_image'
            WHEN ${requestedImageStatus} = 'text_only' THEN 'text_only'
            WHEN trim(COALESCE(slug_choice.image, '')) <> '' THEN 'with_image'
            ELSE 'text_only'
          END,
          CASE
            WHEN ${requestedImageStatus} = 'with_image' THEN '["main","top","carousel","grid","sidebar","extra_headlines"]'::jsonb
            WHEN ${requestedImageStatus} = 'text_only' THEN '["sidebar","extra_headlines"]'::jsonb
            WHEN trim(COALESCE(slug_choice.image, '')) <> '' THEN '["main","top","carousel","grid","sidebar","extra_headlines"]'::jsonb
            ELSE '["sidebar","extra_headlines"]'::jsonb
          END,
          ${publishAtIso},
          'published'
        FROM slug_choice
        CROSS JOIN title_lock
        WHERE NOT EXISTS (
          SELECT 1
          FROM articles a
          WHERE lower(regexp_replace(trim(a.title), '\s+', ' ', 'g'))
            = lower(regexp_replace(trim(COALESCE(slug_choice.title, '')), '\s+', ' ', 'g'))
            AND COALESCE(a.status, 'published') = 'published'
        )
          AND (
            ${requestedImageStatus} <> 'with_image'
            OR trim(COALESCE(slug_choice.image, '')) <> ''
          )
        ON CONFLICT ((lower(trim(slug))))
          WHERE slug IS NOT NULL
            AND trim(slug) <> ''
          DO NOTHING
        RETURNING id, slug
      ),
      inserted_fallback AS (
        INSERT INTO articles (
          slug,
          title,
          description,
          content,
          section,
          beat,
          persona,
          image,
          image_caption,
          image_credit,
          image_status,
          image_status_changed_at,
          render_class,
          placement_eligible,
          pub_date,
          status
        )
        SELECT
          CONCAT(slug_choice."baseSlug", '-', right(md5(clock_timestamp()::text || random()::text), 6)),
          COALESCE(slug_choice.title, ''),
          COALESCE(slug_choice.description, ''),
          COALESCE(slug_choice.content, ''),
          slug_choice.section,
          slug_choice.beat,
          slug_choice.persona,
          CASE
            WHEN ${requestedImageStatus} = 'text_only' THEN ''
            ELSE COALESCE(slug_choice.image, '')
          END,
          CASE
            WHEN ${requestedImageStatus} = 'text_only' THEN ''
            ELSE COALESCE(slug_choice."imageCaption", '')
          END,
          CASE
            WHEN ${requestedImageStatus} = 'text_only' THEN ''
            ELSE COALESCE(slug_choice."imageCredit", '')
          END,
          CASE
            WHEN ${requestedImageStatus} = 'with_image' THEN 'with_image'
            WHEN ${requestedImageStatus} = 'text_only' THEN 'text_only'
            WHEN trim(COALESCE(slug_choice.image, '')) <> '' THEN 'with_image'
            ELSE 'text_only'
          END,
          NOW(),
          CASE
            WHEN ${requestedImageStatus} = 'with_image' THEN 'with_image'
            WHEN ${requestedImageStatus} = 'text_only' THEN 'text_only'
            WHEN trim(COALESCE(slug_choice.image, '')) <> '' THEN 'with_image'
            ELSE 'text_only'
          END,
          CASE
            WHEN ${requestedImageStatus} = 'with_image' THEN '["main","top","carousel","grid","sidebar","extra_headlines"]'::jsonb
            WHEN ${requestedImageStatus} = 'text_only' THEN '["sidebar","extra_headlines"]'::jsonb
            WHEN trim(COALESCE(slug_choice.image, '')) <> '' THEN '["main","top","carousel","grid","sidebar","extra_headlines"]'::jsonb
            ELSE '["sidebar","extra_headlines"]'::jsonb
          END,
          ${publishAtIso},
          'published'
        FROM slug_choice
        CROSS JOIN title_lock
        WHERE NOT EXISTS (SELECT 1 FROM inserted_primary)
          AND NOT EXISTS (
            SELECT 1
            FROM articles a
            WHERE lower(regexp_replace(trim(a.title), '\s+', ' ', 'g'))
              = lower(regexp_replace(trim(COALESCE(slug_choice.title, '')), '\s+', ' ', 'g'))
              AND COALESCE(a.status, 'published') = 'published'
          )
          AND (
            ${requestedImageStatus} <> 'with_image'
            OR trim(COALESCE(slug_choice.image, '')) <> ''
          )
        ON CONFLICT ((lower(trim(slug))))
          WHERE slug IS NOT NULL
            AND trim(slug) <> ''
          DO NOTHING
        RETURNING id, slug
      ),
      inserted AS (
        SELECT id, slug FROM inserted_primary
        UNION ALL
        SELECT id, slug FROM inserted_fallback
      ),
      updated AS (
        UPDATE article_drafts d
        SET
          slug = inserted.slug,
          status = 'published',
          published_article_id = inserted.id,
          updated_at = NOW()
        FROM inserted
        WHERE d.id = ${id}
        RETURNING inserted.id as "articleId", inserted.slug as "articleSlug"
      )
      SELECT "articleId", "articleSlug" FROM updated
      LIMIT 1
    `;

    const articleId = Number(publishRows?.[0]?.articleId || 0);
    const publishedSlug = String(publishRows?.[0]?.articleSlug || '').trim();
    if (!articleId) {
      const lockedDraftRows = await sql`
        SELECT
          title,
          status,
          image
        FROM article_drafts
        WHERE id = ${id}
        LIMIT 1
      `;
      const lockedDraft = lockedDraftRows?.[0] || null;
      if (!lockedDraft) {
        return res.status(404).json({ error: 'Draft not found' });
      }
      if (String(lockedDraft?.status || '').trim() === 'published') {
        return res.status(409).json({ error: 'Draft already published' });
      }
      if (requestedImageStatus === 'with_image' && !String(lockedDraft?.image || '').trim()) {
        return res.status(409).json({ error: 'with_image requires a non-empty draft image URL at publish time' });
      }
      const normalizedCurrentTitle = normalizeComparableTitle(lockedDraft?.title || draft.title || '');
      if (normalizedCurrentTitle) {
        const duplicateAfterAttempt = await sql`
          SELECT id, slug
          FROM articles
          WHERE lower(regexp_replace(trim(title), '\s+', ' ', 'g')) = ${normalizedCurrentTitle}
            AND COALESCE(status, 'published') = 'published'
          LIMIT 1
        `;
        if (duplicateAfterAttempt.length > 0) {
          return res.status(409).json({
            error: 'A published article already uses this headline',
            articleId: duplicateAfterAttempt[0].id,
            slug: duplicateAfterAttempt[0].slug
          });
        }
      }
      throw new Error('Failed to atomically publish draft');
    }

    return res.status(200).json({ ok: true, articleId, slug: publishedSlug || null });
  } catch (error) {
    console.error('Publish draft error:', error);
    return res.status(500).json({ error: 'Failed to publish draft' });
  }
};
