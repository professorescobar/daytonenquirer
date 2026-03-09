const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

function isMissingColumn(error, columnName) {
  return new RegExp(`column\\s+"?${columnName}"?\\s+does\\s+not\\s+exist`, 'i').test(String(error?.message || ''));
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const status = (req.query.status || 'pending_review').toString();
  const parsedLimit = Number.parseInt(String(req.query.limit || '50'), 10);
  const limit = Math.max(1, Math.min(Number.isFinite(parsedLimit) ? parsedLimit : 50, 200));

  try {
    const sql = neon(process.env.DATABASE_URL);
    let hasBeat = true;
    let hasPersona = true;
    let hasImageCaption = true;
    let hasImageCredit = true;
    let hasSourceUrl = true;
    let hasSourceTitle = true;
    let hasSourcePublishedAt = true;
    let hasPubDate = true;
    let hasModel = true;
    let hasPublishedArticleId = true;
    let hasCreatedVia = true;

    async function queryDrafts() {
      return sql`
        SELECT
          id,
          slug,
          title,
          description,
          content,
          section,
          ${hasBeat ? sql`beat` : sql`NULL::text`} as "beat",
          ${hasPersona ? sql`persona` : sql`NULL::text`} as "persona",
          image,
          ${hasImageCaption ? sql`image_caption` : sql`NULL::text`} as "imageCaption",
          ${hasImageCredit ? sql`image_credit` : sql`NULL::text`} as "imageCredit",
          ${hasSourceUrl ? sql`source_url` : sql`NULL::text`} as "sourceUrl",
          ${hasSourceTitle ? sql`source_title` : sql`NULL::text`} as "sourceTitle",
          ${hasSourcePublishedAt ? sql`source_published_at` : sql`NULL::timestamptz`} as "sourcePublishedAt",
          ${hasPubDate ? sql`pub_date` : sql`NULL::timestamptz`} as "pubDate",
          ${hasModel ? sql`model` : sql`NULL::text`} as "model",
          ${hasCreatedVia ? sql`created_via` : sql`NULL::text`} as "createdVia",
          status,
          ${hasPublishedArticleId ? sql`published_article_id` : sql`NULL::bigint`} as "publishedArticleId",
          created_at as "createdAt",
          updated_at as "updatedAt"
        FROM article_drafts
        WHERE (${status} = 'all' OR status = ${status})
        ORDER BY created_at DESC
        LIMIT ${limit}
      `;
    }

    let drafts;
    try {
      drafts = await queryDrafts();
    } catch (error) {
      let changed = false;
      if (hasBeat && isMissingColumn(error, 'beat')) {
        hasBeat = false;
        changed = true;
      }
      if (hasPersona && isMissingColumn(error, 'persona')) {
        hasPersona = false;
        changed = true;
      }
      if (hasImageCaption && isMissingColumn(error, 'image_caption')) {
        hasImageCaption = false;
        changed = true;
      }
      if (hasImageCredit && isMissingColumn(error, 'image_credit')) {
        hasImageCredit = false;
        changed = true;
      }
      if (hasSourceUrl && isMissingColumn(error, 'source_url')) {
        hasSourceUrl = false;
        changed = true;
      }
      if (hasSourceTitle && isMissingColumn(error, 'source_title')) {
        hasSourceTitle = false;
        changed = true;
      }
      if (hasSourcePublishedAt && isMissingColumn(error, 'source_published_at')) {
        hasSourcePublishedAt = false;
        changed = true;
      }
      if (hasPubDate && isMissingColumn(error, 'pub_date')) {
        hasPubDate = false;
        changed = true;
      }
      if (hasModel && isMissingColumn(error, 'model')) {
        hasModel = false;
        changed = true;
      }
      if (hasPublishedArticleId && isMissingColumn(error, 'published_article_id')) {
        hasPublishedArticleId = false;
        changed = true;
      }
      if (hasCreatedVia && isMissingColumn(error, 'created_via')) {
        hasCreatedVia = false;
        changed = true;
      }
      if (!changed) throw error;
      drafts = await queryDrafts();
    }

    return res.status(200).json({ drafts, count: drafts.length });
  } catch (error) {
    console.error('List drafts error:', error);
    return res.status(500).json({ error: 'Failed to load drafts' });
  }
};
