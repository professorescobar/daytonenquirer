const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

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
    let hasStatus = true;
    let hasCreatedAt = true;
    let hasUpdatedAt = true;

    async function queryDrafts() {
      return sql`
        SELECT
          id,
          slug,
          title,
          description,
          content,
          section,
          image,
          ${hasStatus ? sql`status` : sql`NULL::text`} as "status",
          ${hasCreatedAt ? sql`created_at` : sql`NOW()`} as "createdAt",
          ${hasUpdatedAt ? sql`updated_at` : sql`NOW()`} as "updatedAt"
        FROM article_drafts
        WHERE (${status} = 'all' OR ${hasStatus ? sql`status = ${status}` : sql`TRUE`})
        ORDER BY ${hasCreatedAt ? sql`created_at` : sql`id`} DESC
        LIMIT ${limit}
      `;
    }

    let baseDrafts;
    try {
      baseDrafts = await queryDrafts();
    } catch (error) {
      const message = String(error?.message || '').toLowerCase();
      let changed = false;
      if (hasStatus && message.includes('column') && message.includes('status') && message.includes('does not exist')) {
        hasStatus = false;
        changed = true;
      }
      if (hasCreatedAt && message.includes('column') && message.includes('created_at') && message.includes('does not exist')) {
        hasCreatedAt = false;
        changed = true;
      }
      if (hasUpdatedAt && message.includes('column') && message.includes('updated_at') && message.includes('does not exist')) {
        hasUpdatedAt = false;
        changed = true;
      }
      if (!changed) throw error;
      baseDrafts = await queryDrafts();
    }

    const drafts = baseDrafts.map((row) => ({
      ...row,
      beat: null,
      persona: null,
      imageCaption: null,
      imageCredit: null,
      sourceUrl: null,
      sourceTitle: null,
      sourcePublishedAt: null,
      pubDate: null,
      model: null,
      createdVia: null,
      publishedArticleId: null
    }));

    return res.status(200).json({ drafts, count: drafts.length });
  } catch (error) {
    console.error('List drafts error:', error);
    return res.status(500).json({
      error: 'Failed to load drafts',
      details: String(error?.message || '')
    });
  }
};
