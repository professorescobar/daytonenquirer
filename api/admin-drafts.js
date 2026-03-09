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
    const baseDrafts = await sql`
      SELECT
        id,
        slug,
        title,
        description,
        content,
        section,
        image,
        status,
        created_at as "createdAt",
        updated_at as "updatedAt"
      FROM article_drafts
      WHERE (${status} = 'all' OR status = ${status})
      ORDER BY created_at DESC
      LIMIT ${limit}
    `;

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
    return res.status(500).json({ error: 'Failed to load drafts' });
  }
};
