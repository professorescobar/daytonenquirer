const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const status = (req.query.status || 'pending_review').toString();
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);

  try {
    const sql = neon(process.env.DATABASE_URL);
    const drafts = await sql`
      SELECT
        id,
        slug,
        title,
        description,
        content,
        section,
        image,
        image_caption as "imageCaption",
        image_credit as "imageCredit",
        source_url as "sourceUrl",
        source_title as "sourceTitle",
        source_published_at as "sourcePublishedAt",
        pub_date as "pubDate",
        model,
        status,
        published_article_id as "publishedArticleId",
        created_at as "createdAt",
        updated_at as "updatedAt"
      FROM article_drafts
      WHERE (${status} = 'all' OR status = ${status})
      ORDER BY created_at DESC
      LIMIT ${limit}
    `;

    return res.status(200).json({ drafts, count: drafts.length });
  } catch (error) {
    console.error('List drafts error:', error);
    return res.status(500).json({ error: 'Failed to load drafts' });
  }
};
