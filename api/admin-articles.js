const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const section = String(req.query.section || 'all').toLowerCase();
    const limit = Math.min(parseInt(req.query.limit || '25', 10), 200);

    const rows = await sql`
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
        pub_date as "pubDate",
        status,
        created_at as "createdAt",
        updated_at as "updatedAt"
      FROM articles
      WHERE (${section} = 'all' OR section = ${section})
      ORDER BY pub_date DESC, id DESC
      LIMIT ${limit}
    `;

    return res.status(200).json({ articles: rows, count: rows.length });
  } catch (error) {
    console.error('Admin articles list error:', error);
    return res.status(500).json({ error: 'Failed to load articles' });
  }
};
