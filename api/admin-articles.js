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
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 5000);

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

    const totalRows = await sql`
      SELECT COUNT(*)::int AS "totalCount"
      FROM articles
      WHERE (${section} = 'all' OR section = ${section})
    `;
    const totalCount = totalRows?.[0]?.totalCount || 0;

    const totalAllRows = await sql`
      SELECT COUNT(*)::int AS "totalAllCount"
      FROM articles
    `;
    const totalAllCount = totalAllRows?.[0]?.totalAllCount || 0;

    return res.status(200).json({
      articles: rows,
      count: rows.length,
      totalCount,
      totalAllCount
    });
  } catch (error) {
    console.error('Admin articles list error:', error);
    return res.status(500).json({ error: 'Failed to load articles' });
  }
};
