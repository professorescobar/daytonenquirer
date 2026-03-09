const { neon } = require('@neondatabase/serverless');

function cleanText(value, max = 300) {
  return String(value || '').trim().slice(0, max);
}

function parseLimit(value, fallback = 3) {
  const parsed = Number.parseInt(String(value || fallback), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, 1), 6);
}

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.setHeader('Allow', ['GET']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const slug = cleanText(req.query?.slug || '', 180);
  const userQuery = cleanText(req.query?.q || '', 1000);
  const limitCount = parseLimit(req.query?.limit, 3);
  if (!slug && !userQuery) {
    return res.status(400).json({ error: 'slug or q is required' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    if (slug) {
      const targetRows = await sql`
        SELECT slug
        FROM articles
        WHERE lower(trim(slug)) = lower(trim(${slug}))
          AND COALESCE(status, 'published') = 'published'
        LIMIT 1
      `;
      if (!targetRows[0]) {
        return res.status(404).json({ error: 'Article not found' });
      }
    }

    const rows = await sql`
      SELECT
        id,
        slug,
        title,
        description,
        section,
        pub_date as "pubDate",
        score
      FROM find_related_articles(${slug || null}, ${userQuery || null}, ${limitCount})
    `;

    return res.status(200).json({ slug: slug || null, q: userQuery || null, related: rows });
  } catch (error) {
    console.error('Article related search error:', error);
    return res.status(500).json({ error: 'Failed to find related articles' });
  }
};
