const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const id = Number(req.body?.id || 0);

    if (!id) {
      return res.status(400).json({ error: 'Missing article id' });
    }

    const articleRows = await sql`
      SELECT id, slug, title, section, pub_date as "pubDate"
      FROM articles
      WHERE id = ${id}
      LIMIT 1
    `;
    const article = articleRows[0];
    if (!article) {
      return res.status(404).json({ error: 'Article not found' });
    }

    const deletedRows = await sql.transaction([
      sql`
        UPDATE article_drafts
        SET
          status = 'pending_review',
          published_article_id = NULL,
          updated_at = NOW()
        WHERE published_article_id = ${id}
      `,
      sql`
        DELETE FROM articles
        WHERE id = ${id}
        RETURNING id
      `
    ]);
    const deleted = Array.isArray(deletedRows?.[1]) ? deletedRows[1][0] : null;
    if (!deleted) {
      return res.status(404).json({ error: 'Article not found' });
    }

    return res.status(200).json({
      ok: true,
      deletedArticleId: id,
      title: article.title
    });
  } catch (error) {
    console.error('Remove published article error:', error);
    return res.status(500).json({ error: 'Failed to remove published article', details: error.message });
  }
};
