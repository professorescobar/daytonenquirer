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
      return res.status(400).json({ error: 'Missing draft id' });
    }

    const deleted = await sql`
      DELETE FROM article_drafts
      WHERE id = ${id}
      RETURNING id, title, status
    `;

    if (!deleted.length) {
      return res.status(404).json({ error: 'Draft not found' });
    }

    return res.status(200).json({ ok: true, draft: deleted[0] });
  } catch (error) {
    console.error('Delete draft error:', error);
    return res.status(500).json({ error: 'Failed to delete draft' });
  }
};
