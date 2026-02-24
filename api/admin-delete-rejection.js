const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await sql`
      CREATE TABLE IF NOT EXISTS editorial_rejections (
        id SERIAL PRIMARY KEY,
        draft_id INTEGER,
        draft_slug TEXT,
        draft_title TEXT NOT NULL,
        section TEXT,
        source_url TEXT,
        source_title TEXT,
        model TEXT,
        input_tokens INTEGER,
        output_tokens INTEGER,
        total_tokens INTEGER,
        reject_reason TEXT NOT NULL,
        notes TEXT,
        rejected_by TEXT DEFAULT 'admin_ui',
        rejected_at TIMESTAMP DEFAULT NOW()
      )
    `;
    const id = Number(req.body?.id || 0);
    if (!id) return res.status(400).json({ error: 'Missing rejection id' });

    const removed = await sql`
      DELETE FROM editorial_rejections
      WHERE id = ${id}
      RETURNING id, draft_title as "draftTitle", reject_reason as "rejectReason"
    `;
    if (!removed.length) {
      return res.status(404).json({ error: 'Rejection record not found' });
    }

    return res.status(200).json({ ok: true, rejection: removed[0] });
  } catch (error) {
    console.error('Delete rejection error:', error);
    return res.status(500).json({ error: 'Failed to delete rejection record' });
  }
};
