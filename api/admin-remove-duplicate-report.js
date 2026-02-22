const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

async function ensureDuplicateReportsTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS duplicate_reports (
      id SERIAL PRIMARY KEY,
      draft_id INTEGER,
      draft_slug TEXT,
      draft_title TEXT NOT NULL,
      section TEXT,
      source_url TEXT,
      source_title TEXT,
      report_reason TEXT DEFAULT 'manual_duplicate',
      notes TEXT,
      reported_by TEXT DEFAULT 'admin_ui',
      reported_at TIMESTAMP DEFAULT NOW()
    )
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureDuplicateReportsTable(sql);
    const id = Number(req.body?.id || 0);
    if (!id) {
      return res.status(400).json({ error: 'Missing report id' });
    }

    const removed = await sql`
      DELETE FROM duplicate_reports
      WHERE id = ${id}
      RETURNING id, draft_title as "draftTitle"
    `;

    if (!removed.length) {
      return res.status(404).json({ error: 'Duplicate report not found' });
    }

    return res.status(200).json({ ok: true, report: removed[0] });
  } catch (error) {
    console.error('Remove duplicate report error:', error);
    return res.status(500).json({ error: 'Failed to remove duplicate report' });
  }
};
