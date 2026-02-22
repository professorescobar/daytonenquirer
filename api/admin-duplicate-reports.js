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

  await sql`
    CREATE INDEX IF NOT EXISTS idx_duplicate_reports_reported_at
    ON duplicate_reports(reported_at DESC)
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureDuplicateReportsTable(sql);
    const limit = Math.min(Math.max(parseInt(req.query.limit || '50', 10), 1), 500);

    const rows = await sql`
      SELECT
        id,
        draft_id as "draftId",
        draft_slug as "draftSlug",
        draft_title as "draftTitle",
        section,
        source_url as "sourceUrl",
        source_title as "sourceTitle",
        report_reason as "reportReason",
        notes,
        reported_by as "reportedBy",
        reported_at as "reportedAt"
      FROM duplicate_reports
      ORDER BY reported_at DESC
      LIMIT ${limit}
    `;

    return res.status(200).json({ ok: true, reports: rows, count: rows.length });
  } catch (error) {
    console.error('List duplicate reports error:', error);
    return res.status(500).json({ error: 'Failed to load duplicate reports' });
  }
};
