const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

async function ensureEditorialRejectionsTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS editorial_rejections (
      id SERIAL PRIMARY KEY,
      draft_id INTEGER,
      draft_slug TEXT,
      draft_title TEXT NOT NULL,
      section TEXT,
      source_url TEXT,
      source_title TEXT,
      input_tokens INTEGER,
      output_tokens INTEGER,
      total_tokens INTEGER,
      reject_reason TEXT NOT NULL,
      notes TEXT,
      rejected_by TEXT DEFAULT 'admin_ui',
      rejected_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_editorial_rejections_rejected_at
    ON editorial_rejections(rejected_at DESC)
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_editorial_rejections_reason
    ON editorial_rejections(reject_reason)
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureEditorialRejectionsTable(sql);

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
        input_tokens as "inputTokens",
        output_tokens as "outputTokens",
        total_tokens as "totalTokens",
        reject_reason as "rejectReason",
        notes,
        rejected_by as "rejectedBy",
        rejected_at as "rejectedAt"
      FROM editorial_rejections
      ORDER BY rejected_at DESC
      LIMIT ${limit}
    `;

    return res.status(200).json({ ok: true, rejections: rows, count: rows.length });
  } catch (error) {
    console.error('List rejections error:', error);
    return res.status(500).json({ error: 'Failed to load rejected drafts' });
  }
};
