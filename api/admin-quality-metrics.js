const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const REASONS = [
  'duplicate',
  'stale_or_not_time_relevant',
  'low_newsworthiness_or_thin',
  'style_mismatch'
];

async function ensureTables(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS duplicate_reports (
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
      report_reason TEXT DEFAULT 'manual_duplicate',
      notes TEXT,
      reported_by TEXT DEFAULT 'admin_ui',
      reported_at TIMESTAMP DEFAULT NOW()
    )
  `;

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
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS total_tokens INTEGER
  `;

  await sql`
    ALTER TABLE editorial_rejections
    ADD COLUMN IF NOT EXISTS total_tokens INTEGER
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureTables(sql);

    const duplicateRows = await sql`
      SELECT
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM duplicate_reports
    `;

    const rejectionRows = await sql`
      SELECT
        reject_reason as reason,
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM editorial_rejections
      GROUP BY reject_reason
    `;

    const byReason = {};
    const tokensByReason = {};
    for (const reason of REASONS) {
      byReason[reason] = 0;
      tokensByReason[reason] = 0;
    }

    byReason.duplicate = duplicateRows?.[0]?.count || 0;
    tokensByReason.duplicate = duplicateRows?.[0]?.tokens || 0;

    for (const row of rejectionRows) {
      const key = String(row.reason || '').trim();
      if (!key) continue;
      byReason[key] = Number(row.count || 0);
      tokensByReason[key] = Number(row.tokens || 0);
    }

    const totalRejected = Object.values(byReason).reduce((sum, v) => sum + Number(v || 0), 0);
    const badTokensTotal = Object.values(tokensByReason).reduce((sum, v) => sum + Number(v || 0), 0);

    return res.status(200).json({
      ok: true,
      totalRejected,
      byReason,
      tokensByReason,
      badTokensTotal
    });
  } catch (error) {
    console.error('Quality metrics error:', error);
    return res.status(500).json({ error: 'Failed to load quality metrics' });
  }
};
