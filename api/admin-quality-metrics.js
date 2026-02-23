const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const REASONS = [
  'duplicate',
  'stale_or_not_time_relevant',
  'low_newsworthiness_or_thin',
  'style_mismatch',
  'user_error'
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
      duplicate_type TEXT DEFAULT 'internal',
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
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS duplicate_type TEXT DEFAULT 'internal'
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

    const duplicateDailyRows = await sql`
      SELECT
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM duplicate_reports
      WHERE reported_at >= date_trunc('day', now())
    `;

    const duplicateMonthlyRows = await sql`
      SELECT
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM duplicate_reports
      WHERE reported_at >= date_trunc('month', now())
    `;

    const rejectionDailyRows = await sql`
      SELECT
        reject_reason as reason,
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM editorial_rejections
      WHERE rejected_at >= date_trunc('day', now())
      GROUP BY reject_reason
    `;

    const rejectionMonthlyRows = await sql`
      SELECT
        reject_reason as reason,
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM editorial_rejections
      WHERE rejected_at >= date_trunc('month', now())
      GROUP BY reject_reason
    `;

    const byReasonDaily = {};
    const tokensByReasonDaily = {};
    const byReasonMonthly = {};
    const tokensByReasonMonthly = {};
    for (const reason of REASONS) {
      byReasonDaily[reason] = 0;
      tokensByReasonDaily[reason] = 0;
      byReasonMonthly[reason] = 0;
      tokensByReasonMonthly[reason] = 0;
    }

    byReasonDaily.duplicate = duplicateDailyRows?.[0]?.count || 0;
    tokensByReasonDaily.duplicate = duplicateDailyRows?.[0]?.tokens || 0;
    byReasonMonthly.duplicate = duplicateMonthlyRows?.[0]?.count || 0;
    tokensByReasonMonthly.duplicate = duplicateMonthlyRows?.[0]?.tokens || 0;

    for (const row of rejectionDailyRows) {
      const key = String(row.reason || '').trim();
      if (!key) continue;
      byReasonDaily[key] = Number(row.count || 0);
      tokensByReasonDaily[key] = Number(row.tokens || 0);
    }
    for (const row of rejectionMonthlyRows) {
      const key = String(row.reason || '').trim();
      if (!key) continue;
      byReasonMonthly[key] = Number(row.count || 0);
      tokensByReasonMonthly[key] = Number(row.tokens || 0);
    }

    const totalRejectedDaily = Object.values(byReasonDaily).reduce((sum, v) => sum + Number(v || 0), 0);
    const badTokensTotalDaily = Object.values(tokensByReasonDaily).reduce((sum, v) => sum + Number(v || 0), 0);

    const totalRejectedMonthly = Object.values(byReasonMonthly).reduce((sum, v) => sum + Number(v || 0), 0);
    const badTokensTotalMonthly = Object.values(tokensByReasonMonthly).reduce((sum, v) => sum + Number(v || 0), 0);

    return res.status(200).json({
      ok: true,
      daily: {
        totalRejected: totalRejectedDaily,
        byReason: byReasonDaily,
        tokensByReason: tokensByReasonDaily,
        badTokensTotal: badTokensTotalDaily
      },
      monthly: {
        totalRejected: totalRejectedMonthly,
        byReason: byReasonMonthly,
        tokensByReason: tokensByReasonMonthly,
        badTokensTotal: badTokensTotalMonthly
      },
      // Backward-compatible aliases
      totalRejected: totalRejectedDaily,
      byReason: byReasonDaily,
      tokensByReason: tokensByReasonDaily,
      badTokensTotal: badTokensTotalDaily
    });
  } catch (error) {
    console.error('Quality metrics error:', error);
    return res.status(500).json({ error: 'Failed to load quality metrics' });
  }
};
