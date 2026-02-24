const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const ET_TIME_ZONE = 'America/New_York';

const REASONS = [
  'duplicate',
  'stale_or_not_time_relevant',
  'low_newsworthiness_or_thin',
  'style_mismatch',
  'user_error'
];

function normalizeModel(value) {
  const model = String(value || '').trim();
  return model || 'unknown';
}

function emptyReasonCounts() {
  const out = {};
  for (const reason of REASONS) out[reason] = 0;
  return out;
}

function buildRejectionMaps(rows) {
  const byReason = emptyReasonCounts();
  const tokensByReason = emptyReasonCounts();
  for (const row of rows) {
    const reason = String(row.reason || '').trim();
    if (!reason || !Object.prototype.hasOwnProperty.call(byReason, reason)) continue;
    byReason[reason] = Number(row.count || 0);
    tokensByReason[reason] = Number(row.tokens || 0);
  }
  return { byReason, tokensByReason };
}

function combineDuplicateAndRejections(duplicateRows, rejectionRows) {
  const { byReason, tokensByReason } = buildRejectionMaps(rejectionRows);
  byReason.duplicate = Number(duplicateRows?.[0]?.count || 0);
  tokensByReason.duplicate = Number(duplicateRows?.[0]?.tokens || 0);

  const totalRejected = Object.values(byReason).reduce((sum, v) => sum + Number(v || 0), 0);
  const badTokensTotal = Object.values(tokensByReason).reduce((sum, v) => sum + Number(v || 0), 0);
  return { totalRejected, byReason, tokensByReason, badTokensTotal };
}

function ensureModelBucket(map, model) {
  if (map.has(model)) return map.get(model);
  const bucket = {
    model,
    draftsGiven: 0,
    turnedDown: 0,
    byReason: emptyReasonCounts()
  };
  map.set(model, bucket);
  return bucket;
}

function sortModelBreakdown(items) {
  return items.sort((a, b) => {
    const aScore = Number(a.turnedDown || 0) + Number(a.draftsGiven || 0);
    const bScore = Number(b.turnedDown || 0) + Number(b.draftsGiven || 0);
    if (bScore !== aScore) return bScore - aScore;
    return String(a.model || '').localeCompare(String(b.model || ''));
  });
}

function buildModelBreakdown(draftRows, duplicateRows, rejectionRows) {
  const byModel = new Map();

  for (const row of draftRows) {
    const model = normalizeModel(row.model);
    const bucket = ensureModelBucket(byModel, model);
    bucket.draftsGiven += Number(row.count || 0);
  }

  for (const row of duplicateRows) {
    const model = normalizeModel(row.model);
    const bucket = ensureModelBucket(byModel, model);
    const count = Number(row.count || 0);
    bucket.draftsGiven += count;
    bucket.turnedDown += count;
    bucket.byReason.duplicate += count;
  }

  for (const row of rejectionRows) {
    const model = normalizeModel(row.model);
    const reason = String(row.reason || '').trim();
    if (!reason || !REASONS.includes(reason)) continue;
    const bucket = ensureModelBucket(byModel, model);
    const count = Number(row.count || 0);
    bucket.draftsGiven += count;
    bucket.turnedDown += count;
    bucket.byReason[reason] += count;
  }

  return sortModelBreakdown(Array.from(byModel.values()));
}

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
      model TEXT,
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

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS total_tokens INTEGER
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS duplicate_type TEXT DEFAULT 'internal'
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS model TEXT
  `;

  await sql`
    ALTER TABLE editorial_rejections
    ADD COLUMN IF NOT EXISTS total_tokens INTEGER
  `;

  await sql`
    ALTER TABLE editorial_rejections
    ADD COLUMN IF NOT EXISTS model TEXT
  `;
}

async function ensureModelTrackingReset(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS admin_runtime_flags (
      key TEXT PRIMARY KEY,
      value TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `;

  const rows = await sql`
    SELECT key
    FROM admin_runtime_flags
    WHERE key = 'model_memory_reset_v1'
    LIMIT 1
  `;
  if (rows.length) return;

  await sql`DELETE FROM duplicate_reports`;
  await sql`DELETE FROM editorial_rejections`;
  await sql`
    INSERT INTO admin_runtime_flags (key, value, created_at, updated_at)
    VALUES ('model_memory_reset_v1', 'done', NOW(), NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        updated_at = NOW()
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
    await ensureModelTrackingReset(sql);

    const duplicateDailyRows = await sql`
      SELECT
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM duplicate_reports
      WHERE (reported_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
    `;
    const duplicateMonthlyRows = await sql`
      SELECT
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM duplicate_reports
      WHERE date_trunc('month', (reported_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE}))
            = date_trunc('month', (NOW() AT TIME ZONE ${ET_TIME_ZONE}))
    `;
    const duplicateAnnualRows = await sql`
      SELECT
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM duplicate_reports
      WHERE date_trunc('year', (reported_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE}))
            = date_trunc('year', (NOW() AT TIME ZONE ${ET_TIME_ZONE}))
    `;
    const duplicateTotalRows = await sql`
      SELECT
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM duplicate_reports
    `;

    const rejectionDailyRows = await sql`
      SELECT
        reject_reason as reason,
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM editorial_rejections
      WHERE (rejected_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
      GROUP BY reject_reason
    `;
    const rejectionMonthlyRows = await sql`
      SELECT
        reject_reason as reason,
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM editorial_rejections
      WHERE date_trunc('month', (rejected_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE}))
            = date_trunc('month', (NOW() AT TIME ZONE ${ET_TIME_ZONE}))
      GROUP BY reject_reason
    `;
    const rejectionAnnualRows = await sql`
      SELECT
        reject_reason as reason,
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM editorial_rejections
      WHERE date_trunc('year', (rejected_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE}))
            = date_trunc('year', (NOW() AT TIME ZONE ${ET_TIME_ZONE}))
      GROUP BY reject_reason
    `;
    const rejectionTotalRows = await sql`
      SELECT
        reject_reason as reason,
        COUNT(*)::int AS "count",
        COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM editorial_rejections
      GROUP BY reject_reason
    `;

    const daily = combineDuplicateAndRejections(duplicateDailyRows, rejectionDailyRows);
    const monthly = combineDuplicateAndRejections(duplicateMonthlyRows, rejectionMonthlyRows);
    const annual = combineDuplicateAndRejections(duplicateAnnualRows, rejectionAnnualRows);
    const total = combineDuplicateAndRejections(duplicateTotalRows, rejectionTotalRows);

    const draftModelDailyRows = await sql`
      SELECT COALESCE(NULLIF(TRIM(model), ''), 'unknown') as model, COUNT(*)::int as "count"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
      GROUP BY 1
    `;
    const duplicateModelDailyRows = await sql`
      SELECT COALESCE(NULLIF(TRIM(model), ''), 'unknown') as model, COUNT(*)::int as "count"
      FROM duplicate_reports
      WHERE (reported_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
      GROUP BY 1
    `;
    const rejectionModelDailyRows = await sql`
      SELECT
        COALESCE(NULLIF(TRIM(model), ''), 'unknown') as model,
        reject_reason as reason,
        COUNT(*)::int as "count"
      FROM editorial_rejections
      WHERE (rejected_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
      GROUP BY 1, 2
    `;

    const draftModelTotalRows = await sql`
      SELECT COALESCE(NULLIF(TRIM(model), ''), 'unknown') as model, COUNT(*)::int as "count"
      FROM article_drafts
      GROUP BY 1
    `;
    const duplicateModelTotalRows = await sql`
      SELECT COALESCE(NULLIF(TRIM(model), ''), 'unknown') as model, COUNT(*)::int as "count"
      FROM duplicate_reports
      GROUP BY 1
    `;
    const rejectionModelTotalRows = await sql`
      SELECT
        COALESCE(NULLIF(TRIM(model), ''), 'unknown') as model,
        reject_reason as reason,
        COUNT(*)::int as "count"
      FROM editorial_rejections
      GROUP BY 1, 2
    `;

    const modelBreakdown = {
      daily: buildModelBreakdown(draftModelDailyRows, duplicateModelDailyRows, rejectionModelDailyRows),
      total: buildModelBreakdown(draftModelTotalRows, duplicateModelTotalRows, rejectionModelTotalRows)
    };

    return res.status(200).json({
      ok: true,
      timezone: ET_TIME_ZONE,
      daily,
      monthly,
      annual,
      total,
      modelBreakdown,
      totalRejected: daily.totalRejected,
      byReason: daily.byReason,
      tokensByReason: daily.tokensByReason,
      badTokensTotal: daily.badTokensTotal
    });
  } catch (error) {
    console.error('Quality metrics error:', error);
    return res.status(500).json({ error: 'Failed to load quality metrics' });
  }
};
