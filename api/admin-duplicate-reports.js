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
    CREATE INDEX IF NOT EXISTS idx_duplicate_reports_reported_at
    ON duplicate_reports(reported_at DESC)
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS input_tokens INTEGER
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS output_tokens INTEGER
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
}

async function ensureModelTrackingReset(sql) {
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
  await sql`DELETE FROM admin_runtime_flags WHERE key = 'model_memory_reset_v1'`;
  await sql`
    INSERT INTO admin_runtime_flags (key, value, created_at, updated_at)
    VALUES ('model_memory_reset_v1', 'done', NOW(), NOW())
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
    await ensureModelTrackingReset(sql);
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
        COALESCE(NULLIF(TRIM(model), ''), 'unknown') as "model",
        COALESCE(NULLIF(TRIM(duplicate_type), ''), 'internal') as "duplicateType",
        input_tokens as "inputTokens",
        output_tokens as "outputTokens",
        total_tokens as "totalTokens",
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
