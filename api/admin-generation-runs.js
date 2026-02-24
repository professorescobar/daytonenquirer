const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

async function ensureDraftGenerationRunsTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS draft_generation_runs (
      id SERIAL PRIMARY KEY,
      run_at TIMESTAMP DEFAULT NOW(),
      run_status TEXT NOT NULL,
      run_reason TEXT,
      schedule_mode TEXT,
      track TEXT,
      run_mode TEXT,
      created_via TEXT,
      dry_run BOOLEAN DEFAULT false,
      include_sections TEXT,
      exclude_sections TEXT,
      active_sections TEXT,
      et_date TEXT,
      et_time TEXT,
      requested_count INTEGER,
      target_count INTEGER,
      created_count INTEGER DEFAULT 0,
      skipped_count INTEGER DEFAULT 0,
      daily_token_budget INTEGER,
      tokens_used_today INTEGER DEFAULT 0,
      run_tokens_consumed INTEGER DEFAULT 0,
      top_skip_reasons JSONB DEFAULT '[]'::jsonb
    )
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_draft_generation_runs_run_at
    ON draft_generation_runs(run_at DESC)
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureDraftGenerationRunsTable(sql);

    const limit = Math.min(Math.max(parseInt(req.query.limit || '50', 10), 1), 500);
    const rows = await sql`
      SELECT
        id,
        run_at as "runAt",
        run_status as "runStatus",
        run_reason as "runReason",
        schedule_mode as "scheduleMode",
        track,
        run_mode as "runMode",
        created_via as "createdVia",
        dry_run as "dryRun",
        include_sections as "includeSections",
        exclude_sections as "excludeSections",
        active_sections as "activeSections",
        et_date as "etDate",
        et_time as "etTime",
        requested_count as "requestedCount",
        target_count as "targetCount",
        created_count as "createdCount",
        skipped_count as "skippedCount",
        daily_token_budget as "dailyTokenBudget",
        tokens_used_today as "tokensUsedToday",
        run_tokens_consumed as "runTokensConsumed",
        top_skip_reasons as "topSkipReasons"
      FROM draft_generation_runs
      ORDER BY run_at DESC
      LIMIT ${limit}
    `;

    return res.status(200).json({ ok: true, runs: rows, count: rows.length });
  } catch (error) {
    console.error('List generation runs error:', error);
    return res.status(500).json({ error: 'Failed to load generation runs' });
  }
};
