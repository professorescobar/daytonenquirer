const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { getDailyTokenBudgets, getModelDailyTokenBudget } = require('./_admin-settings');

const ET_TIME_ZONE = 'America/New_York';
const DEFAULT_DAILY_DRAFT_TARGET = 81;

async function ensureQualityTables(sql) {
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
    ADD COLUMN IF NOT EXISTS model TEXT
  `;
  await sql`
    ALTER TABLE editorial_rejections
    ADD COLUMN IF NOT EXISTS model TEXT
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureQualityTables(sql);
    const scope = String(req.query.scope || 'auto').toLowerCase();
    const normalizedScope = ['auto', 'manual', 'all'].includes(scope) ? scope : 'auto';
    const requestedModel = String(req.query.model || '').trim();
    const budgets = await getDailyTokenBudgets(sql);
    const baseDailyTokenBudget = normalizedScope === 'manual'
      ? budgets.manual
      : (normalizedScope === 'auto' ? budgets.auto : (budgets.auto + budgets.manual));
    let dailyTokenBudget = baseDailyTokenBudget;
    if (requestedModel) {
      if (normalizedScope === 'all') {
        const [autoModelBudget, manualModelBudget] = await Promise.all([
          getModelDailyTokenBudget(sql, requestedModel, Math.floor(budgets.auto / 3), 'auto'),
          getModelDailyTokenBudget(sql, requestedModel, Math.floor(budgets.manual / 3), 'manual')
        ]);
        dailyTokenBudget = autoModelBudget + manualModelBudget;
      } else {
        const scopeFallback = normalizedScope === 'manual'
          ? Math.floor(budgets.manual / 3)
          : Math.floor(budgets.auto / 3);
        dailyTokenBudget = await getModelDailyTokenBudget(sql, requestedModel, scopeFallback, normalizedScope);
      }
    }
    const dailyDraftTarget = Math.max(
      1,
      parseInt(String(req.query.dailyDraftTarget || process.env.DAILY_DRAFT_TARGET || DEFAULT_DAILY_DRAFT_TARGET), 10) || DEFAULT_DAILY_DRAFT_TARGET
    );

    const dailyRows = await sql`
      SELECT COALESCE(SUM(total_tokens), 0)::int AS "tokensUsedToday"
      , COUNT(*)::int AS "draftsToday"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
        AND (${normalizedScope} = 'all' OR created_via = ${normalizedScope})
        AND (${requestedModel} = '' OR COALESCE(NULLIF(TRIM(model), ''), 'unknown') = ${requestedModel})
    `;

    const weeklyRows = await sql`
      SELECT
        COALESCE(SUM(total_tokens), 0)::int AS "tokensUsedWeek",
        COUNT(*)::int AS "draftsWeek"
      FROM article_drafts
      WHERE date_trunc('week', (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE}))
            = date_trunc('week', (NOW() AT TIME ZONE ${ET_TIME_ZONE}))
        AND (${normalizedScope} = 'all' OR created_via = ${normalizedScope})
        AND (${requestedModel} = '' OR COALESCE(NULLIF(TRIM(model), ''), 'unknown') = ${requestedModel})
    `;

    const monthlyRows = await sql`
      SELECT
        COALESCE(SUM(total_tokens), 0)::int AS "tokensUsedMonth",
        COUNT(*)::int AS "draftsMonth"
      FROM article_drafts
      WHERE date_trunc('month', (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE}))
            = date_trunc('month', (NOW() AT TIME ZONE ${ET_TIME_ZONE}))
        AND (${normalizedScope} = 'all' OR created_via = ${normalizedScope})
        AND (${requestedModel} = '' OR COALESCE(NULLIF(TRIM(model), ''), 'unknown') = ${requestedModel})
    `;

    const duplicateRows = await sql`
      SELECT COUNT(*)::int AS "count"
      FROM duplicate_reports
      WHERE (reported_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
        AND (${requestedModel} = '' OR COALESCE(NULLIF(TRIM(model), ''), 'unknown') = ${requestedModel})
    `;
    const rejectionRows = await sql`
      SELECT COUNT(*)::int AS "count"
      FROM editorial_rejections
      WHERE (rejected_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
        AND (${requestedModel} = '' OR COALESCE(NULLIF(TRIM(model), ''), 'unknown') = ${requestedModel})
    `;
    const acceptedRows = await sql`
      SELECT COUNT(*)::int AS "count"
      FROM article_drafts
      WHERE (updated_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
        AND status = 'published'
        AND (${requestedModel} = '' OR COALESCE(NULLIF(TRIM(model), ''), 'unknown') = ${requestedModel})
    `;

    const tokensUsedToday = dailyRows?.[0]?.tokensUsedToday || 0;
    const draftsToday = dailyRows?.[0]?.draftsToday || 0;
    const tokensUsedWeek = weeklyRows?.[0]?.tokensUsedWeek || 0;
    const draftsWeek = weeklyRows?.[0]?.draftsWeek || 0;
    const tokensUsedMonth = monthlyRows?.[0]?.tokensUsedMonth || 0;
    const draftsMonth = monthlyRows?.[0]?.draftsMonth || 0;
    const budgetUsedPercent = dailyTokenBudget > 0
      ? Math.min((tokensUsedToday / dailyTokenBudget) * 100, 100)
      : 0;
    const weeklyBudgetUsedPercent = dailyTokenBudget > 0
      ? Math.min((tokensUsedWeek / (dailyTokenBudget * 7)) * 100, 100)
      : 0;
    const monthlyBudgetUsedPercent = dailyTokenBudget > 0
      ? Math.min((tokensUsedMonth / (dailyTokenBudget * 30)) * 100, 100)
      : 0;
    const budgetRemainingPercent = Math.max(0, 100 - budgetUsedPercent);
    const turnedDownToday = Number(duplicateRows?.[0]?.count || 0) + Number(rejectionRows?.[0]?.count || 0);
    const acceptedToday = Number(acceptedRows?.[0]?.count || 0);
    const reviewedToday = acceptedToday + turnedDownToday;
    const qualityLossRatePercent = draftsToday > 0
      ? Math.min((turnedDownToday / draftsToday) * 100, 100)
      : 0;
    const acceptanceRatePercent = reviewedToday > 0
      ? Math.min((acceptedToday / reviewedToday) * 100, 100)
      : 0;
    const throughputPercent = dailyDraftTarget > 0
      ? Math.min((draftsToday / dailyDraftTarget) * 100, 100)
      : 0;

    return res.status(200).json({
      ok: true,
      scope: normalizedScope,
      model: requestedModel || null,
      timezone: ET_TIME_ZONE,
      tokensUsedToday,
      dailyTokenBudget,
      dailyTokenBudgetAuto: budgets.auto,
      dailyTokenBudgetManual: budgets.manual,
      budgetUsedPercent: Number(budgetUsedPercent.toFixed(1)),
      weeklyBudgetUsedPercent: Number(weeklyBudgetUsedPercent.toFixed(1)),
      monthlyBudgetUsedPercent: Number(monthlyBudgetUsedPercent.toFixed(1)),
      budgetRemainingPercent: Number(budgetRemainingPercent.toFixed(1)),
      // Backward-compatible alias used by existing UI.
      budgetPercent: Number(budgetUsedPercent.toFixed(1)),
      draftsToday,
      tokensRemainingToday: Math.max(0, dailyTokenBudget - tokensUsedToday),
      dailyTokensUsed: tokensUsedToday,
      dailyDrafts: draftsToday,
      weeklyTokensUsed: tokensUsedWeek,
      weeklyDrafts: draftsWeek,
      monthlyTokensUsed: tokensUsedMonth,
      monthlyDrafts: draftsMonth,
      dailyDraftTarget,
      throughputPercent: Number(throughputPercent.toFixed(1)),
      turnedDownToday,
      acceptedToday,
      reviewedToday,
      qualityLossRatePercent: Number(qualityLossRatePercent.toFixed(1)),
      acceptanceRatePercent: Number(acceptanceRatePercent.toFixed(1))
    });
  } catch (error) {
    console.error('Admin usage error:', error);
    return res.status(500).json({ error: 'Failed to load usage metrics' });
  }
};
