const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { getDailyTokenBudgets, getModelDailyTokenBudget } = require('./_admin-settings');

const ET_TIME_ZONE = 'America/New_York';
const DEFAULT_DAILY_DRAFT_TARGET = 81;

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const scope = String(req.query.scope || 'manual').toLowerCase();
    const normalizedScope = ['manual', 'all'].includes(scope) ? scope : 'manual';
    const requestedModel = String(req.query.model || '').trim();
    const budgets = await getDailyTokenBudgets(sql);
    const baseDailyTokenBudget = budgets.manual;
    let dailyTokenBudget = baseDailyTokenBudget;
    if (requestedModel) {
      const scopeFallback = Math.floor(budgets.manual / 3);
      dailyTokenBudget = await getModelDailyTokenBudget(sql, requestedModel, scopeFallback, 'manual');
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

    const acceptedRows = await sql`
      SELECT COUNT(*)::int AS "count"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date
            = (NOW() AT TIME ZONE ${ET_TIME_ZONE})::date
        AND status = 'published'
        AND (${normalizedScope} = 'all' OR created_via = ${normalizedScope})
        AND (${requestedModel} = '' OR COALESCE(NULLIF(TRIM(model), ''), 'unknown') = ${requestedModel})
    `;

    const tokensUsedToday = dailyRows?.[0]?.tokensUsedToday || 0;
    const draftsTodayLive = dailyRows?.[0]?.draftsToday || 0;
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
    const turnedDownToday = 0;
    const draftsTodayGenerated = draftsTodayLive;
    const acceptedToday = Number(acceptedRows?.[0]?.count || 0);
    const reviewedToday = acceptedToday + turnedDownToday;
    const qualityLossRatePercent = draftsTodayGenerated > 0
      ? Math.min((turnedDownToday / draftsTodayGenerated) * 100, 100)
      : 0;
    const acceptanceRatePercent = reviewedToday > 0
      ? Math.min((acceptedToday / reviewedToday) * 100, 100)
      : 0;
    const throughputPercent = dailyDraftTarget > 0
      ? Math.min((draftsTodayGenerated / dailyDraftTarget) * 100, 100)
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
      draftsToday: draftsTodayGenerated,
      draftsTodayLive,
      draftsTodayGenerated,
      tokensRemainingToday: Math.max(0, dailyTokenBudget - tokensUsedToday),
      dailyTokensUsed: tokensUsedToday,
      dailyDrafts: draftsTodayGenerated,
      dailyDraftsLive: draftsTodayLive,
      dailyDraftsGenerated: draftsTodayGenerated,
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
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Failed to load usage metrics' });
  }
};
