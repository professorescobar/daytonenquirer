const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { getDailyTokenBudget, getDailyTokenBudgets } = require('./_admin-settings');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const scope = String(req.query.scope || 'auto').toLowerCase();
    const normalizedScope = ['auto', 'manual', 'all'].includes(scope) ? scope : 'auto';
    const budgets = await getDailyTokenBudgets(sql);
    const dailyTokenBudget = normalizedScope === 'manual'
      ? budgets.manual
      : (normalizedScope === 'auto' ? budgets.auto : (budgets.auto + budgets.manual));

    const dailyRows = await sql`
      SELECT COALESCE(SUM(total_tokens), 0)::int AS "tokensUsedToday"
      , COUNT(*)::int AS "draftsToday"
      FROM article_drafts
      WHERE created_at >= date_trunc('day', now())
        AND (${normalizedScope} = 'all' OR created_via = ${normalizedScope})
    `;

    const monthlyRows = await sql`
      SELECT
        COALESCE(SUM(total_tokens), 0)::int AS "tokensUsedMonth",
        COUNT(*)::int AS "draftsMonth"
      FROM article_drafts
      WHERE created_at >= date_trunc('month', now())
        AND (${normalizedScope} = 'all' OR created_via = ${normalizedScope})
    `;

    const tokensUsedToday = dailyRows?.[0]?.tokensUsedToday || 0;
    const draftsToday = dailyRows?.[0]?.draftsToday || 0;
    const tokensUsedMonth = monthlyRows?.[0]?.tokensUsedMonth || 0;
    const draftsMonth = monthlyRows?.[0]?.draftsMonth || 0;
    const budgetUsedPercent = dailyTokenBudget > 0
      ? Math.min((tokensUsedToday / dailyTokenBudget) * 100, 100)
      : 0;
    const budgetRemainingPercent = Math.max(0, 100 - budgetUsedPercent);

    return res.status(200).json({
      ok: true,
      scope: normalizedScope,
      tokensUsedToday,
      dailyTokenBudget,
      dailyTokenBudgetAuto: budgets.auto,
      dailyTokenBudgetManual: budgets.manual,
      budgetUsedPercent: Number(budgetUsedPercent.toFixed(1)),
      budgetRemainingPercent: Number(budgetRemainingPercent.toFixed(1)),
      // Backward-compatible alias used by existing UI.
      budgetPercent: Number(budgetUsedPercent.toFixed(1)),
      draftsToday,
      tokensRemainingToday: Math.max(0, dailyTokenBudget - tokensUsedToday),
      dailyTokensUsed: tokensUsedToday,
      dailyDrafts: draftsToday,
      monthlyTokensUsed: tokensUsedMonth,
      monthlyDrafts: draftsMonth
    });
  } catch (error) {
    console.error('Admin usage error:', error);
    return res.status(500).json({ error: 'Failed to load usage metrics' });
  }
};
