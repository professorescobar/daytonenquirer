const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { getDailyTokenBudget } = require('./_admin-settings');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const dailyTokenBudget = await getDailyTokenBudget(sql);

    const tokenRows = await sql`
      SELECT COALESCE(SUM(total_tokens), 0)::int AS "tokensUsedToday"
      FROM article_drafts
      WHERE created_at >= date_trunc('day', now())
    `;

    const draftRows = await sql`
      SELECT COUNT(*)::int AS "draftsToday"
      FROM article_drafts
      WHERE created_at >= date_trunc('day', now())
    `;

    const tokensUsedToday = tokenRows?.[0]?.tokensUsedToday || 0;
    const draftsToday = draftRows?.[0]?.draftsToday || 0;
    const budgetPercent = dailyTokenBudget > 0
      ? Math.min((tokensUsedToday / dailyTokenBudget) * 100, 100)
      : 0;

    return res.status(200).json({
      ok: true,
      tokensUsedToday,
      dailyTokenBudget,
      budgetPercent: Number(budgetPercent.toFixed(1)),
      draftsToday
    });
  } catch (error) {
    console.error('Admin usage error:', error);
    return res.status(500).json({ error: 'Failed to load usage metrics' });
  }
};
