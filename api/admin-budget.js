const { requireAdmin } = require('./_admin-auth');
const { createSql, getDailyTokenBudgets, setDailyTokenBudget } = require('./_admin-settings');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  try {
    const sql = createSql();

    if (req.method === 'GET') {
      const budgets = await getDailyTokenBudgets(sql);
      return res.status(200).json({
        ok: true,
        dailyTokenBudgetAuto: budgets.auto,
        dailyTokenBudgetManual: budgets.manual
      });
    }

    if (req.method === 'POST') {
      const autoBudgetInput = req.body?.dailyTokenBudgetAuto;
      const manualBudgetInput = req.body?.dailyTokenBudgetManual;
      if (!autoBudgetInput && !manualBudgetInput) {
        return res.status(400).json({ error: 'Missing dailyTokenBudgetAuto or dailyTokenBudgetManual' });
      }

      let dailyTokenBudgetAuto = null;
      let dailyTokenBudgetManual = null;

      if (autoBudgetInput) {
        dailyTokenBudgetAuto = await setDailyTokenBudget(sql, autoBudgetInput, 'auto');
      }
      if (manualBudgetInput) {
        dailyTokenBudgetManual = await setDailyTokenBudget(sql, manualBudgetInput, 'manual');
      }

      if (dailyTokenBudgetAuto === null || dailyTokenBudgetManual === null) {
        const budgets = await getDailyTokenBudgets(sql);
        if (dailyTokenBudgetAuto === null) dailyTokenBudgetAuto = budgets.auto;
        if (dailyTokenBudgetManual === null) dailyTokenBudgetManual = budgets.manual;
      }

      return res.status(200).json({
        ok: true,
        dailyTokenBudgetAuto,
        dailyTokenBudgetManual
      });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    console.error('Admin budget error:', error);
    return res.status(500).json({ error: 'Failed to process budget request' });
  }
};
