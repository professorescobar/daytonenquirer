const { requireAdmin } = require('./_admin-auth');
const {
  createSql,
  getDailyTokenBudgets,
  setDailyTokenBudget,
  getModelDailyTokenBudgets,
  setModelDailyTokenBudget
} = require('./_admin-settings');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  try {
    const sql = createSql();

    if (req.method === 'GET') {
      const budgets = await getDailyTokenBudgets(sql);
      const requestedModels = String(req.query.models || '')
        .split(',')
        .map((v) => String(v || '').trim())
        .filter(Boolean);
      const modelBudgets = requestedModels.length
        ? await getModelDailyTokenBudgets(sql, requestedModels, Math.floor(budgets.auto / 3))
        : {};
      return res.status(200).json({
        ok: true,
        dailyTokenBudgetAuto: budgets.auto,
        dailyTokenBudgetManual: budgets.manual,
        modelBudgets
      });
    }

    if (req.method === 'POST') {
      const autoBudgetInput = req.body?.dailyTokenBudgetAuto;
      const manualBudgetInput = req.body?.dailyTokenBudgetManual;
      const modelBudgetsInput = req.body?.modelBudgets;
      const hasModelBudgetInput = modelBudgetsInput && typeof modelBudgetsInput === 'object' && !Array.isArray(modelBudgetsInput);

      if (!autoBudgetInput && !manualBudgetInput && !hasModelBudgetInput) {
        return res.status(400).json({ error: 'Missing dailyTokenBudgetAuto, dailyTokenBudgetManual, or modelBudgets' });
      }

      let dailyTokenBudgetAuto = null;
      let dailyTokenBudgetManual = null;
      const savedModelBudgets = {};

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

      if (hasModelBudgetInput) {
        for (const [modelName, budgetValue] of Object.entries(modelBudgetsInput)) {
          if (!String(modelName || '').trim()) continue;
          const saved = await setModelDailyTokenBudget(sql, modelName, budgetValue);
          savedModelBudgets[modelName] = saved;
        }
      }

      return res.status(200).json({
        ok: true,
        dailyTokenBudgetAuto,
        dailyTokenBudgetManual,
        modelBudgets: savedModelBudgets
      });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    console.error('Admin budget error:', error);
    return res.status(500).json({ error: 'Failed to process budget request' });
  }
};
