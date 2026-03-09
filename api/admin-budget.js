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
      const scope = 'manual';
      const requestedModels = String(req.query.models || '')
        .split(',')
        .map((v) => String(v || '').trim())
        .filter(Boolean);
      const modelBudgets = requestedModels.length
        ? await getModelDailyTokenBudgets(
          sql,
          requestedModels,
          Math.floor((budgets.manual || 0) / 3),
          scope
        )
        : {};
      return res.status(200).json({
        ok: true,
        dailyTokenBudgetAuto: budgets.auto,
        dailyTokenBudgetManual: budgets.manual,
        modelBudgets
      });
    }

    if (req.method === 'POST') {
      const legacyAutoBudgetInput = req.body?.dailyTokenBudgetAuto;
      const manualBudgetInput = req.body?.dailyTokenBudgetManual;
      const genericBudgetInput = req.body?.dailyTokenBudget;
      const modelBudgetsInput = req.body?.modelBudgets;
      const modelBudgetMode = 'manual';
      const hasModelBudgetInput = modelBudgetsInput && typeof modelBudgetsInput === 'object' && !Array.isArray(modelBudgetsInput);

      if (!legacyAutoBudgetInput && !manualBudgetInput && !genericBudgetInput && !hasModelBudgetInput) {
        return res.status(400).json({ error: 'Missing dailyTokenBudgetManual, dailyTokenBudget, or modelBudgets' });
      }

      let dailyTokenBudgetAuto = null;
      let dailyTokenBudgetManual = null;
      const savedModelBudgets = {};

      const chosenBudget = manualBudgetInput || genericBudgetInput || legacyAutoBudgetInput;
      if (chosenBudget) {
        dailyTokenBudgetManual = await setDailyTokenBudget(sql, chosenBudget, 'manual');
      }

      if (dailyTokenBudgetManual === null) {
        const budgets = await getDailyTokenBudgets(sql);
        if (dailyTokenBudgetManual === null) dailyTokenBudgetManual = budgets.manual;
      }
      dailyTokenBudgetAuto = dailyTokenBudgetManual;

      if (hasModelBudgetInput) {
        for (const [modelName, budgetValue] of Object.entries(modelBudgetsInput)) {
          if (!String(modelName || '').trim()) continue;
          const saved = await setModelDailyTokenBudget(sql, modelName, budgetValue, modelBudgetMode);
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
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Failed to process budget request' });
  }
};
