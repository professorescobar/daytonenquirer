const { requireAdmin } = require('./_admin-auth');
const { createSql, getDailyTokenBudget, setDailyTokenBudget } = require('./_admin-settings');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  try {
    const sql = createSql();

    if (req.method === 'GET') {
      const dailyTokenBudget = await getDailyTokenBudget(sql);
      return res.status(200).json({ ok: true, dailyTokenBudget });
    }

    if (req.method === 'POST') {
      const budget = req.body?.dailyTokenBudget;
      if (!budget) {
        return res.status(400).json({ error: 'Missing dailyTokenBudget' });
      }
      const dailyTokenBudget = await setDailyTokenBudget(sql, budget);
      return res.status(200).json({ ok: true, dailyTokenBudget });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    console.error('Admin budget error:', error);
    return res.status(500).json({ error: 'Failed to process budget request' });
  }
};
