const { neon } = require('@neondatabase/serverless');

function createSql() {
  return neon(process.env.DATABASE_URL);
}

async function ensureSettingsTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS system_settings (
      key TEXT PRIMARY KEY,
      value JSONB,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `;
}

async function getDailyTokenBudgets(sql) {
  await ensureSettingsTable(sql);
  const rows = await sql`SELECT key, value FROM system_settings WHERE key IN ('daily_token_budget_auto', 'daily_token_budget_manual')`;
  const auto = Number(rows.find(r => r.key === 'daily_token_budget_auto')?.value || 350000);
  const manual = Number(rows.find(r => r.key === 'daily_token_budget_manual')?.value || 350000);
  return { auto, manual };
}

async function setDailyTokenBudget(sql, amount, scope) {
  await ensureSettingsTable(sql);
  const key = scope === 'manual' ? 'daily_token_budget_manual' : 'daily_token_budget_auto';
  const val = Number(amount);
  await sql`
    INSERT INTO system_settings (key, value, updated_at)
    VALUES (${key}, ${val}::jsonb, NOW())
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
  `;
  return val;
}

async function getModelDailyTokenBudget(sql, model, fallback, scope) {
  await ensureSettingsTable(sql);
  const key = `budget_model_${model}`;
  const rows = await sql`SELECT value FROM system_settings WHERE key = ${key}`;
  if (rows.length) return Number(rows[0].value);
  return fallback;
}

async function getModelDailyTokenBudgets(sql, models, fallback, scope) {
  const out = {};
  for (const m of models) {
    out[m] = await getModelDailyTokenBudget(sql, m, fallback, scope);
  }
  return out;
}

async function setModelDailyTokenBudget(sql, model, amount, scope) {
  await ensureSettingsTable(sql);
  const key = `budget_model_${model}`;
  const val = Number(amount);
  await sql`
    INSERT INTO system_settings (key, value, updated_at)
    VALUES (${key}, ${val}::jsonb, NOW())
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
  `;
  return val;
}

module.exports = {
  createSql,
  getDailyTokenBudgets,
  setDailyTokenBudget,
  getModelDailyTokenBudget,
  getModelDailyTokenBudgets,
  setModelDailyTokenBudget
};