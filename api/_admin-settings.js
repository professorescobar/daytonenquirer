const { neon } = require('@neondatabase/serverless');

const DEFAULT_DAILY_TOKEN_BUDGET_AUTO = 100000;
const DEFAULT_DAILY_TOKEN_BUDGET_MANUAL = 30000;
const MAX_DAILY_TOKEN_BUDGET = 1000000;

async function getSettingInt(sql, key) {
  const rows = await sql`
    SELECT value
    FROM admin_settings
    WHERE key = ${key}
    LIMIT 1
  `;
  const value = parseInt(rows?.[0]?.value || '', 10);
  return Number.isFinite(value) && value > 0
    ? Math.min(value, MAX_DAILY_TOKEN_BUDGET)
    : null;
}

async function getDailyTokenBudget(sql, mode = 'auto') {
  const normalizedMode = String(mode || 'auto').toLowerCase() === 'manual' ? 'manual' : 'auto';
  const modeKey = normalizedMode === 'manual'
    ? 'daily_token_budget_manual'
    : 'daily_token_budget_auto';

  const modeDbValue = await getSettingInt(sql, modeKey);
  if (modeDbValue) return modeDbValue;

  const envValue = parseInt(
    normalizedMode === 'manual'
      ? (process.env.DAILY_TOKEN_BUDGET_MANUAL || String(DEFAULT_DAILY_TOKEN_BUDGET_MANUAL))
      : (process.env.DAILY_TOKEN_BUDGET_AUTO || process.env.DAILY_TOKEN_BUDGET || String(DEFAULT_DAILY_TOKEN_BUDGET_AUTO)),
    10
  );
  if (Number.isFinite(envValue) && envValue > 0) {
    return Math.min(envValue, MAX_DAILY_TOKEN_BUDGET);
  }

  return normalizedMode === 'manual'
    ? DEFAULT_DAILY_TOKEN_BUDGET_MANUAL
    : DEFAULT_DAILY_TOKEN_BUDGET_AUTO;
}

async function getDailyTokenBudgets(sql) {
  const [auto, manual] = await Promise.all([
    getDailyTokenBudget(sql, 'auto'),
    getDailyTokenBudget(sql, 'manual')
  ]);
  return { auto, manual };
}

async function setDailyTokenBudget(sql, budget, mode = 'auto') {
  const numeric = Math.max(1, Math.min(parseInt(String(budget), 10), MAX_DAILY_TOKEN_BUDGET));
  const normalizedMode = String(mode || 'auto').toLowerCase() === 'manual' ? 'manual' : 'auto';
  const key = normalizedMode === 'manual'
    ? 'daily_token_budget_manual'
    : 'daily_token_budget_auto';
  await sql`
    INSERT INTO admin_settings (key, value, updated_at)
    VALUES (${key}, ${String(numeric)}, NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = NOW()
  `;
  return numeric;
}

function createSql() {
  return neon(process.env.DATABASE_URL);
}

module.exports = {
  createSql,
  getDailyTokenBudget,
  getDailyTokenBudgets,
  setDailyTokenBudget
};
