const { neon } = require('@neondatabase/serverless');

const DEFAULT_DAILY_TOKEN_BUDGET = 120000;
const MAX_DAILY_TOKEN_BUDGET = 1000000;

async function getDailyTokenBudget(sql) {
  const rows = await sql`
    SELECT value
    FROM admin_settings
    WHERE key = 'daily_token_budget'
    LIMIT 1
  `;
  const dbValue = parseInt(rows?.[0]?.value || '', 10);
  if (Number.isFinite(dbValue) && dbValue > 0) {
    return Math.min(dbValue, MAX_DAILY_TOKEN_BUDGET);
  }

  const envValue = parseInt(
    process.env.DAILY_TOKEN_BUDGET || String(DEFAULT_DAILY_TOKEN_BUDGET),
    10
  );
  if (Number.isFinite(envValue) && envValue > 0) {
    return Math.min(envValue, MAX_DAILY_TOKEN_BUDGET);
  }

  return DEFAULT_DAILY_TOKEN_BUDGET;
}

async function setDailyTokenBudget(sql, budget) {
  const numeric = Math.max(1, Math.min(parseInt(String(budget), 10), MAX_DAILY_TOKEN_BUDGET));
  await sql`
    INSERT INTO admin_settings (key, value, updated_at)
    VALUES ('daily_token_budget', ${String(numeric)}, NOW())
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
  setDailyTokenBudget
};
