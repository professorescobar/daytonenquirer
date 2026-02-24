const { neon } = require('@neondatabase/serverless');

const DEFAULT_DAILY_TOKEN_BUDGET_AUTO = 100000;
const DEFAULT_DAILY_TOKEN_BUDGET_MANUAL = 30000;
const MAX_DAILY_TOKEN_BUDGET = 1000000;

function normalizeBudgetModelName(modelName) {
  return String(modelName || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function detectModelFamily(modelName) {
  const value = String(modelName || '').toLowerCase();
  if (value.includes('gpt')) return 'openai';
  if (value.includes('claude')) return 'anthropic';
  if (value.includes('gemini')) return 'gemini';
  if (value.includes('grok')) return 'grok';
  return 'other';
}

function recommendedModelBudget(modelName, fallbackBudget = 25000) {
  const family = detectModelFamily(modelName);
  if (family === 'openai') return 45000;
  if (family === 'anthropic') return 35000;
  if (family === 'gemini') return 35000;
  if (family === 'grok') return 30000;
  return fallbackBudget;
}

async function ensureAdminSettingsTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS admin_settings (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `;
}

async function getSettingInt(sql, key) {
  await ensureAdminSettingsTable(sql);
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
  await ensureAdminSettingsTable(sql);
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

async function getModelDailyTokenBudget(sql, modelName, fallbackBudget = null) {
  await ensureAdminSettingsTable(sql);
  const normalizedModel = normalizeBudgetModelName(modelName);
  if (!normalizedModel) {
    const fallback = Number.isFinite(fallbackBudget) && fallbackBudget > 0 ? fallbackBudget : 25000;
    return Math.min(Math.max(1, Math.floor(fallback)), MAX_DAILY_TOKEN_BUDGET);
  }

  const key = `daily_token_budget_model_${normalizedModel}`;
  const dbValue = await getSettingInt(sql, key);
  if (dbValue) return dbValue;

  const envKey = `DAILY_TOKEN_BUDGET_MODEL_${normalizedModel.toUpperCase()}`;
  const envValue = parseInt(process.env[envKey] || '', 10);
  if (Number.isFinite(envValue) && envValue > 0) {
    return Math.min(envValue, MAX_DAILY_TOKEN_BUDGET);
  }

  const fallback = Number.isFinite(fallbackBudget) && fallbackBudget > 0
    ? Math.floor(fallbackBudget)
    : 25000;
  return Math.min(
    recommendedModelBudget(modelName, fallback),
    MAX_DAILY_TOKEN_BUDGET
  );
}

async function setModelDailyTokenBudget(sql, modelName, budget) {
  await ensureAdminSettingsTable(sql);
  const normalizedModel = normalizeBudgetModelName(modelName);
  if (!normalizedModel) {
    throw new Error('Invalid model name for budget update');
  }
  const numeric = Math.max(1, Math.min(parseInt(String(budget), 10), MAX_DAILY_TOKEN_BUDGET));
  const key = `daily_token_budget_model_${normalizedModel}`;
  await sql`
    INSERT INTO admin_settings (key, value, updated_at)
    VALUES (${key}, ${String(numeric)}, NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = NOW()
  `;
  return numeric;
}

async function getModelDailyTokenBudgets(sql, modelNames, fallbackBudget = null) {
  const names = Array.isArray(modelNames) ? modelNames : [];
  const out = {};
  await Promise.all(
    names.map(async (name) => {
      out[name] = await getModelDailyTokenBudget(sql, name, fallbackBudget);
    })
  );
  return out;
}

function createSql() {
  return neon(process.env.DATABASE_URL);
}

module.exports = {
  createSql,
  getDailyTokenBudget,
  getDailyTokenBudgets,
  setDailyTokenBudget,
  getModelDailyTokenBudget,
  setModelDailyTokenBudget,
  getModelDailyTokenBudgets
};
