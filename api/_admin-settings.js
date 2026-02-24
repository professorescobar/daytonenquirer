const { neon } = require('@neondatabase/serverless');

const DEFAULT_DAILY_TOKEN_BUDGET_AUTO = 350000;
const DEFAULT_DAILY_TOKEN_BUDGET_MANUAL = 80000;
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

function normalizeBudgetMode(mode) {
  return String(mode || '').toLowerCase() === 'manual' ? 'manual' : 'auto';
}

function recommendedModelBudget(modelName, mode = 'auto', fallbackBudget = 25000) {
  const normalizedMode = normalizeBudgetMode(mode);
  const family = detectModelFamily(modelName);
  let autoBudget = fallbackBudget;
  if (family === 'openai') autoBudget = 120000;
  else if (family === 'anthropic') autoBudget = 100000;
  else if (family === 'gemini') autoBudget = 100000;
  else if (family === 'grok') autoBudget = 90000;
  if (normalizedMode === 'manual') {
    return Math.max(1, Math.floor(autoBudget / 3));
  }
  return autoBudget;
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

async function getModelDailyTokenBudget(sql, modelName, fallbackBudget = null, mode = 'auto') {
  await ensureAdminSettingsTable(sql);
  const normalizedMode = normalizeBudgetMode(mode);
  const normalizedModel = normalizeBudgetModelName(modelName);
  if (!normalizedModel) {
    const fallback = Number.isFinite(fallbackBudget) && fallbackBudget > 0 ? fallbackBudget : 25000;
    return Math.min(Math.max(1, Math.floor(fallback)), MAX_DAILY_TOKEN_BUDGET);
  }

  const modeKey = `daily_token_budget_model_${normalizedMode}_${normalizedModel}`;
  const dbModeValue = await getSettingInt(sql, modeKey);
  if (dbModeValue) return dbModeValue;

  const key = `daily_token_budget_model_${normalizedModel}`;
  const dbValue = await getSettingInt(sql, key);
  if (dbValue && normalizedMode === 'auto') return dbValue;

  const envModeKey = `DAILY_TOKEN_BUDGET_MODEL_${normalizedMode.toUpperCase()}_${normalizedModel.toUpperCase()}`;
  let envValue = parseInt(process.env[envModeKey] || '', 10);
  if (!Number.isFinite(envValue) && normalizedMode === 'auto') {
    const envKey = `DAILY_TOKEN_BUDGET_MODEL_${normalizedModel.toUpperCase()}`;
    envValue = parseInt(process.env[envKey] || '', 10);
  }
  if (Number.isFinite(envValue) && envValue > 0) {
    return Math.min(envValue, MAX_DAILY_TOKEN_BUDGET);
  }

  const fallback = Number.isFinite(fallbackBudget) && fallbackBudget > 0
    ? Math.floor(fallbackBudget)
    : 25000;
  return Math.min(
    recommendedModelBudget(modelName, normalizedMode, fallback),
    MAX_DAILY_TOKEN_BUDGET
  );
}

async function setModelDailyTokenBudget(sql, modelName, budget, mode = 'auto') {
  await ensureAdminSettingsTable(sql);
  const normalizedMode = normalizeBudgetMode(mode);
  const normalizedModel = normalizeBudgetModelName(modelName);
  if (!normalizedModel) {
    throw new Error('Invalid model name for budget update');
  }
  const numeric = Math.max(1, Math.min(parseInt(String(budget), 10), MAX_DAILY_TOKEN_BUDGET));
  const key = `daily_token_budget_model_${normalizedMode}_${normalizedModel}`;
  await sql`
    INSERT INTO admin_settings (key, value, updated_at)
    VALUES (${key}, ${String(numeric)}, NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = NOW()
  `;
  return numeric;
}

async function getModelDailyTokenBudgets(sql, modelNames, fallbackBudget = null, mode = 'auto') {
  const names = Array.isArray(modelNames) ? modelNames : [];
  const out = {};
  await Promise.all(
    names.map(async (name) => {
      out[name] = await getModelDailyTokenBudget(sql, name, fallbackBudget, mode);
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
