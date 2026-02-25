const { neon } = require('@neondatabase/serverless');

const DEFAULT_DAILY_TOKEN_BUDGET_MANUAL = 350000;
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
  return 'manual';
}

function recommendedModelBudget(modelName, mode = 'manual', fallbackBudget = 25000) {
  const family = detectModelFamily(modelName);
  let manualBudget = fallbackBudget;
  if (family === 'openai') manualBudget = 120000;
  else if (family === 'anthropic') manualBudget = 100000;
  else if (family === 'gemini') manualBudget = 100000;
  else if (family === 'grok') manualBudget = 90000;
  return Math.max(1, Math.floor(manualBudget));
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
  const normalizedMode = 'manual';
  const modeKey = 'daily_token_budget_manual';

  await ensureAdminSettingsTable(sql);
  const migrationRows = await sql`
    SELECT key
    FROM admin_settings
    WHERE key = 'budget_mode_manual_only_migration_v1'
    LIMIT 1
  `;
  if (!migrationRows.length) {
    const legacyAutoDbValue = await getSettingInt(sql, 'daily_token_budget_auto');
    if (legacyAutoDbValue) {
      await sql`
        INSERT INTO admin_settings (key, value, updated_at)
        VALUES ('daily_token_budget_manual', ${String(legacyAutoDbValue)}, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = NOW()
      `;
    }
    await sql`
      INSERT INTO admin_settings (key, value, updated_at)
      VALUES ('budget_mode_manual_only_migration_v1', 'done', NOW())
      ON CONFLICT (key) DO NOTHING
    `;
  }

  const modeDbValue = await getSettingInt(sql, modeKey);
  if (modeDbValue) return modeDbValue;

  const legacyAutoDbValue = await getSettingInt(sql, 'daily_token_budget_auto');
  if (legacyAutoDbValue) return legacyAutoDbValue;

  const envValue = parseInt(
    process.env.DAILY_TOKEN_BUDGET_MANUAL ||
      process.env.DAILY_TOKEN_BUDGET_AUTO ||
      process.env.DAILY_TOKEN_BUDGET ||
      String(DEFAULT_DAILY_TOKEN_BUDGET_MANUAL),
    10
  );
  if (Number.isFinite(envValue) && envValue > 0) {
    return Math.min(envValue, MAX_DAILY_TOKEN_BUDGET);
  }

  return DEFAULT_DAILY_TOKEN_BUDGET_MANUAL;
}

async function getDailyTokenBudgets(sql) {
  const manual = await getDailyTokenBudget(sql, 'manual');
  return { auto: manual, manual };
}

async function setDailyTokenBudget(sql, budget, mode = 'auto') {
  await ensureAdminSettingsTable(sql);
  const numeric = Math.max(1, Math.min(parseInt(String(budget), 10), MAX_DAILY_TOKEN_BUDGET));
  const key = 'daily_token_budget_manual';
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
  const normalizedMode = 'manual';
  const normalizedModel = normalizeBudgetModelName(modelName);
  if (!normalizedModel) {
    const fallback = Number.isFinite(fallbackBudget) && fallbackBudget > 0 ? fallbackBudget : 25000;
    return Math.min(Math.max(1, Math.floor(fallback)), MAX_DAILY_TOKEN_BUDGET);
  }

  const modeKey = `daily_token_budget_model_${normalizedMode}_${normalizedModel}`;
  const dbModeValue = await getSettingInt(sql, modeKey);
  if (dbModeValue) return dbModeValue;

  const legacyAutoKey = `daily_token_budget_model_auto_${normalizedModel}`;
  const legacyAutoValue = await getSettingInt(sql, legacyAutoKey);
  if (legacyAutoValue) return legacyAutoValue;

  const legacyKey = `daily_token_budget_model_${normalizedModel}`;
  const legacyValue = await getSettingInt(sql, legacyKey);
  if (legacyValue) return legacyValue;

  const envModeKey = `DAILY_TOKEN_BUDGET_MODEL_${normalizedMode.toUpperCase()}_${normalizedModel.toUpperCase()}`;
  let envValue = parseInt(process.env[envModeKey] || '', 10);
  if (!Number.isFinite(envValue)) {
    const envAutoKey = `DAILY_TOKEN_BUDGET_MODEL_AUTO_${normalizedModel.toUpperCase()}`;
    envValue = parseInt(process.env[envAutoKey] || '', 10);
  }
  if (!Number.isFinite(envValue)) {
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
  const normalizedMode = 'manual';
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
