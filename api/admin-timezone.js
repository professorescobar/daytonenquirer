const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const SETTING_KEY = 'topic_engine_admin_timezone';
const DEFAULT_TIMEZONE = 'America/New_York';

function cleanText(value, max = 120) {
  return String(value || '').trim().slice(0, max);
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

async function loadTimezone(sql) {
  const rows = await sql`
    SELECT COALESCE(value #>> '{}', ${DEFAULT_TIMEZONE}) as timezone
    FROM system_settings
    WHERE key = ${SETTING_KEY}
    LIMIT 1
  `;
  return cleanText(rows?.[0]?.timezone || DEFAULT_TIMEZONE, 120) || DEFAULT_TIMEZONE;
}

async function timezoneExists(sql, timezone) {
  const rows = await sql`
    SELECT EXISTS(
      SELECT 1
      FROM pg_timezone_names
      WHERE name = ${timezone}
    ) as exists
  `;
  return Boolean(rows?.[0]?.exists);
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (!['GET', 'PUT'].includes(req.method)) {
    res.setHeader('Allow', ['GET', 'PUT']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureSettingsTable(sql);

    if (req.method === 'GET') {
      const timezone = await loadTimezone(sql);
      return res.status(200).json({ timezone });
    }

    const timezone = cleanText(req.body?.timezone, 120);
    if (!timezone) return res.status(400).json({ error: 'timezone is required' });
    if (!(await timezoneExists(sql, timezone))) {
      return res.status(400).json({ error: 'Invalid IANA timezone' });
    }

    await sql`
      INSERT INTO system_settings (key, value, updated_at)
      VALUES (${SETTING_KEY}, ${timezone}::jsonb, NOW())
      ON CONFLICT (key) DO UPDATE
      SET value = EXCLUDED.value, updated_at = NOW()
    `;
    return res.status(200).json({ ok: true, timezone });
  } catch (error) {
    console.error('Admin timezone API error:', error);
    return res.status(500).json({ error: 'Failed to update admin timezone' });
  }
};
