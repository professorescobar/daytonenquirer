const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { ensureTopicEngineTables } = require('./_topic-engine-workflow');

function cleanText(value, max = 255) {
  return String(value || '').trim().slice(0, max);
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureTopicEngineTables(sql);

    if (req.method === 'GET') {
      const rows = await sql`
        SELECT
          te.persona_id as "personaId",
          te.is_auto_promote_enabled as "isAutoPromoteEnabled",
          te.updated_at as "updatedAt"
        FROM topic_engines te
        ORDER BY te.persona_id ASC
      `;
      return res.status(200).json({ topicEngines: rows });
    }

    if (req.method === 'PUT') {
      const personaId = cleanText(req.body?.personaId || req.body?.id || '', 255);
      if (!personaId) return res.status(400).json({ error: 'personaId is required' });
      const isAutoPromoteEnabled = req.body?.isAutoPromoteEnabled === true;

      const rows = await sql`
        INSERT INTO topic_engines (persona_id, is_auto_promote_enabled, updated_at)
        VALUES (${personaId}, ${isAutoPromoteEnabled}, NOW())
        ON CONFLICT (persona_id) DO UPDATE
        SET
          is_auto_promote_enabled = EXCLUDED.is_auto_promote_enabled,
          updated_at = NOW()
        RETURNING
          persona_id as "personaId",
          is_auto_promote_enabled as "isAutoPromoteEnabled",
          updated_at as "updatedAt"
      `;
      return res.status(200).json({ topicEngine: rows[0] || null });
    }

    res.setHeader('Allow', ['GET', 'PUT']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  } catch (error) {
    console.error('Admin topic engines error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Failed to manage topic engines' });
  }
};
