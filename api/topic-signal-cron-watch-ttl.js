const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') {
    res.setHeader('Allow', ['POST']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const rows = await sql`
      WITH expired_watch AS (
        SELECT s.id, COALESCE(NULLIF(s.event_key, ''), s.dedupe_key) AS corr_key
        FROM topic_signals s
        WHERE s.action = 'watch'
          AND s.created_at < NOW() - interval '48 hours'
      ),
      corroborated AS (
        SELECT ew.id
        FROM expired_watch ew
        JOIN topic_signals s2
          ON COALESCE(NULLIF(s2.event_key, ''), s2.dedupe_key) = ew.corr_key
         AND s2.id <> ew.id
         AND s2.created_at > NOW() - interval '48 hours'
        GROUP BY ew.id
        HAVING
          COUNT(DISTINCT CASE WHEN s2.source_type IN ('rss', 'webhook') THEN s2.source_type END) >= 1
          OR COUNT(DISTINCT CASE WHEN s2.source_type IN ('chat_yes', 'chat_specify') THEN s2.session_hash END) >= 2
      ),
      updated AS (
        UPDATE topic_signals s
        SET
          action = 'reject',
          next_step = 'none',
          review_decision = 'rejected',
          review_notes = CASE
            WHEN length(trim(COALESCE(s.review_notes, ''))) = 0
              THEN 'Auto-rejected: watch TTL expired without corroboration'
            ELSE s.review_notes || ' | Auto-rejected: watch TTL expired without corroboration'
          END,
          policy_flags = array_append(s.policy_flags, 'watch_ttl_expired'),
          processed_at = NOW(),
          updated_at = NOW()
        WHERE s.id IN (SELECT id FROM expired_watch)
          AND s.id NOT IN (SELECT id FROM corroborated)
        RETURNING s.id
      )
      SELECT COUNT(*)::int as "updatedCount"
      FROM updated
    `;

    return res.status(200).json({
      ok: true,
      updatedCount: Number(rows[0]?.updatedCount || 0)
    });
  } catch (error) {
    console.error('Watch TTL cleanup error:', error);
    if (String(error?.message || '').toLowerCase().includes('topic_signals')) {
      return res.status(500).json({
        error: 'Topic signal table missing',
        details: 'Run migration 20260304_05_gatekeeper_signals.sql first.'
      });
    }
    return res.status(500).json({ error: 'Failed to run watch TTL cleanup' });
  }
};
