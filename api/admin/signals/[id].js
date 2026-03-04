const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('../../_admin-auth');

const VALID_ACTIONS = new Set(['promote', 'reject', 'watch']);

function cleanText(value, max = 2000) {
  return String(value || '').trim().slice(0, max);
}

async function emitManualNextStepEvent(payload) {
  const endpoint = cleanText(process.env.INNGEST_EVENT_URL || '', 1000);
  if (!endpoint) {
    return { attempted: false, sent: false, reason: 'missing_inggest_event_url' };
  }
  const key = cleanText(process.env.INNGEST_EVENT_KEY || '', 500);

  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(key ? { Authorization: `Bearer ${key}` } : {})
      },
      body: JSON.stringify({
        name: 'signal.gatekeeper.route.manual',
        data: payload
      })
    });
    return {
      attempted: true,
      sent: response.ok,
      status: response.status
    };
  } catch (error) {
    return {
      attempted: true,
      sent: false,
      reason: cleanText(error?.message || 'event_send_failed', 500)
    };
  }
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'PUT') {
    res.setHeader('Allow', ['PUT']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const id = Number.parseInt(String(req.query?.id || ''), 10);
  if (!Number.isFinite(id) || id < 1) {
    return res.status(400).json({ error: 'Valid signal id is required' });
  }

  const action = cleanText(req.body?.action || '', 30).toLowerCase();
  const reviewNotes = cleanText(req.body?.reviewNotes || req.body?.review_notes || '', 4000);
  if (!VALID_ACTIONS.has(action)) {
    return res.status(400).json({ error: 'action must be one of: promote, reject, watch' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const rows = await sql`
      UPDATE topic_signals
      SET
        action = ${action},
        next_step = CASE
          WHEN ${action} = 'promote'
            THEN CASE
              WHEN relation_to_archive = 'update' THEN 'cluster_update'
              ELSE 'research_discovery'
            END
          ELSE 'none'
        END,
        review_decision = CASE
          WHEN ${action} = 'promote' THEN 'promoted'
          WHEN ${action} = 'reject' THEN 'rejected'
          ELSE review_decision
        END,
        review_notes = CASE
          WHEN ${reviewNotes || null} IS NULL THEN review_notes
          WHEN length(trim(COALESCE(review_notes, ''))) = 0 THEN ${reviewNotes || null}
          ELSE review_notes || ' | ' || ${reviewNotes || null}
        END,
        processed_at = NOW(),
        updated_at = NOW()
      WHERE id = ${id}
      RETURNING
        id,
        persona_id as "personaId",
        title,
        action,
        next_step as "nextStep",
        review_decision as "reviewDecision",
        relation_to_archive as "relationToArchive",
        event_key as "eventKey",
        dedupe_key as "dedupeKey",
        processed_at as "processedAt"
    `;

    const signal = rows[0];
    if (!signal) {
      return res.status(404).json({ error: 'Signal not found' });
    }

    const triggerResult = await emitManualNextStepEvent({
      signalId: signal.id,
      personaId: signal.personaId,
      action: signal.action,
      nextStep: signal.nextStep,
      relationToArchive: signal.relationToArchive,
      eventKey: signal.eventKey || signal.dedupeKey,
      trigger: 'admin_manual'
    });

    return res.status(200).json({
      ok: true,
      signal,
      manualTrigger: triggerResult
    });
  } catch (error) {
    console.error('Admin signal update error:', error);
    if (String(error?.message || '').toLowerCase().includes('topic_signals')) {
      return res.status(500).json({
        error: 'Topic signal table missing',
        details: 'Run migration 20260304_05_gatekeeper_signals.sql first.'
      });
    }
    return res.status(500).json({ error: 'Failed to update signal' });
  }
};
