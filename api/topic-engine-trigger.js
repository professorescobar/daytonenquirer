const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { runTopicEngineWorkflow } = require('./_topic-engine-workflow');

function requireWebhookOrAdmin(req, res) {
  const expectedSecret = String(process.env.TOPIC_ENGINE_WEBHOOK_SECRET || '').trim();
  if (expectedSecret) {
    const provided = String(
      req.headers['x-topic-engine-secret'] ||
      req.query.secret ||
      (req.body && req.body.secret) ||
      ''
    ).trim();
    if (!provided || provided !== expectedSecret) {
      res.status(401).json({ error: 'Unauthorized' });
      return false;
    }
    return true;
  }
  return requireAdmin(req, res);
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }
  if (!requireWebhookOrAdmin(req, res)) return;

  const personaId = String(req.body?.personaId || req.body?.engineId || '').trim();
  if (!personaId) {
    return res.status(400).json({ error: 'personaId is required' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const result = await runTopicEngineWorkflow(sql, {
      personaId,
      triggerMode: 'event',
      signal: req.body?.signal || req.body
    });

    if (!result.ok) {
      return res.status(400).json({ ok: false, error: result.reason || 'trigger_failed', details: result });
    }
    return res.status(200).json({
      ok: true,
      triggerMode: 'event',
      result
    });
  } catch (error) {
    console.error('Topic engine event trigger error:', error);
    return res.status(500).json({ error: 'Failed to process topic engine event trigger' });
  }
};

