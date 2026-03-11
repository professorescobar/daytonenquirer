const { requireAdmin } = require('./_admin-auth');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function cleanText(value, max = 1000) {
  return String(value || '').trim().slice(0, max);
}

function parsePositiveInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function getRequestOrigin(req) {
  const host = cleanText(req?.headers?.['x-forwarded-host'] || req?.headers?.host || '', 1000);
  if (!host) return '';
  const forwardedProto = cleanText(req?.headers?.['x-forwarded-proto'] || '', 50).toLowerCase();
  const protocol = forwardedProto || (host.startsWith('localhost') || host.startsWith('127.0.0.1') ? 'http' : 'https');
  return `${protocol}://${host}`;
}

function resolveInngestEventEndpoint(req) {
  const configured = cleanText(process.env.INNGEST_EVENT_URL || '', 1200);
  if (configured) return configured;
  const origin = getRequestOrigin(req);
  if (!origin) return '';
  return `${origin}/api/inngest`;
}

async function emitDictionaryDispatchEvent(req, payload) {
  const endpoint = resolveInngestEventEndpoint(req);
  if (!endpoint) {
    return { attempted: false, sent: false, reason: 'missing_inngest_event_url' };
  }

  const key = cleanText(process.env.INNGEST_EVENT_KEY || '', 600);
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(key ? { Authorization: `Bearer ${key}` } : {})
      },
      body: JSON.stringify({
        name: 'dictionary.substrate.ingestion.dispatch',
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
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const rootSourceId = cleanText(req.body?.rootSourceId || req.body?.root_source_id || '', 80) || null;
  const limit = parsePositiveInt(req.body?.limit, 25, 1, 200);

  if (rootSourceId && !UUID_RE.test(rootSourceId)) {
    return res.status(400).json({ error: 'rootSourceId must be a valid UUID' });
  }

  const eventPayload = {
    trigger: 'manual',
    limit,
    ...(rootSourceId ? { rootSourceId } : {})
  };

  const eventResult = await emitDictionaryDispatchEvent(req, eventPayload);
  if (!eventResult.sent) {
    return res.status(400).json({
      ok: false,
      error: eventResult.reason || 'dictionary_ingestion_dispatch_failed',
      event: eventResult
    });
  }

  return res.status(200).json({
    ok: true,
    triggerMode: 'event',
    event: eventResult,
    payload: eventPayload
  });
};
