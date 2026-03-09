const { verifyKitWebhook } = require('./_webhook-verify');
const {
  getSql,
  ensureNewsletterTables,
  getCampaignByKitBroadcastId,
  updateCampaignDelivery,
  appendCampaignEvent
} = require('./_newsletter-store');

function asPositiveInt(value) {
  const num = Number(value);
  return Number.isInteger(num) && num > 0 ? num : null;
}

function getEventName(payload) {
  return String(
    payload?.event?.name ||
    payload?.event_name ||
    payload?.type ||
    payload?.name ||
    'kit_webhook'
  );
}

function getBroadcastId(payload) {
  const candidates = [
    payload?.broadcast?.id,
    payload?.broadcast_id,
    payload?.data?.broadcast?.id,
    payload?.data?.broadcast_id
  ];
  for (const candidate of candidates) {
    const parsed = asPositiveInt(candidate);
    if (parsed) return parsed;
  }
  return null;
}

function mapStatusFromEvent(eventName, payload) {
  const name = String(eventName || '').toLowerCase();
  if (name.includes('sent') || name.includes('delivered') || name.includes('complete')) {
    return { status: 'sent', kitStatus: name, kitProgress: 100, sentAt: new Date().toISOString() };
  }
  if (name.includes('fail') || name.includes('error')) {
    return { status: 'failed', kitStatus: name, kitProgress: 0 };
  }
  const progress = Number(
    payload?.broadcast?.progress_percent ||
    payload?.progress_percent ||
    payload?.stats?.progress_percent ||
    0
  );
  const boundedProgress = Math.max(0, Math.min(100, Number.isFinite(progress) ? progress : 0));
  return { status: 'sending', kitStatus: name || 'sending', kitProgress: Math.round(boundedProgress) };
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const verification = verifyKitWebhook(req);
  if (!verification.ok) {
    return res.status(verification.status || 401).json({ error: verification.error || 'Unauthorized' });
  }

  try {
    const sql = getSql();
    await ensureNewsletterTables(sql);
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const eventName = getEventName(payload);
    const broadcastId = getBroadcastId(payload);
    let campaignId = null;

    if (broadcastId) {
      const campaign = await getCampaignByKitBroadcastId(sql, broadcastId);
      if (campaign) {
        campaignId = campaign.id;
        const nextState = mapStatusFromEvent(eventName, payload);
        await updateCampaignDelivery(sql, campaign.id, {
          ...nextState,
          kitBroadcastId: broadcastId,
          lastSyncedAt: new Date().toISOString()
        });
      }
    }

    if (campaignId) {
      await appendCampaignEvent(sql, {
        campaignId,
        provider: 'kit',
        eventType: eventName,
        payload
      });
    }
  } catch (error) {
    console.error('Kit webhook processing error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Webhook processing failed' });
  }

  return res.status(200).json({
    ok: true,
    verifiedBy: verification.verifiedBy,
    receivedAt: new Date().toISOString()
  });
};
