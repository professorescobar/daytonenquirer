const { requireAdmin } = require('./_admin-auth');
const { getBroadcast, getBroadcastStats } = require('./_kit-client');
const {
  getSql,
  ensureNewsletterTables,
  getCampaignById,
  updateCampaignDelivery,
  appendCampaignEvent
} = require('./_newsletter-store');

function parsePercent(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Math.max(0, Math.min(100, Math.round(num)));
}

function mapKitStatus(payload, statsPayload) {
  const statusRaw = String(
    payload?.broadcast?.status ||
    payload?.status ||
    statsPayload?.status ||
    ''
  ).toLowerCase();

  if (statusRaw.includes('sent') || statusRaw.includes('complete')) {
    return { status: 'sent', kitStatus: statusRaw || 'sent', kitProgress: 100, sentAt: new Date().toISOString() };
  }
  if (statusRaw.includes('fail') || statusRaw.includes('error')) {
    return { status: 'failed', kitStatus: statusRaw || 'failed', kitProgress: 0 };
  }

  const pct =
    statsPayload?.stats?.progress_percent ||
    statsPayload?.progress_percent ||
    payload?.broadcast?.progress_percent ||
    payload?.progress_percent ||
    0;

  return {
    status: 'sending',
    kitStatus: statusRaw || 'sending',
    kitProgress: parsePercent(pct)
  };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const id = Number(req.query.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ error: 'Missing or invalid campaign id' });
    }

    const refresh = String(req.query.refresh || '').toLowerCase() === '1'
      || String(req.query.refresh || '').toLowerCase() === 'true';

    const sql = getSql();
    await ensureNewsletterTables(sql);

    let campaign = await getCampaignById(sql, id);
    if (!campaign) return res.status(404).json({ error: 'Campaign not found' });

    if (refresh && campaign.kitBroadcastId) {
      let broadcastPayload = {};
      let statsPayload = {};

      try {
        [broadcastPayload, statsPayload] = await Promise.all([
          getBroadcast(campaign.kitBroadcastId),
          getBroadcastStats(campaign.kitBroadcastId)
        ]);
      } catch (error) {
        await appendCampaignEvent(sql, {
          campaignId: campaign.id,
          provider: 'kit',
          eventType: 'status_refresh_error',
          payload: { message: error.message || 'unknown error' }
        });
        return res.status(502).json({ error: `Status refresh failed: ${error.message}` });
      }

      const nextState = mapKitStatus(broadcastPayload, statsPayload);
      campaign = await updateCampaignDelivery(sql, campaign.id, {
        ...nextState,
        lastSyncedAt: new Date().toISOString()
      });

      await appendCampaignEvent(sql, {
        campaignId: campaign.id,
        provider: 'kit',
        eventType: 'status_refreshed',
        payload: { broadcastPayload, statsPayload }
      });
    }

    return res.status(200).json({ ok: true, campaign });
  } catch (error) {
    console.error('Newsletter status error:', error);
    return res.status(500).json({ error: error.message || 'Failed to fetch campaign status' });
  }
};
