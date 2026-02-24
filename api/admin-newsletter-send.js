const { requireAdmin } = require('./_admin-auth');
const { createBroadcast } = require('./_kit-client');
const {
  getSql,
  ensureNewsletterTables,
  getCampaignById,
  updateCampaignDelivery,
  appendCampaignEvent
} = require('./_newsletter-store');

function asPositiveInt(value) {
  const num = Number(value);
  return Number.isInteger(num) && num > 0 ? num : null;
}

function extractBroadcastId(payload) {
  const candidates = [
    payload?.broadcast?.id,
    payload?.id,
    payload?.data?.id,
    payload?.data?.broadcast?.id
  ];
  for (const candidate of candidates) {
    const parsed = asPositiveInt(candidate);
    if (parsed) return parsed;
  }
  return null;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const campaignId = asPositiveInt(req.body?.id);
    if (!campaignId) return res.status(400).json({ error: 'Missing campaign id' });

    const sql = getSql();
    await ensureNewsletterTables(sql);
    const campaign = await getCampaignById(sql, campaignId);
    if (!campaign) return res.status(404).json({ error: 'Campaign not found' });
    if (campaign.status === 'sending' || campaign.status === 'sent') {
      return res.status(400).json({ error: 'Campaign already sent or currently sending' });
    }
    if (!String(campaign.subject || '').trim()) {
      return res.status(400).json({ error: 'Campaign subject is required before sending' });
    }
    if (!String(campaign.contentHtml || campaign.contentText || '').trim()) {
      return res.status(400).json({ error: 'Campaign content is required before sending' });
    }

    const providerResponse = await createBroadcast(campaign);
    const kitBroadcastId = extractBroadcastId(providerResponse);

    const updatedCampaign = await updateCampaignDelivery(sql, campaign.id, {
      status: 'sending',
      kitStatus: 'queued_in_kit',
      kitProgress: 0,
      kitBroadcastId,
      lastSyncedAt: new Date().toISOString()
    });

    await appendCampaignEvent(sql, {
      campaignId: campaign.id,
      provider: 'kit',
      eventType: 'broadcast_created',
      payload: providerResponse
    });

    return res.status(200).json({
      ok: true,
      campaign: updatedCampaign,
      provider: {
        name: 'kit',
        broadcastId: kitBroadcastId
      }
    });
  } catch (error) {
    console.error('Newsletter send error:', error);
    return res.status(500).json({ error: error.message || 'Failed to send campaign via Kit' });
  }
};
