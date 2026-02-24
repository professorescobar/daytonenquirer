const { requireAdmin } = require('./_admin-auth');
const {
  getSql,
  ensureNewsletterTables,
  listCampaigns,
  createCampaign,
  updateCampaign,
  getCampaignById
} = require('./_newsletter-store');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  try {
    const sql = getSql();
    await ensureNewsletterTables(sql);

    if (req.method === 'GET') {
      const campaigns = await listCampaigns(sql, {
        status: req.query.status,
        limit: req.query.limit
      });
      return res.status(200).json({ campaigns, count: campaigns.length });
    }

    if (req.method === 'POST') {
      const campaign = await createCampaign(sql, req.body || {});
      return res.status(200).json({ ok: true, campaign });
    }

    if (req.method === 'PATCH') {
      const id = Number(req.body?.id);
      if (!Number.isInteger(id) || id <= 0) {
        return res.status(400).json({ error: 'Missing or invalid campaign id' });
      }
      const current = await getCampaignById(sql, id);
      if (!current) return res.status(404).json({ error: 'Campaign not found' });
      if (current.status === 'sending' || current.status === 'sent') {
        return res.status(400).json({ error: 'Campaign is locked after send' });
      }
      const campaign = await updateCampaign(sql, id, req.body || {});
      return res.status(200).json({ ok: true, campaign });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    console.error('Newsletter campaigns error:', error);
    return res.status(500).json({ error: 'Failed to manage newsletter campaigns' });
  }
};
