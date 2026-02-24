module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  return res.status(200).json({
    turnstileSiteKey: String(process.env.TURNSTILE_SITE_KEY || '').trim()
  });
};
