function getAdminToken(req) {
  const authHeader = req.headers.authorization || '';
  const bearer = authHeader.startsWith('Bearer ')
    ? authHeader.slice('Bearer '.length).trim()
    : '';

  return (
    bearer ||
    req.headers['x-admin-token'] ||
    req.headers['x-cron-token'] ||
    req.query.token ||
    ''
  );
}

function requireAdmin(req, res) {
  const candidates = [
    process.env.ADMIN_API_TOKEN || '',
    process.env.CRON_SECRET || ''
  ].filter(Boolean);

  if (candidates.length === 0) {
    res.status(500).json({ error: 'Missing ADMIN_API_TOKEN or CRON_SECRET env var' });
    return false;
  }

  const token = getAdminToken(req);
  if (!token || !candidates.includes(token)) {
    res.status(401).json({ error: 'Unauthorized' });
    return false;
  }

  return true;
}

module.exports = { requireAdmin };
