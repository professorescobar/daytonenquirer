module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const expected = process.env.ADMIN_UI_PASSWORD || '';
  if (!expected) {
    return res.status(500).json({ error: 'Missing ADMIN_UI_PASSWORD env var' });
  }

  const password = String(req.body?.password || '');
  if (!password || password !== expected) {
    return res.status(401).json({ error: 'Invalid password' });
  }

  return res.status(200).json({ ok: true });
};
