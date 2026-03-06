const { requireAdmin } = require('./_admin-auth');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  return res.status(404).json({ error: 'Not found' });
};
