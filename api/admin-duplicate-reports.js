const { requireAdmin } = require("./_admin-auth");

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  return res.status(410).json({ error: "This endpoint has been removed" });
};
