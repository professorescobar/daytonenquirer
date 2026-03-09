module.exports = async (req, res) => {
  return res.status(410).json({
    error: 'Setup endpoint disabled',
    details: 'Schema setup must run via migrations, not public runtime routes.'
  });
};
