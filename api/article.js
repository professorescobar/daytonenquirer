const getCustomArticles = require('./custom-articles');

module.exports = async (req, res) => {
  const { slug } = req.query;
  
  if (!slug) {
    return res.status(400).json({ error: 'Slug required' });
  }

  // Get all custom articles
  const allCustoms = [
    ...getCustomArticles('local'),
    ...getCustomArticles('national'),
    ...getCustomArticles('world'),
    ...getCustomArticles('business'),
    ...getCustomArticles('sports'),
    ...getCustomArticles('health'),
    ...getCustomArticles('entertainment'),
    ...getCustomArticles('technology'),
    ...getCustomArticles('all')
  ];

  // Find article by slug
  const article = allCustoms.find(a => a.url === slug);

  if (!article) {
    return res.status(404).json({ error: 'Article not found' });
  }

  res.status(200).json({ article });
};