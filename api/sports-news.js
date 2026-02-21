const { getArticlesBySection } = require('./_db-helper');

export default async function handler(req, res) {
  try {
    const articles = await getArticlesBySection('sports');
    res.json({ articles });
  } catch (error) {
    console.error('Database error:', error);
    res.status(500).json({ error: 'Failed to fetch articles' });
  }
}