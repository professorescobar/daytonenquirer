const path = require('path');
const fs = require('fs');

module.exports = function getCustomArticles(section) {
  try {
    const filePath = path.join(process.cwd(), 'content', 'custom-articles.json');
    const raw = fs.readFileSync(filePath, 'utf8');
    const all = JSON.parse(raw);
    return all.filter(article => article.section === section || article.section === 'all');
  } catch (err) {
    console.error('Failed to load custom articles:', err.message);
    return [];
  }
};