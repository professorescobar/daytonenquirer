const getCustomArticles = require('./custom-articles');

module.exports = async (req, res) => {
  try {
    // Get custom articles for this section
    const customArticles = getCustomArticles('world');
    
    // Sort by date (most recent first)
    customArticles.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Featured article = most recent with image
    const featuredArticle = customArticles.find(a => a.image);

    // Remaining articles for headlines
    const headlines = customArticles.filter(a => a !== featuredArticle);

    // Final articles array: featured first, then headlines
    const articles = featuredArticle 
      ? [featuredArticle, ...headlines] 
      : customArticles;

    res.status(200).json({ 
      articles, 
      articleCount: articles.length
    });
  } catch (err) {
    console.error("Full error:", err);
    res.status(500).json({ error: "Failed to fetch articles", details: err.message });
  }
};