const getCustomArticles = require('./custom-articles');

module.exports = async (req, res) => {
  try {
    // Get custom articles from all sections
    const sections = ['local', 'national', 'world', 'business', 'sports', 'health', 'entertainment', 'technology'];
    const categoryMap = {
      local: 'Local',
      national: 'National',
      world: 'World',
      business: 'Business',
      sports: 'Sports',
      health: 'Health',
      entertainment: 'Entertainment',
      technology: 'Technology'
    };

    const carouselStories = [];

    // Get the most recent custom article with an image from each section
    for (const section of sections) {
      const customArticles = getCustomArticles(section);
      
      // Find most recent with image
      const withImages = customArticles
        .filter(article => article.image)
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
      
      if (withImages.length > 0) {
        carouselStories.push({
          ...withImages[0],
          category: categoryMap[section]
        });
      }
    }

    // Also get any "all" section articles
    const allArticles = getCustomArticles('all')
      .filter(article => article.image)
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
    
    if (allArticles.length > 0) {
      carouselStories.push(...allArticles.slice(0, 2));
    }

    // Sort by date, most recent first
    carouselStories.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    res.status(200).json({
      stories: carouselStories,
      count: carouselStories.length
    });
  } catch (err) {
    console.error("Carousel error:", err);
    res.status(500).json({ error: "Failed to fetch carousel stories" });
  }
};