const getCustomArticles = require('./custom-articles');

// Cache variables
let cachedCarousel = null;
let cacheTimestamp = 0;
const CACHE_DURATION = 30 * 60 * 1000; // 30 minutes in milliseconds

module.exports = async (req, res) => {
  try {
    const now = Date.now();
    
    // Check if cache is still valid
    if (cachedCarousel && (now - cacheTimestamp) < CACHE_DURATION) {
      return res.status(200).json(cachedCarousel);
    }

    // Cache expired or doesn't exist, generate fresh data
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

    for (const section of sections) {
      const customArticles = getCustomArticles(section);
      
      // Get second most recent custom article with image
      const withImages = customArticles
        .filter(article => article.image)
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
      
      // Use index [1] for second most recent, fall back to [0] if only one exists
      const carouselArticle = withImages[1] || withImages[0];
      
      if (carouselArticle) {
        carouselStories.push({
          ...carouselArticle,
          category: categoryMap[section]
        });
      }
    }

    // Also get any "all" section articles (second most recent)
    const allArticles = getCustomArticles('all')
      .filter(article => article.image)
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
    
    if (allArticles.length > 0) {
      carouselStories.push(...allArticles.slice(0, 2));
    }

    // Sort by date, most recent first
    carouselStories.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    const response = {
      stories: carouselStories,
      count: carouselStories.length
    };

    // Update cache
    cachedCarousel = response;
    cacheTimestamp = now;

    res.status(200).json(response);
  } catch (err) {
    console.error("Carousel error:", err);
    res.status(500).json({ error: "Failed to fetch carousel stories" });
  }
};