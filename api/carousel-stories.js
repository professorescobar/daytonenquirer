const Parser = require("rss-parser");
const parser = new Parser({
  headers: {
    'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)'
  },
  timeout: 10000,
  customFields: {
    item: [
      ['media:content', 'media'],
      ['media:thumbnail', 'thumbnail'],
      ['enclosure', 'enclosure']
    ]
  }
});

async function fetchFeed(feed) {
  try {
    const parsed = await parser.parseURL(feed.url);
    return parsed.items.slice(0, 5).map(item => {
      let imageUrl = '';
      
      if (item.enclosure && item.enclosure.url) {
        imageUrl = item.enclosure.url;
      } else if (item.media && item.media.$) {
        imageUrl = item.media.$.url;
      } else if (item.thumbnail && item.thumbnail.$) {
        imageUrl = item.thumbnail.$.url;
      } else if (item['media:content'] && item['media:content'].$) {
        imageUrl = item['media:content'].$.url;
      }

      return {
        title: item.title,
        url: item.link,
        description: item.contentSnippet || item.description || "",
        source: feed.name,
        category: feed.category,
        image: imageUrl,
        pubDate: item.pubDate || item.isoDate || ""
      };
    }).filter(article => article.image); // Only articles with images
  } catch (err) {
    console.error(`Failed to fetch ${feed.name}:`, err.message);
    return [];
  }
}

module.exports = async (req, res) => {
  try {
   const feeds = [
  // Local
  { name: "WHIO", url: "https://www.whio.com/arc/outboundfeeds/rss/", category: "Local" },
  
  // National
  { name: "NPR", url: "https://feeds.npr.org/1001/rss.xml", category: "National" },
  { name: "CBS News", url: "https://www.cbsnews.com/latest/rss/main", category: "National" },
  { name: "PBS NewsHour", url: "https://www.pbs.org/newshour/feeds/rss/headlines", category: "National" },
  
  // Business
  { name: "CNBC", url: "https://www.cnbc.com/id/100003114/device/rss/rss.html", category: "Business" },
  { name: "MarketWatch", url: "https://www.marketwatch.com/rss/topstories", category: "Business" },
  
  // Sports
  { name: "ESPN", url: "https://www.espn.com/espn/rss/news", category: "Sports" },
  { name: "CBS Sports", url: "https://www.cbssports.com/rss/headlines", category: "Sports" },
  
  // World (only France24 and RTE for image quality)
  { name: "France24", url: "https://www.france24.com/en/rss", category: "World" },
  { name: "RTE", url: "https://www.rte.ie/news/rss/news-headlines.xml", category: "World" }
];

    // Fetch all feeds in parallel
    const allArticlesPromises = feeds.map(feed => fetchFeed(feed));
    const allArticlesArrays = await Promise.all(allArticlesPromises);
    const allArticles = allArticlesArrays.flat();

    // Group by category
    const byCategory = {
      Local: allArticles.filter(a => a.category === "Local"),
      National: allArticles.filter(a => a.category === "National"),
      Business: allArticles.filter(a => a.category === "Business"),
      Sports: allArticles.filter(a => a.category === "Sports"),
      World: allArticles.filter(a => a.category === "World")
    };

    // Pick one random article from each category (if available)
    const carouselStories = [];
    
    for (const category of ["Local", "National", "Business", "Sports", "World"]) {
      const articles = byCategory[category];
      if (articles.length > 0) {
        const randomIndex = Math.floor(Math.random() * articles.length);
        carouselStories.push(articles[randomIndex]);
      }
    }

    // If we have less than 6, fill with random articles that have images
    while (carouselStories.length < 6 && allArticles.length > 0) {
      const randomIndex = Math.floor(Math.random() * allArticles.length);
      const article = allArticles[randomIndex];
      if (!carouselStories.includes(article)) {
        carouselStories.push(article);
      }
    }

    res.status(200).json({ 
      stories: carouselStories.slice(0, 6),
      count: carouselStories.length
    });
  } catch (err) {
    console.error("Carousel error:", err);
    res.status(500).json({ error: "Failed to fetch carousel stories" });
  }
};