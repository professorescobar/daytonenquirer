const Parser = require("rss-parser");
const parser = new Parser({
  headers: {
    'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)'
  },
  timeout: 8000,
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
      if (item.enclosure && item.enclosure.url) imageUrl = item.enclosure.url;
      else if (item.media && item.media.$) imageUrl = item.media.$.url;
      else if (item.thumbnail && item.thumbnail.$) imageUrl = item.thumbnail.$.url;
      else if (item['media:content'] && item['media:content'].$) imageUrl = item['media:content'].$.url;

      return {
        title: item.title,
        url: item.link,
        description: item.contentSnippet || item.description || "",
        source: feed.name,
        category: feed.category,
        image: imageUrl,
        pubDate: item.pubDate || item.isoDate || ""
      };
    }).filter(article => article.image);
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
      { name: "WDTN", url: "https://www.wdtn.com/feed/", category: "Local" },

      // National
      { name: "NPR", url: "https://feeds.npr.org/1001/rss.xml", category: "National" },
      { name: "CBS News", url: "https://www.cbsnews.com/latest/rss/main", category: "National" },
      { name: "PBS NewsHour", url: "https://www.pbs.org/newshour/feeds/rss/headlines", category: "National" },

      // World
      { name: "France24", url: "https://www.france24.com/en/rss", category: "World" },
      { name: "RTE", url: "https://www.rte.ie/news/rss/news-headlines.xml", category: "World" },

      // Business
      { name: "CNBC", url: "https://www.cnbc.com/id/100003114/device/rss/rss.html", category: "Business" },
      { name: "MarketWatch", url: "https://www.marketwatch.com/rss/topstories", category: "Business" },

      // Sports
      { name: "ESPN", url: "https://www.espn.com/espn/rss/news", category: "Sports" },
      { name: "CBS Sports", url: "https://www.cbssports.com/rss/headlines", category: "Sports" },

      // Health
      { name: "CBS News Health", url: "https://www.cbsnews.com/latest/rss/health", category: "Health" },
      { name: "NBC News Health", url: "https://feeds.nbcnews.com/nbcnews/public/health", category: "Health" },

      // Entertainment
      { name: "Variety", url: "https://variety.com/feed/", category: "Entertainment" },
      { name: "Hollywood Reporter", url: "https://www.hollywoodreporter.com/feed/", category: "Entertainment" },
      { name: "Rolling Stone", url: "https://www.rollingstone.com/feed/", category: "Entertainment" },

      // Technology
      { name: "Wired", url: "https://www.wired.com/feed/rss", category: "Technology" },
      { name: "Ars Technica", url: "https://feeds.arstechnica.com/arstechnica/index", category: "Technology" },
      { name: "TechCrunch", url: "https://techcrunch.com/feed/", category: "Technology" }
    ];

    // Fetch all feeds in parallel
    const results = await Promise.allSettled(
      feeds.map(feed => fetchFeed(feed).then(articles => ({ feed, articles })))
    );

    // Group by category
    const byCategory = {};
    results.forEach(result => {
      if (result.status === 'fulfilled') {
        const { feed, articles } = result.value;
        if (!byCategory[feed.category]) byCategory[feed.category] = [];
        byCategory[feed.category].push(...articles);
      }
    });

    // Pick one random article with image from each category
    const categories = ["Local", "National", "World", "Business", "Sports", "Health", "Entertainment", "Technology"];
    const carouselStories = [];

    for (const category of categories) {
      const articles = byCategory[category] || [];
      if (articles.length > 0) {
        const randomIndex = Math.floor(Math.random() * Math.min(articles.length, 5));
        carouselStories.push(articles[randomIndex]);
      }
    }

    res.status(200).json({
      stories: carouselStories.slice(0, 8),
      count: carouselStories.length
    });
  } catch (err) {
    console.error("Carousel error:", err);
    res.status(500).json({ error: "Failed to fetch carousel stories" });
  }
};