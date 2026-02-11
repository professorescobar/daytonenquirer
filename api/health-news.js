const getCustomArticles = require('./custom-articles');
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
    return parsed.items.slice(0, 10).map(item => {
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
        image: imageUrl,
        pubDate: item.pubDate || item.isoDate || ""
      };
    });
  } catch (err) {
    console.error(`Failed to fetch ${feed.name}:`, err.message);
    return [];
  }
}

module.exports = async (req, res) => {
  try {
    const feeds = [
      // These sources are known to include images in RSS
      { name: "CBS News Health", url: "https://www.cbsnews.com/latest/rss/health" },
      { name: "NBC News Health", url: "https://feeds.nbcnews.com/nbcnews/public/health" },
      { name: "ABC News Health", url: "https://abcnews.go.com/abcnews/healthheadlines" },
      { name: "NPR Health", url: "https://feeds.npr.org/1128/rss.xml" },
      { name: "STAT News", url: "https://www.statnews.com/feed/" },
      // These are good for headlines even without images
      { name: "ScienceDaily Health", url: "https://www.sciencedaily.com/rss/health_medicine.xml" },
      { name: "CDC", url: "https://tools.cdc.gov/api/v2/resources/media/404952.rss" },
      { name: "Healthline", url: "https://www.healthline.com/rss/health-news" }
    ];

    // Fetch all feeds in parallel
    const results = await Promise.allSettled(
      feeds.map(feed => fetchFeed(feed).then(articles => ({ feed, articles })))
    );

    const allArticles = [];
    const feedStatus = {};

    results.forEach(result => {
      if (result.status === 'fulfilled') {
        const { feed, articles } = result.value;
        feedStatus[feed.name] = `Success: ${articles.length} items`;
        allArticles.push(...articles);
      }
    });

    // Mix in custom articles for this section
    const customArticles = getCustomArticles('health'); // change section name for each API
    allArticles.push(...customArticles);

    // Sort all articles by date (most recent first)
    allArticles.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Featured: most recent article WITH an image
    const featuredArticle = allArticles.find(article => article.image);

    // Headlines: everything else in date order
    const headlines = allArticles.filter(article => article !== featuredArticle);

    const articles = featuredArticle ? [featuredArticle, ...headlines] : headlines;

    res.status(200).json({ articles, articleCount: articles.length, feedStatus });
  } catch (err) {
    console.error("Full error:", err);
    res.status(500).json({ error: "Failed to fetch RSS feeds", details: err.message });
  }
};