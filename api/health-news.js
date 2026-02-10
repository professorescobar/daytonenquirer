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

module.exports = async (req, res) => {
  try {
    const feeds = [
      { name: "NPR Health", url: "https://feeds.npr.org/1128/rss.xml" },
      { name: "WebMD", url: "https://rssfeeds.webmd.com/rss/rss.aspx?RSSSource=RSS_PUBLIC" },
      { name: "Medical News Today", url: "https://www.medicalnewstoday.com/rss/all" },
      { name: "ScienceDaily Health", url: "https://www.sciencedaily.com/rss/health_medicine.xml" },
      { name: "Harvard Health", url: "https://www.health.harvard.edu/blog/feed" },
      { name: "CDC", url: "https://tools.cdc.gov/api/v2/resources/media/404952.rss" },
      { name: "NIH News", url: "https://www.nih.gov/rss/allevents.xml" },
      { name: "Healthline", url: "https://www.healthline.com/rss/health-news" }
    ];

    const allArticles = [];
    const feedStatus = {};

    for (const feed of feeds) {
      try {
        const parsed = await parser.parseURL(feed.url);
        feedStatus[feed.name] = `Success: ${parsed.items.length} items`;
        
        parsed.items.slice(0, 10).forEach(item => {
          let imageUrl = '';
          if (item.enclosure && item.enclosure.url) imageUrl = item.enclosure.url;
          else if (item.media && item.media.$) imageUrl = item.media.$.url;
          else if (item.thumbnail && item.thumbnail.$) imageUrl = item.thumbnail.$.url;
          else if (item['media:content'] && item['media:content'].$) imageUrl = item['media:content'].$.url;

          allArticles.push({
            title: item.title,
            url: item.link,
            description: item.contentSnippet || item.description || "",
            source: feed.name,
            image: imageUrl,
            pubDate: item.pubDate || item.isoDate || ""
          });
        });
      } catch (feedError) {
        feedStatus[feed.name] = `Failed: ${feedError.message}`;
        console.error(`Failed to fetch ${feed.name}:`, feedError.message);
      }
    }

    // Sort all articles by date (most recent first)
    allArticles.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Featured: most recent article WITH an image
    const featuredArticle = allArticles.find(article => article.image);

    // Headlines: everything else, keeping date order
    const headlines = allArticles.filter(article => article !== featuredArticle);

    const articles = featuredArticle ? [featuredArticle, ...headlines] : headlines;

    res.status(200).json({ articles, articleCount: articles.length, feedStatus });
  } catch (err) {
    console.error("Full error:", err);
    res.status(500).json({ error: "Failed to fetch RSS feeds", details: err.message });
  }
};