const getCustomArticles = require('./custom-articles');
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
      { name: "France24", url: "https://www.france24.com/en/rss" },
      { name: "BBC", url: "http://feeds.bbci.co.uk/news/world/rss.xml" },
      { name: "RTE", url: "https://www.rte.ie/news/rss/news-headlines.xml" },
      { name: "The Guardian", url: "https://www.theguardian.com/world/rss" },
      { name: "CNN World", url: "http://rss.cnn.com/rss/cnn_world.rss" },
      { name: "Sky News", url: "https://feeds.skynews.com/feeds/rss/world.xml" }
    ];

    const articlesBySource = {};
    const feedStatus = {};

    for (const feed of feeds) {
      try {
        const parsed = await parser.parseURL(feed.url);
        feedStatus[feed.name] = `Success: ${parsed.items.length} items`;
        
        articlesBySource[feed.name] = parsed.items.slice(0, 10).map(item => {
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
            image: imageUrl,
            pubDate: item.pubDate || item.isoDate || ""
          };
        });
      } catch (feedError) {
        feedStatus[feed.name] = `Failed: ${feedError.message}`;
        console.error(`Failed to fetch ${feed.name}:`, feedError.message);
        articlesBySource[feed.name] = [];
      }
    }

    const customArticles = getCustomArticles('world');
    customArticles.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    const featuredArticle = customArticles.find(a => a.image);
    const rssArticles = Object.values(articlesBySource).flat();
    const remainingCustoms = customArticles.filter(a => a !== featuredArticle);

    const headlines = [...remainingCustoms, ...rssArticles]
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    const articles = featuredArticle
      ? [featuredArticle, ...headlines]
      : headlines;

    res.status(200).json({
      articles,
      articleCount: articles.length,
      feedStatus
    });
  } catch (err) {
    console.error("Full error:", err);
    res.status(500).json({ error: "Failed to fetch RSS feeds", details: err.message });
  }
};