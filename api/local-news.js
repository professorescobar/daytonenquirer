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
      { name: "WDTN", url: "https://www.wdtn.com/feed/" }
    ];

    const articlesBySource = {};
    const feedStatus = {};

    // Fetch RSS feeds
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

    // Get custom articles for this section
    const customArticles = getCustomArticles('local');
    
    // Sort custom articles by date
    customArticles.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Featured article = most recent custom article with image
    const featuredArticle = customArticles.find(a => a.image);

    // All RSS articles (no images used for featured)
    const rssArticles = Object.values(articlesBySource).flat();

    // Remaining custom articles (exclude featured)
    const remainingCustoms = customArticles.filter(a => a !== featuredArticle);

    // Mix remaining customs + RSS, sort by date
    const headlines = [...remainingCustoms, ...rssArticles]
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Final articles array: featured first, then mixed headlines
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