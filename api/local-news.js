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
    // Manual articles - you can edit these directly in this file
    const manualArticles = [
      // {
      //   title: "Your custom headline here",
      //   url: "https://example.com/article",
      //   description: "Brief description of the article",
      //   source: "Dayton Enquirer Staff",
      //   image: "https://example.com/image.jpg", // optional
      //   pubDate: new Date().toISOString()
      // }
    ];

    const feeds = [
      { name: "WHIO", url: "https://www.whio.com/arc/outboundfeeds/rss/" },
      { name: "WDTN", url: "https://www.wdtn.com/feed/" }
      // Add more local feeds here as you find them
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

    // Combine RSS feeds with manual articles
    const allArticles = [
      ...manualArticles,
      ...Object.values(articlesBySource).flat()
    ];

    // Sort by date
    allArticles.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Find featured article (first one with image)
    const featuredArticle = allArticles.find(article => article.image);
    
    // Remove featured from headlines
    const headlines = allArticles.filter(article => article !== featuredArticle);

    const articles = featuredArticle ? [featuredArticle, ...headlines] : allArticles;

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