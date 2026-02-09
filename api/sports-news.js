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
      // National sports sources
      { name: "ESPN", url: "https://www.espn.com/espn/rss/news" },
      { name: "CBS Sports", url: "https://www.cbssports.com/rss/headlines" },
      { name: "Sports Illustrated", url: "https://www.si.com/rss/si_topstories.rss" },
      { name: "The Athletic", url: "https://theathletic.com/feed/" },
      { name: "Yahoo Sports", url: "https://sports.yahoo.com/rss/" },
      { name: "Sporting News", url: "https://www.sportingnews.com/us/rss" }
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

    const workingSources = Object.keys(articlesBySource).filter(
      source => articlesBySource[source].length > 0
    );

    const articlesWithImages = workingSources
      .flatMap(source => articlesBySource[source] || [])
      .filter(article => article.image)
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
    
    const featuredArticle = articlesWithImages[0];

    const sortedBySource = workingSources.map(source => {
      const articles = (articlesBySource[source] || [])
        .filter(article => article !== featuredArticle)
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
      return articles;
    });

    const headlines = [];
    let maxLength = Math.max(...sortedBySource.map(arr => arr.length));
    
    for (let i = 0; i < maxLength; i++) {
      sortedBySource.forEach(sourceArticles => {
        if (sourceArticles[i]) {
          headlines.push(sourceArticles[i]);
        }
      });
    }

    const articles = featuredArticle ? [featuredArticle, ...headlines] : headlines;

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