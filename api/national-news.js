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
      // National news sources with images
      { name: "NPR", url: "https://feeds.npr.org/1001/rss.xml" }, // NPR News
      { name: "PBS NewsHour", url: "https://www.pbs.org/newshour/feeds/rss/headlines" },
      { name: "Politico", url: "https://www.politico.com/rss/politics08.xml" },
      { name: "The Hill", url: "https://thehill.com/feed/" },
      { name: "USA Today", url: "http://rssfeeds.usatoday.com/usatoday-NewsTopStories" },
      { name: "CBS News", url: "https://www.cbsnews.com/latest/rss/main" },
      { name: "NBC News", url: "https://feeds.nbcnews.com/nbcnews/public/news" },
      { name: "ABC News", url: "https://abcnews.go.com/abcnews/topstories" }
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