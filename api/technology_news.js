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
      // Technology
      { name: "Wired", url: "https://www.wired.com/feed/rss" },
      { name: "Ars Technica", url: "https://feeds.arstechnica.com/arstechnica/index" },
      { name: "TechCrunch", url: "https://techcrunch.com/feed/" },
      { name: "The Verge", url: "https://www.theverge.com/rss/index.xml" },
      { name: "Engadget", url: "https://www.engadget.com/rss.xml" },
      // Science
      { name: "ScienceDaily", url: "https://www.sciencedaily.com/rss/top/science.xml" },
      { name: "NASA", url: "https://www.nasa.gov/rss/dyn/breaking_news.rss" },
      { name: "Scientific American", url: "https://www.scientificamerican.com/platform/morgue/rss/sciamcom-rss2.0.xml" },
      // Engineering
      { name: "MIT News", url: "https://news.mit.edu/rss/feed" },
      { name: "IEEE Spectrum", url: "https://spectrum.ieee.org/feeds/feed.rss" }
    ];

    const articlesBySource = {};
    const feedStatus = {};

    for (const feed of feeds) {
      try {
        const parsed = await parser.parseURL(feed.url);
        feedStatus[feed.name] = `Success: ${parsed.items.length} items`;
        
        articlesBySource[feed.name] = parsed.items.slice(0, 10).map(item => {
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
      } catch (feedError) {
        feedStatus[feed.name] = `Failed: ${feedError.message}`;
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
      return (articlesBySource[source] || [])
        .filter(article => article !== featuredArticle)
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
    });

    const headlines = [];
    let maxLength = Math.max(...sortedBySource.map(arr => arr.length));
    for (let i = 0; i < maxLength; i++) {
      sortedBySource.forEach(sourceArticles => {
        if (sourceArticles[i]) headlines.push(sourceArticles[i]);
      });
    }

    const articles = featuredArticle ? [featuredArticle, ...headlines] : headlines;

    res.status(200).json({ articles, articleCount: articles.length, feedStatus });
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch RSS feeds", details: err.message });
  }
};