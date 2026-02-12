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
      // Feeds with images (for featured article)
      { name: "France24", url: "https://www.france24.com/en/rss" },
      { name: "BBC", url: "http://feeds.bbci.co.uk/news/world/rss.xml" },
      { name: "RTE", url: "https://www.rte.ie/news/rss/news-headlines.xml" },
      { name: "The Guardian", url: "https://www.theguardian.com/world/rss" },
      
      // Additional feeds (for headline diversity)
      { name: "Deutsche Welle", url: "https://rss.dw.com/rdf/rss-en-world" },
      { name: "Al Jazeera", url: "https://www.aljazeera.com/xml/rss/all.xml" },
      { name: "NPR", url: "https://feeds.npr.org/1004/rss.xml" },
      { name: "ABC Australia", url: "https://www.abc.net.au/news/feed/51120/rss.xml" },
      { name: "Euronews", url: "https://www.euronews.com/rss" }
    ];

    const articlesBySource = {};
    const feedStatus = {};

    // Fetch all feeds
    for (const feed of feeds) {
      try {
        const parsed = await parser.parseURL(feed.url);
        feedStatus[feed.name] = `Success: ${parsed.items.length} items`;
        
        articlesBySource[feed.name] = parsed.items.slice(0, 10).map(item => {
          // Try multiple ways to get the image
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

    // Sources with images (for featured article)
    const imageSources = ['France24', 'BBC', 'RTE', 'The Guardian'];
    
    // FEATURED ARTICLE: Most recent article WITH image from image sources
    const articlesWithImages = imageSources
      .flatMap(source => articlesBySource[source] || [])
      .filter(article => article.image)
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Get all working sources
    const workingSources = Object.keys(articlesBySource).filter(
      source => articlesBySource[source].length > 0
    );

    // OTHER HEADLINES: Interleave all working sources for diversity
    const sortedBySource = workingSources.map(source => {
      const articles = (articlesBySource[source] || [])
        .filter(article => article !== featuredArticle)
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
      return articles;
    });

    // Interleave sources for diversity
    let maxLength = Math.max(...sortedBySource.map(arr => arr.length));
    
    for (let i = 0; i < maxLength; i++) {
      sortedBySource.forEach(sourceArticles => {
        if (sourceArticles[i]) {
          headlines.push(sourceArticles[i]);
        }
      });
    }

   // Mix in custom articles for this section
    const customArticles = getCustomArticles('world'); // change for each API
    allArticles.push(...customArticles);

    // Sort all articles by date (most recent first)
    allArticles.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Featured: most recent article WITH an image
    const featuredArticle = allArticles.find(article => article.image);

    // Headlines: everything else in date order
    const headlines = allArticles.filter(article => article !== featuredArticle);

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