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
      { name: "Deutsche Welle", url: "https://rss.dw.com/rdf/rss-en-world" },
      { name: "Al Jazeera", url: "https://www.aljazeera.com/xml/rss/all.xml" },
      { name: "BBC", url: "http://feeds.bbci.co.uk/news/world/rss.xml" }
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

    // FEATURED ARTICLE: Most recent article WITH image from BBC or France24
    const featuredSources = ['BBC', 'France24'];
    const articlesWithImages = featuredSources
      .flatMap(source => articlesBySource[source] || [])
      .filter(article => article.image)
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
    
    const featuredArticle = articlesWithImages[0];

    // OTHER HEADLINES: Interleave all sources for diversity, sorted by date within each source
    const headlineSources = ['BBC', 'France24', 'Al Jazeera', 'Deutsche Welle'];
    
    // Sort each source's articles by date
    const sortedBySource = headlineSources.map(source => {
      const articles = (articlesBySource[source] || [])
        .filter(article => article !== featuredArticle) // Exclude the featured article
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
      return articles;
    });

    // Interleave sources for diversity
    const headlines = [];
    let maxLength = Math.max(...sortedBySource.map(arr => arr.length));
    
    for (let i = 0; i < maxLength; i++) {
      sortedBySource.forEach(sourceArticles => {
        if (sourceArticles[i]) {
          headlines.push(sourceArticles[i]);
        }
      });
    }

    // Combine: featured first, then headlines
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