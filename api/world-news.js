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
      { name: "BBC", url: "http://feeds.bbci.co.uk/news/world/rss.xml" }  // Try BBC instead
    ];

    const allArticles = [];
    const feedStatus = {};

    for (const feed of feeds) {
      try {
        const parsed = await parser.parseURL(feed.url);
        feedStatus[feed.name] = `Success: ${parsed.items.length} items`;
        
        parsed.items.slice(0, 10).forEach(item => {
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

    // Sort by date (most recent first), but keep France24 with images at the top
    allArticles.sort((a, b) => {
      // Prioritize France24 articles with images for featured story
      if (a.source === "France24" && a.image && (!b.image || b.source !== "France24")) return -1;
      if (b.source === "France24" && b.image && (!a.image || a.source !== "France24")) return 1;
      
      // Then sort by date
      const dateA = new Date(a.pubDate);
      const dateB = new Date(b.pubDate);
      return dateB - dateA; // Most recent first
    });

    res.status(200).json({ 
      articles: allArticles, 
      articleCount: allArticles.length,
      feedStatus
    });
  } catch (err) {
    console.error("Full error:", err);
    res.status(500).json({ error: "Failed to fetch RSS feeds", details: err.message });
  }
};