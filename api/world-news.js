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
      { name: "AP News", url: "https://feeds.apnews.com/rss/world" },
      { name: "NHK World", url: "https://www3.nhk.or.jp/nhkworld/en/news/rss.xml" },
      { name: "Deutsche Welle", url: "https://rss.dw.com/rdf/rss-en-world" },
      { name: "France24", url: "https://www.france24.com/en/rss" }
    ];

    const articles = [];

    for (const feed of feeds) {
      try {
        const parsed = await parser.parseURL(feed.url);
        parsed.items.slice(0, 5).forEach(item => {
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

          articles.push({
            title: item.title,
            url: item.link,
            description: item.contentSnippet || item.description || "",
            source: feed.name,
            image: imageUrl
          });
        });
      } catch (feedError) {
        console.error(`Failed to fetch ${feed.name}:`, feedError.message);
      }
    }

    res.status(200).json({ articles, articleCount: articles.length });
  } catch (err) {
    console.error("Full error:", err);
    res.status(500).json({ error: "Failed to fetch RSS feeds", details: err.message });
  }
};
