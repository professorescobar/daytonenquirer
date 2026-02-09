const Parser = require("rss-parser");
const parser = new Parser();

module.exports = async (req, res) => {
  try {
    const feeds = [
      { name: "Reuters", url: "https://feeds.reuters.com/reuters/worldNews" },
      { name: "AP", url: "https://apnews.com/rss" },
      { name: "AFP", url: "https://www.afp.com/rss" },
      { name: "NHK", url: "https://www3.nhk.or.jp/rss/news/cat0.xml" }
    ];

    const articles = [];

    for (const feed of feeds) {
      try {
        const parsed = await parser.parseURL(feed.url);
        parsed.items.slice(0, 5).forEach(item => {
          articles.push({
            title: item.title,
            url: item.link,
            description: item.contentSnippet || "",
            source: feed.name,
            image: item.enclosure?.url || ""
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
