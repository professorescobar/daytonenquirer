import Parser from "rss-parser";

const parser = new Parser({
  timeout: 10000,
});

const FEEDS = [
  {
    source: "Reuters",
    url: "https://feeds.reuters.com/Reuters/worldNews",
  },
  {
    source: "Associated Press",
    url: "https://apnews.com/rss/world",
  },
  {
    source: "AFP",
    url: "https://www.afp.com/rss/en",
  },
  {
    source: "NHK World",
    url: "https://www3.nhk.or.jp/rss/news/cat0.xml",
  },
];

export default async function handler(req, res) {
  try {
    const articles = [];

    for (const feed of FEEDS) {
      const data = await parser.parseURL(feed.url);

      data.items.slice(0, 5).forEach(item => {
        articles.push({
          title: item.title,
          link: item.link,
          description:
            item.contentSnippet || item.content || "",
          image:
            item.enclosure?.url ||
            item.media?.content?.url ||
            null,
          source: feed.source,
          pubDate: item.pubDate ? new Date(item.pubDate) : null,
        });
      });
    }

    // Sort newest first
    articles.sort((a, b) => {
      if (!a.pubDate || !b.pubDate) return 0;
      return b.pubDate - a.pubDate;
    });

    res.setHeader(
      "Cache-Control",
      "s-maxage=1800, stale-while-revalidate"
    );

    res.status(200).json(articles);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch world news" });
  }
}
