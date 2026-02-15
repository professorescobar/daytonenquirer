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
      // Dayton Area Sports
      { name: "WHIO Sports Dayton", url: "https://www.whio.com/sports/feed/" },
      { name: "UD Flyers Athletics", url: "https://daytonflyers.com/rss.aspx?path=general" },
      { name: "Dayton 24/7 Sports", url: "https://www.dayton247now.com/sports/feed/" },

      // Cincinnati Teams
      { name: "Cincinnati Bengals", url: "https://www.bengals.com/news/rss.xml" },
      { name: "Cincinnati Reds", url: "https://www.mlb.com/reds/feeds/news/rss.xml" },
      { name: "FC Cincinnati", url: "https://www.fccincinnati.com/feed" },

      // Cleveland Teams
      { name: "Cleveland Browns", url: "https://www.clevelandbrowns.com/news/rss.xml" },
      { name: "Cleveland Cavaliers", url: "https://www.nba.com/cavaliers/rss.xml" },
      { name: "Cleveland Guardians", url: "https://www.mlb.com/guardians/feeds/news/rss.xml" },

      // Columbus Teams
      { name: "Columbus Blue Jackets", url: "https://www.nhl.com/bluejackets/news" },
      { name: "Columbus Crew", url: "https://www.columbuscrew.com/feed" },

      // Ohio State
      { name: "Ohio State Buckeyes", url: "https://ohiostatebuckeyes.com/rss.aspx?path=football" },
      { name: "Buckeye Sports Bulletin", url: "https://buckeyesports.com/feed/" },

      // Regional Sports News
      { name: "WKYC Cleveland Sports", url: "https://rssfeeds.wkyc.com/wkyc/sports" },
      { name: "NBC4 Columbus Sports", url: "https://www.nbc4i.com/sports/feed/" },
      { name: "Columbus Dispatch Sports", url: "https://www.dispatch.com/sports/feed/" }
    ];

    const allArticles = [];
    const feedStatus = {};

    for (const feed of feeds) {
      try {
        const parsed = await parser.parseURL(feed.url);
        feedStatus[feed.name] = `Success: ${parsed.items.length} items`;
        
        const articles = parsed.items.slice(0, 10).map(item => {
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
        
        allArticles.push(...articles);
      } catch (feedError) {
        feedStatus[feed.name] = `Failed: ${feedError.message}`;
        console.error(`Failed to fetch ${feed.name}:`, feedError.message);
      }
    }

    // Mix in custom articles
    const customArticles = getCustomArticles('sports');
    allArticles.push(...customArticles);

    // Sort all by date
    allArticles.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    // Featured: most recent with image
    const featuredArticle = allArticles.find(article => article.image);

    // Headlines: everything else
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