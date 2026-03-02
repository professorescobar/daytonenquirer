const Parser = require('rss-parser');
const { requireAdmin } = require('./_admin-auth');
const { cleanText, normalizeSection } = require('./_draft-utils');

const parser = new Parser({
  timeout: 15000,
  customFields: {
    item: ['source']
  }
});
const MAX_HEADLINE_AGE_DAYS = 3;
const MAX_HEADLINE_AGE_MS = MAX_HEADLINE_AGE_DAYS * 24 * 60 * 60 * 1000;

const FEEDS_BY_SECTION = {
  local: [
    'https://www.wdtn.com/feed/',
    'https://news.google.com/rss/search?q=Dayton+Ohio+local+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Ohio+breaking+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Ohio+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Ohio+breaking+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Southwest+Ohio+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:wdtn.com+Dayton+news+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Kettering+Ohio+news+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Beavercreek+Ohio+news+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Centerville+Ohio+news+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Xenia+Ohio+news+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Springfield+Ohio+news+when:4d&hl=en-US&gl=US&ceid=US:en'
  ],
  national: [
    'https://news.google.com/rss/search?q=site:npr.org+national+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:pbs.org+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:abcnews.go.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:cbsnews.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:nbcnews.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:usatoday.com+nation+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:axios.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:thehill.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:dailywire.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:washingtonexaminer.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:newsnationnow.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:realclearpolitics.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:news.yahoo.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:washingtontimes.com+u.s.+news+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  world: [
    'https://www.france24.com/en/rss',
    'https://rss.dw.com/rdf/rss-en-world',
    'https://news.google.com/rss/search?q=site:afp.com+world+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:bbc.com+world+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:aljazeera.com+world+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:theguardian.com+world+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:reuters.com+world+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:apnews.com+world+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:nhk.or.jp+world+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:straitstimes.com+world+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:lemonde.fr+international+news+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  business: [
    'https://finance.yahoo.com/news/rssindex',
    'https://news.google.com/rss/search?q=site:finance.yahoo.com+markets+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:cnbc.com+markets+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:bloomberg.com+markets+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:wsj.com+markets+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:ft.com+markets+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:marketwatch.com+markets+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:fool.com+stock+market+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:barrons.com+market+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=federal+reserve+news+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  sports: [
    'https://daytonflyers.com/rss.aspx',
    'https://wsuraiders.com/rss.aspx',
    'https://miamiredhawks.com/rss.aspx',
    'https://ohiostatebuckeyes.com/feed/',
    'https://news.google.com/rss/search?q=Dayton+Ohio+sports+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Ohio+college+sports+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Flyers+athletics+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Wright+State+Raiders+athletics+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Miami+RedHawks+athletics+Ohio+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Ohio+State+Buckeyes+athletics+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Ohio+high+school+sports+when:4d&hl=en-US&gl=US&ceid=US:en'
  ],
  health: [
    'https://news.google.com/rss/search?q=health+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=breaking+health+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=public+health+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=medical+research+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=hospital+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+health+news+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=cdc+health+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=fda+health+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=mental+health+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=infectious+disease+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=nutrition+research+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=healthcare+policy+news+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  entertainment: [
    'https://news.google.com/rss/search?q=site:ign.com+video+game+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:gamespot.com+video+game+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:polygon.com+video+games+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:kotaku.com+video+games+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:variety.com+film+tv+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:hollywoodreporter.com+film+tv+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:deadline.com+tv+movies+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:billboard.com+music+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:rollingstone.com+music+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:news.yahoo.com+pop+culture+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:vulture.com+pop+culture+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:artnews.com+art+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:hyperallergic.com+art+news+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  technology: [
    'https://news.google.com/rss/search?q=engineering+news+when:2d+-health+-hospital+-stock+-earnings&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=science+breakthrough+news+when:2d+-health+-hospital+-stock+-earnings&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=physics+research+news+when:3d+-health+-hospital+-biotech&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=materials+science+news+when:3d+-health+-hospital+-stock&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=robotics+engineering+news+when:2d+-health+-hospital+-stock&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=semiconductor+engineering+news+when:2d+-earnings+-stock&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=space+engineering+news+when:2d+-health+-hospital+-stock&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=energy+technology+engineering+news+when:3d+-health+-hospital+-stock&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:sciencedaily.com+engineering+news+when:3d+-health&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:newscientist.com+technology+science+when:3d+-health+-stock&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:ieeexplore.ieee.org+engineering+news+when:7d+-health&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:nasa.gov+technology+news+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:arstechnica.com+science+news+when:3d+-health+-stock&hl=en-US&gl=US&ceid=US:en'
  ]
};

function normalizeUrl(rawUrl) {
  const raw = String(rawUrl || '').trim();
  if (!raw) return '';
  try {
    const url = new URL(raw);
    url.hash = '';
    url.searchParams.delete('utm_source');
    url.searchParams.delete('utm_medium');
    url.searchParams.delete('utm_campaign');
    return url.toString();
  } catch (_) {
    return raw;
  }
}

function getPublishedTimestamp(rawDate) {
  const value = String(rawDate || '').trim();
  if (!value) return null;
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return null;
  return timestamp;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const section = normalizeSection(req.query.section || 'local');
    const feeds = FEEDS_BY_SECTION[section] || [];
    const page = Math.max(1, parseInt(String(req.query.page || '1'), 10) || 1);
    const perPage = Math.min(36, Math.max(1, parseInt(String(req.query.perPage || '18'), 10) || 18));

    const nowMs = Date.now();
    const minAllowedMs = nowMs - MAX_HEADLINE_AGE_MS;
    const seenUrls = new Set();
    const items = [];

    await Promise.all(feeds.map(async (feedUrl) => {
      try {
        const feed = await parser.parseURL(feedUrl);
        for (const item of feed.items || []) {
          const title = cleanText(item.title);
          const link = normalizeUrl(item.link);
          if (!title || !link || seenUrls.has(link)) continue;
          seenUrls.add(link);

          const publishedAtRaw = item.isoDate || item.pubDate || null;
          const publishedAtMs = getPublishedTimestamp(publishedAtRaw);
          if (!publishedAtMs || publishedAtMs < minAllowedMs) continue;

          const source = cleanText(
            item?.source?.title || item?.creator || feed?.title || 'Unknown source'
          );

          items.push({
            title,
            link,
            source,
            publishedAt: new Date(publishedAtMs).toISOString(),
            publishedAtMs
          });
        }
      } catch (error) {
        // Ignore individual feed failures so one broken feed doesn't fail the page.
      }
    }));

    items.sort((a, b) => Number(b.publishedAtMs || 0) - Number(a.publishedAtMs || 0));

    const totalItems = items.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / perPage));
    const safePage = Math.min(page, totalPages);
    const start = (safePage - 1) * perPage;
    const pagedItems = items.slice(start, start + perPage).map((item) => ({
      title: item.title,
      link: item.link,
      source: item.source,
      publishedAt: item.publishedAt
    }));

    return res.status(200).json({
      ok: true,
      section,
      page: safePage,
      perPage,
      totalItems,
      totalPages,
      items: pagedItems
    });
  } catch (error) {
    console.error('Admin leads feed error:', error);
    return res.status(500).json({ error: 'Failed to load RSS headlines' });
  }
};
