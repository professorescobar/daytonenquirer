const { neon } = require('@neondatabase/serverless');
const Parser = require('rss-parser');
const { requireAdmin } = require('./_admin-auth');
const { ensureTopicEngineTables, runTopicEngineWorkflow, normalizeUrl } = require('./_topic-engine-workflow');
const { getPersonaSection } = require('../lib/personas');

const parser = new Parser({
  timeout: 12000,
  headers: {
    'User-Agent': 'DaytonEnquirerTopicEngineBot/1.0 (+https://thedaytonenquirer.com)'
  }
});

const DEFAULT_FEEDS_BY_SECTION = {
  local: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  national: [
    'https://news.google.com/rss/search?q=national+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  world: [
    'https://news.google.com/rss/search?q=world+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  business: [
    'https://news.google.com/rss/search?q=stock+market+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  sports: [
    'https://news.google.com/rss/search?q=dayton+ohio+sports+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  health: [
    'https://news.google.com/rss/search?q=health+news+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  entertainment: [
    'https://news.google.com/rss/search?q=dayton+entertainment+events+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  technology: [
    'https://news.google.com/rss/search?q=technology+innovation+when:2d&hl=en-US&gl=US&ceid=US:en'
  ]
};

function parsePositiveInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

async function getScheduledPersonas(sql, maxEngines) {
  const rows = await sql`
    SELECT id, COALESCE(NULLIF(trim(activation_mode), ''), 'both') as "activationMode"
    FROM personas
    WHERE COALESCE(NULLIF(trim(activation_mode), ''), 'both') IN ('scheduled', 'both')
    ORDER BY id ASC
    LIMIT ${maxEngines}
  `;
  return rows;
}

async function getFeedsForPersona(sql, personaId, section) {
  const configured = await sql`
    SELECT feed_url as "feedUrl", COALESCE(source_name, '') as "sourceName"
    FROM topic_engine_feeds
    WHERE persona_id = ${personaId}
      AND enabled = true
    ORDER BY priority ASC, id ASC
    LIMIT 50
  `;
  if (configured.length) {
    return configured.map((row) => ({
      feedUrl: normalizeUrl(row.feedUrl),
      sourceName: row.sourceName || ''
    })).filter((item) => item.feedUrl);
  }
  const fallbacks = DEFAULT_FEEDS_BY_SECTION[section] || [];
  return fallbacks.map((feedUrl) => ({ feedUrl: normalizeUrl(feedUrl), sourceName: section }));
}

function itemToSignal(item, sourceName, feedUrl) {
  return {
    title: String(item?.title || '').trim(),
    url: normalizeUrl(item?.link || item?.guid || ''),
    snippet: String(item?.contentSnippet || item?.summary || item?.content || '').trim().slice(0, 2000),
    sourceName: sourceName || '',
    sourceUrl: feedUrl || '',
    publishedAt: item?.isoDate || item?.pubDate || null,
    metadata: {
      feedTitle: item?.creator || '',
      categories: Array.isArray(item?.categories) ? item.categories.slice(0, 12) : []
    }
  };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (!['POST', 'GET'].includes(req.method)) {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const input = req.method === 'POST' ? (req.body || {}) : (req.query || {});
  const maxEngines = parsePositiveInt(input.maxEngines, 50, 1, 500);
  const maxFeedsPerEngine = parsePositiveInt(input.maxFeedsPerEngine, 5, 1, 50);
  const maxItemsPerFeed = parsePositiveInt(input.maxItemsPerFeed, 5, 1, 50);

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureTopicEngineTables(sql);

    const personas = await getScheduledPersonas(sql, maxEngines);
    const summary = {
      enginesScanned: personas.length,
      feedsScanned: 0,
      signalsSeen: 0,
      inserted: 0,
      deduped: 0,
      skipped: 0,
      errors: 0
    };

    const engineResults = [];

    for (const persona of personas) {
      const personaId = String(persona.id || '').trim();
      const section = getPersonaSection(personaId) || 'local';
      const feeds = (await getFeedsForPersona(sql, personaId, section)).slice(0, maxFeedsPerEngine);
      let engineSeen = 0;
      let engineInserted = 0;
      let engineDeduped = 0;
      let engineSkipped = 0;
      let engineErrors = 0;

      for (const feed of feeds) {
        summary.feedsScanned += 1;
        let parsed;
        try {
          parsed = await parser.parseURL(feed.feedUrl);
        } catch (error) {
          engineErrors += 1;
          summary.errors += 1;
          continue;
        }

        const items = Array.isArray(parsed?.items) ? parsed.items.slice(0, maxItemsPerFeed) : [];
        for (const item of items) {
          const signal = itemToSignal(item, feed.sourceName || parsed?.title || section, feed.feedUrl);
          if (!signal.title) continue;
          summary.signalsSeen += 1;
          engineSeen += 1;
          const result = await runTopicEngineWorkflow(sql, {
            personaId,
            triggerMode: 'scheduled',
            signal
          });
          if (!result.ok) {
            engineErrors += 1;
            summary.errors += 1;
          } else if (result.skipped) {
            engineSkipped += 1;
            summary.skipped += 1;
          } else if (result.deduped) {
            engineDeduped += 1;
            summary.deduped += 1;
          } else {
            engineInserted += 1;
            summary.inserted += 1;
          }
        }
      }

      engineResults.push({
        personaId,
        section,
        feedsScanned: feeds.length,
        signalsSeen: engineSeen,
        inserted: engineInserted,
        deduped: engineDeduped,
        skipped: engineSkipped,
        errors: engineErrors
      });
    }

    return res.status(200).json({
      ok: true,
      triggerMode: 'scheduled',
      params: { maxEngines, maxFeedsPerEngine, maxItemsPerFeed },
      summary,
      engines: engineResults
    });
  } catch (error) {
    console.error('Topic engine scheduled trigger error:', error);
    return res.status(500).json({ error: 'Failed to run scheduled topic discovery' });
  }
};

