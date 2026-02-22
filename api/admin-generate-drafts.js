const { neon } = require('@neondatabase/serverless');
const Parser = require('rss-parser');
const { requireAdmin } = require('./_admin-auth');
const { getDailyTokenBudget } = require('./_admin-settings');
const {
  cleanText,
  generateSlug,
  normalizeSection,
  truncate
} = require('./_draft-utils');

const parser = new Parser({
  timeout: 15000,
  customFields: {
    item: ['source']
  }
});

const SECTION_DAILY_TARGETS = {
  local: 5,
  national: 5,
  world: 5,
  business: 5,
  sports: 4,
  health: 1,
  entertainment: 1,
  technology: 1
};

const LOCAL_SCOPE_SECTIONS = new Set([
  'local',
  'business',
  'sports',
  'health',
  'entertainment',
  'technology'
]);

const SECTION_ORDER = [
  'local',
  'national',
  'world',
  'business',
  'sports',
  'health',
  'entertainment',
  'technology'
];

const MIN_ARTICLE_WORDS = 600;
const TARGET_ARTICLE_WORDS = 800;
const DEFAULT_MAX_OUTPUT_TOKENS = 3200;

const ET_TIME_ZONE = 'America/New_York';
const MULTI_TRACK_SLOTS_ET = ['06:05', '09:05', '12:05', '15:05', '18:05', '21:05'];
const SINGLE_TRACK_SLOTS_ET = ['05:05'];

function parseSectionList(raw) {
  if (!raw) return null;
  const list = String(raw)
    .split(',')
    .map((s) => normalizeSection(s))
    .filter(Boolean);
  return list.length ? Array.from(new Set(list)) : null;
}

function resolveActiveSections(includeSections, excludeSections) {
  let active = [...SECTION_ORDER];
  if (includeSections && includeSections.length) {
    const set = new Set(includeSections);
    active = active.filter((s) => set.has(s));
  }
  if (excludeSections && excludeSections.length) {
    const set = new Set(excludeSections);
    active = active.filter((s) => !set.has(s));
  }
  return active;
}

function getNowInEtParts() {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone: ET_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  });
  const parts = dtf.formatToParts(new Date());
  const obj = {};
  for (const p of parts) {
    if (p.type !== 'literal') obj[p.type] = p.value;
  }
  return obj;
}

function getEtDateKey(parts) {
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function getEtTimeKey(parts) {
  return `${parts.hour}:${parts.minute}`;
}

function shouldRunScheduledTrack(track, etTime) {
  if (track === 'single') return SINGLE_TRACK_SLOTS_ET.includes(etTime);
  if (track === 'multi') return MULTI_TRACK_SLOTS_ET.includes(etTime);
  return false;
}

const FEEDS_BY_SECTION = {
  local: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+local+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Montgomery+County+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  national: [
    'https://news.google.com/rss/headlines/section/topic/NATION?hl=en-US&gl=US&ceid=US:en'
  ],
  world: [
    'https://news.google.com/rss/headlines/section/topic/WORLD?hl=en-US&gl=US&ceid=US:en'
  ],
  business: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+business+economy+jobs+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  sports: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+sports+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  health: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+health+hospital+medical+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  entertainment: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+entertainment+arts+music+events+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  technology: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+technology+startup+innovation+when:3d&hl=en-US&gl=US&ceid=US:en'
  ]
};

function normalizeUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';
  try {
    const u = new URL(raw);
    u.hash = '';
    u.searchParams.delete('utm_source');
    u.searchParams.delete('utm_medium');
    u.searchParams.delete('utm_campaign');
    return u.toString();
  } catch (_) {
    return raw;
  }
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (_) {
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return null;
    try {
      return JSON.parse(match[0]);
    } catch (err) {
      return null;
    }
  }
}

function countWords(text) {
  const plain = String(text || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!plain) return 0;
  return plain.split(' ').length;
}

async function callAnthropicForDraft(candidate) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    throw new Error('Missing ANTHROPIC_API_KEY');
  }

  const localityRule = LOCAL_SCOPE_SECTIONS.has(candidate.section)
    ? 'This article MUST be focused on Dayton, Ohio and/or the Miami Valley region. If the source is not locally relevant, reject it by returning JSON with empty title/description/content.'
    : 'This article may cover broader non-local scope.';

  const model = process.env.ANTHROPIC_MODEL || 'claude-3-5-sonnet-latest';
  const maxOutputTokens = Math.min(
    parseInt(process.env.ANTHROPIC_MAX_OUTPUT_TOKENS || String(DEFAULT_MAX_OUTPUT_TOKENS), 10),
    8192
  );
  const prompt = `
You are writing a fully original local-news publication draft for The Dayton Enquirer.

SECTION: ${candidate.section}
HEADLINE: ${candidate.title}
SOURCE URL: ${candidate.url}
SOURCE SNIPPET: ${candidate.snippet || 'N/A'}

Requirements:
1) Return valid JSON only.
2) JSON keys: title, description, content, section.
3) title: brief, attention-grabbing newsroom headline (8-14 words preferred, no clickbait).
4) description: concise 2-4 sentence summary.
5) content: detailed long-form article in plain HTML-friendly text with paragraph breaks using \\n\\n.
   - Minimum ${MIN_ARTICLE_WORDS} words.
   - Target ${TARGET_ARTICLE_WORDS}-${TARGET_ARTICLE_WORDS + 300} words.
6) Writing quality:
   - Open by centering the main current event in the first 1-2 paragraphs.
   - Then transition into meaningful related context readers care about (local impact, timeline, policy, business, public safety, practical implications).
   - Be thorough and specific, but concise. Do not ramble or repeat.
   - Avoid fluff and generic filler language.
6) section must be one of: local, national, world, business, sports, health, entertainment, technology.
7) Do not include fake quotes or unverifiable claims. If details are uncertain, state uncertainty clearly.
8) ${localityRule}

Return only JSON.
`;

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model,
      max_tokens: maxOutputTokens,
      temperature: 0.4,
      messages: [{ role: 'user', content: prompt }]
    })
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Anthropic API error ${response.status}: ${body.slice(0, 200)}`);
  }

  const data = await response.json();
  const text = data?.content?.find((c) => c.type === 'text')?.text || '';
  const usage = data?.usage || {};
  const inputTokens = Number(usage.input_tokens || 0);
  const outputTokens = Number(usage.output_tokens || 0);
  const totalTokens = inputTokens + outputTokens;
  const parsed = safeJsonParse(text);
  if (!parsed) {
    throw new Error('Model did not return valid JSON');
  }

  return {
    title: cleanText(parsed.title) || cleanText(candidate.title),
    description: truncate(cleanText(parsed.description), 800),
    content: cleanText(parsed.content),
    section: normalizeSection(candidate.section),
    model,
    inputTokens,
    outputTokens,
    totalTokens
  };
}

async function buildDraftWithMinWords(candidate) {
  let draft = await callAnthropicForDraft(candidate);
  let words = countWords(draft.content);
  if (words >= MIN_ARTICLE_WORDS) return { draft, words };

  // Retry once with stricter length guidance.
  draft = await callAnthropicForDraft({
    ...candidate,
    title: `${candidate.title} (expand with more verified context and impact details)`
  });
  words = countWords(draft.content);
  return { draft, words };
}

function buildRunTargets(remainingBySection, maxForRun, activeSections) {
  const targets = {};
  for (const section of SECTION_ORDER) targets[section] = 0;
  const activeSet = new Set(activeSections);

  let allocated = 0;
  while (allocated < maxForRun) {
    let bestSection = null;
    let bestRemaining = 0;
    for (const section of SECTION_ORDER) {
      if (!activeSet.has(section)) continue;
      const remaining = (remainingBySection[section] || 0) - targets[section];
      if (remaining > bestRemaining) {
        bestRemaining = remaining;
        bestSection = section;
      }
    }
    if (!bestSection || bestRemaining <= 0) break;
    targets[bestSection] += 1;
    allocated += 1;
  }

  return targets;
}

async function fetchCandidates(runTargets, activeSections) {
  const out = [];
  const seen = new Set();
  const activeSet = new Set(activeSections);

  for (const section of SECTION_ORDER) {
    if (!activeSet.has(section)) continue;
    if (!runTargets[section]) continue;
    const perSection = Math.max(8, runTargets[section] * 4);

    for (const feedUrl of FEEDS_BY_SECTION[section]) {
      try {
        const feed = await parser.parseURL(feedUrl);
        const items = (feed.items || []).slice(0, perSection * 2);
        for (const item of items) {
          const title = cleanText(item.title);
          const url = normalizeUrl(item.link);
          if (!title || !url) continue;
          const key = `${title.toLowerCase()}|${url}`;
          if (seen.has(key)) continue;
          seen.add(key);

          out.push({
            section,
            title,
            url,
            snippet: cleanText(item.contentSnippet || item.content || ''),
            sourcePublishedAt: item.isoDate || item.pubDate || null
          });
        }
      } catch (error) {
        console.error(`Feed fetch failed for ${section}:`, error.message);
      }
    }
  }

  return out;
}

function normalizeTitleForCompare(title) {
  const stop = new Set([
    'the', 'a', 'an', 'and', 'or', 'to', 'of', 'in', 'on', 'for', 'with', 'from',
    'at', 'by', 'as', 'is', 'are', 'be', 'this', 'that', 'after', 'amid'
  ]);
  return String(title || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, ' ')
    .split(/\s+/)
    .filter((t) => t && !stop.has(t) && t.length > 2);
}

function isNearDuplicateTitle(candidateTitle, existingTitles) {
  const cand = normalizeTitleForCompare(candidateTitle);
  if (!cand.length) return false;
  const candSet = new Set(cand);

  for (const title of existingTitles) {
    const other = normalizeTitleForCompare(title);
    if (!other.length) continue;
    const otherSet = new Set(other);
    let overlap = 0;
    for (const token of candSet) {
      if (otherSet.has(token)) overlap += 1;
    }
    const similarity = overlap / Math.max(candSet.size, otherSet.size);
    if (similarity >= 0.7) return true;
  }
  return false;
}

async function alreadyExists(sql, candidate) {
  const slug = generateSlug(candidate.title);
  const rows = await sql`
    SELECT EXISTS(
      SELECT 1 FROM articles
      WHERE slug = ${slug}
         OR lower(title) = lower(${candidate.title})
      UNION ALL
      SELECT 1 FROM article_drafts
      WHERE source_url = ${candidate.url}
         OR lower(title) = lower(${candidate.title})
    ) AS "exists"
  `;
  return !!rows?.[0]?.exists;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const requestedCount = Math.min(parseInt(req.body?.count || req.query.count || '6', 10), 50);
  const dryRun = String(req.body?.dryRun || req.query.dryRun || 'false') === 'true';
  const dailyTokenBudgetOverride = req.body?.dailyTokenBudget || req.query.dailyTokenBudget;
  const scheduleMode = String(req.body?.schedule || req.query.schedule || '').toLowerCase();
  const track = String(req.body?.track || req.query.track || '').toLowerCase();
  const includeSections = parseSectionList(req.body?.includeSections || req.query.includeSections);
  const excludeSections = parseSectionList(req.body?.excludeSections || req.query.excludeSections);
  const activeSections = resolveActiveSections(includeSections, excludeSections);

  if (!activeSections.length) {
    return res.status(400).json({ error: 'No active sections after include/exclude filters' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const dailyTokenBudget = dailyTokenBudgetOverride
      ? Math.max(1, Math.min(parseInt(String(dailyTokenBudgetOverride), 10), 1000000))
      : await getDailyTokenBudget(sql);
    const etNowParts = getNowInEtParts();
    const etTime = getEtTimeKey(etNowParts);
    const etDate = getEtDateKey(etNowParts);

    if (scheduleMode === 'auto') {
      if (!['multi', 'single'].includes(track)) {
        return res.status(400).json({ error: 'schedule=auto requires track=multi|single' });
      }
      if (!shouldRunScheduledTrack(track, etTime)) {
        return res.status(200).json({
          ok: true,
          skipped: true,
          reason: `Not a scheduled ${track} slot`,
          etDate,
          etTime,
          track
        });
      }
    }

    const todayBySection = await sql`
      SELECT section, COUNT(*)::int AS "count"
      FROM article_drafts
      WHERE created_at >= date_trunc('day', now())
        AND created_via = 'auto'
      GROUP BY section
    `;

    const todayTokensRows = await sql`
      SELECT COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM article_drafts
      WHERE created_at >= date_trunc('day', now())
        AND created_via = 'auto'
    `;
    const tokensUsedToday = todayTokensRows?.[0]?.tokens || 0;
    if (!dryRun && tokensUsedToday >= dailyTokenBudget) {
      return res.status(200).json({
        ok: true,
        skipped: true,
        reason: 'Daily token budget reached',
        dailyTokenBudget,
        tokensUsedToday
      });
    }

    const generatedMap = {};
    for (const row of todayBySection) {
      generatedMap[row.section] = row.count;
    }

    const remainingBySection = {};
    let remainingToday = 0;
    for (const section of SECTION_ORDER) {
      const target = SECTION_DAILY_TARGETS[section] || 0;
      const done = generatedMap[section] || 0;
      const remaining = Math.max(0, target - done);
      remainingBySection[section] = remaining;
      if (activeSections.includes(section)) {
        remainingToday += remaining;
      }
    }

    const targetCount = dryRun ? requestedCount : Math.min(requestedCount, remainingToday);

    if (targetCount <= 0) {
      return res.status(200).json({
        ok: true,
        dryRun,
        requested: requestedCount,
        includeSections,
        excludeSections,
        activeSections,
        sectionTargets: SECTION_DAILY_TARGETS,
        remainingBySection,
        createdCount: 0,
        skippedCount: 0,
        created: [],
        skipped: [],
        message: 'Daily section quotas reached'
      });
    }

    const runTargets = buildRunTargets(remainingBySection, targetCount, activeSections);
    const candidates = await fetchCandidates(runTargets, activeSections);
    const created = [];
    const skipped = [];
    let runTokensConsumed = 0;
    const createdBySection = {};
    for (const section of SECTION_ORDER) createdBySection[section] = 0;
    const existingTitleRows = await sql`
      SELECT title FROM articles
      WHERE pub_date >= NOW() - INTERVAL '14 days'
      UNION ALL
      SELECT title FROM article_drafts
      WHERE created_at >= NOW() - INTERVAL '14 days'
    `;
    const existingTitles = existingTitleRows.map((r) => cleanText(r.title)).filter(Boolean);
    const runTitles = [];

    for (const candidate of candidates) {
      if (!dryRun && (tokensUsedToday + runTokensConsumed) >= dailyTokenBudget) {
        skipped.push({ reason: 'daily_token_budget_reached', title: candidate.title, url: candidate.url });
        break;
      }
      if (created.length >= targetCount) break;
      if (createdBySection[candidate.section] >= (runTargets[candidate.section] || 0)) {
        continue;
      }

      const exists = await alreadyExists(sql, candidate);
      if (exists) {
        skipped.push({ reason: 'duplicate', title: candidate.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(candidate.title, existingTitles) || isNearDuplicateTitle(candidate.title, runTitles)) {
        skipped.push({ reason: 'near_duplicate_title', title: candidate.title, url: candidate.url });
        continue;
      }

      const { draft, words } = await buildDraftWithMinWords(candidate);
      if (!draft.title || !draft.content) {
        skipped.push({ reason: 'rejected_non_local_or_empty', title: candidate.title, url: candidate.url });
        continue;
      }
      if (words < MIN_ARTICLE_WORDS) {
        skipped.push({
          reason: 'below_min_word_count',
          title: draft.title || candidate.title,
          url: candidate.url,
          words
        });
        continue;
      }
      if (isNearDuplicateTitle(draft.title, existingTitles) || isNearDuplicateTitle(draft.title, runTitles)) {
        skipped.push({ reason: 'near_duplicate_draft_title', title: draft.title, url: candidate.url });
        continue;
      }
      const slug = generateSlug(draft.title);

      if (!dryRun) {
        await sql`
          INSERT INTO article_drafts (
            slug,
            title,
            description,
            content,
            section,
            source_url,
            source_title,
            source_published_at,
            pub_date,
            model,
            input_tokens,
            output_tokens,
            total_tokens,
            created_via,
            status
          )
          VALUES (
            ${slug},
            ${draft.title},
            ${draft.description},
            ${draft.content},
            ${draft.section},
            ${candidate.url},
            ${candidate.title},
            ${candidate.sourcePublishedAt},
            ${new Date().toISOString()},
            ${draft.model},
            ${draft.inputTokens || 0},
            ${draft.outputTokens || 0},
            ${draft.totalTokens || 0},
            'auto',
            'pending_review'
          )
          ON CONFLICT (slug) DO NOTHING
        `;
      }

      createdBySection[draft.section] = (createdBySection[draft.section] || 0) + 1;
      runTokensConsumed += Number(draft.totalTokens || 0);
      runTitles.push(draft.title);
      created.push({
        slug,
        title: draft.title,
        section: draft.section,
        sourceUrl: candidate.url,
        words,
        inputTokens: draft.inputTokens || 0,
        outputTokens: draft.outputTokens || 0,
        totalTokens: draft.totalTokens || 0
      });
    }

    return res.status(200).json({
      ok: true,
      dryRun,
      requested: requestedCount,
      dailyTokenBudget,
      tokensUsedToday,
      runTokensConsumed,
      tokensUsedAfterRun: tokensUsedToday + runTokensConsumed,
      scheduleMode,
      track,
      etDate,
      etTime,
      includeSections,
      excludeSections,
      activeSections,
      targetCount,
      sectionTargets: SECTION_DAILY_TARGETS,
      runTargets,
      remainingBySection,
      createdBySection,
      createdCount: created.length,
      skippedCount: skipped.length,
      created,
      skipped: skipped.slice(0, 20)
    });
  } catch (error) {
    console.error('Generate drafts error:', error);
    return res.status(500).json({ error: 'Failed to generate drafts', details: error.message });
  }
};
