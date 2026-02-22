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
const SPORTS_FOCUS_MODES = new Set(['auto', 'college_basketball', 'nba', 'baseball', 'football', 'broad']);
const SPORTS_UPCOMING_TERMS = [
  'preview',
  'upcoming',
  'schedule',
  'matchup',
  'vs',
  'versus',
  'tonight',
  'tomorrow',
  'this week',
  'next game',
  'game notes',
  'tipoff',
  'kickoff'
];
const SPORTS_LOCAL_COMMUNITY_TERMS = [
  'high school',
  'hs',
  'prep',
  'varsity',
  'district',
  'regional',
  'state semifinal',
  'state championship',
  'miami valley conference',
  'gwoc',
  'mvl',
  'dayton area',
  'community league',
  'youth sports'
];

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

function resolveSportsFocusMode(rawMode, etNowParts) {
  const requested = String(rawMode || process.env.SPORTS_FOCUS_MODE || 'auto').trim().toLowerCase();
  const mode = SPORTS_FOCUS_MODES.has(requested) ? requested : 'auto';
  if (mode !== 'auto') return mode;

  // ET seasonal auto-routing:
  // Sep-Jan: football heavy
  // Feb-Mar: college basketball heavy
  // Apr-May: NBA bridge (Cavs/Pacers) after local college hoops cools off
  // Jun-Aug: baseball heavy
  const month = Number(etNowParts?.month || '1');
  if (month >= 9 || month <= 1) return 'football';
  if (month >= 2 && month <= 3) return 'college_basketball';
  if (month >= 4 && month <= 5) return 'nba';
  if (month >= 6 && month <= 8) return 'baseball';
  return 'broad';
}

const BASE_FEEDS_BY_SECTION = {
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
  sports: [],
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

const SPORTS_FEEDS_BY_MODE = {
  college_basketball: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+college+basketball+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:daytonflyers.com+basketball+preview+schedule+game+notes+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:wsuraiders.com+basketball+preview+schedule+game+notes+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:miamiredhawks.com+basketball+preview+schedule+game+notes+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=University+of+Dayton+basketball+preview+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Wright+State+basketball+preview+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Miami+University+Ohio+basketball+preview+when:7d&hl=en-US&gl=US&ceid=US:en'
  ],
  nba: [
    'https://news.google.com/rss/search?q=Cleveland+Cavaliers+preview+matchup+injury+report+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Indiana+Pacers+preview+matchup+injury+report+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Cavs+Pacers+playoff+race+eastern+conference+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Ohio+Cavs+Pacers+when:5d&hl=en-US&gl=US&ceid=US:en'
  ],
  baseball: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+baseball+Dragons+Reds+when:5d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:daytondragons.com+preview+schedule+when:10d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Cincinnati+Reds+preview+rotation+lineup+injury+report+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:mlb.com/reds+preview+schedule+when:10d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:daytonflyers.com+baseball+preview+schedule+when:10d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:wsuraiders.com+baseball+preview+schedule+when:10d&hl=en-US&gl=US&ceid=US:en'
  ],
  football: [
    'https://news.google.com/rss/search?q=Cincinnati+Bengals+preview+injury+report+matchup+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Ohio+State+Buckeyes+football+preview+injury+report+matchup+when:4d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:bengals.com+preview+game+notes+when:10d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:ohiostatebuckeyes.com+football+preview+game+notes+when:10d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Ohio+Bengals+Buckeyes+football+when:5d&hl=en-US&gl=US&ceid=US:en'
  ],
  broad: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+sports+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=University+of+Dayton+athletics+preview+schedule+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Wright+State+athletics+preview+schedule+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Miami+University+Ohio+athletics+preview+schedule+when:7d&hl=en-US&gl=US&ceid=US:en'
  ]
};

const SPORTS_LOCAL_COMMUNITY_FEEDS = [
  'https://news.google.com/rss/search?q=Dayton+Ohio+high+school+sports+when:7d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Miami+Valley+high+school+sports+when:7d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Dayton+area+prep+sports+when:7d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=GWOC+sports+Dayton+when:14d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Miami+Valley+League+sports+Ohio+when:14d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Dayton+community+sports+league+when:14d&hl=en-US&gl=US&ceid=US:en'
];

function getFeedsBySection(sportsFocusMode) {
  const modeSports = SPORTS_FEEDS_BY_MODE[sportsFocusMode] || SPORTS_FEEDS_BY_MODE.broad;
  return {
    ...BASE_FEEDS_BY_SECTION,
    sports: [...modeSports, ...SPORTS_LOCAL_COMMUNITY_FEEDS]
  };
}

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
    ? 'This article MUST be focused on Dayton, Ohio and/or the Miami Valley region. "Miami" in this context means Miami Valley or Miami University (Oxford, Ohio), not Miami, Florida. If the source is not locally relevant, reject it by returning JSON with empty title/description/content.'
    : 'This article may cover broader non-local scope.';
  const sportsRule = candidate.section === 'sports'
    ? `Prioritize upcoming local game coverage (previews, schedules, matchup context, stakes, and what to watch) when available. Current sports focus mode is "${candidate.sportsFocusMode || 'broad'}". Also prioritize Dayton/Miami Valley high school and community sports whenever strong local coverage is available. Avoid writing a second article on the same recent matchup unless there is clearly new and material information.`
    : '';

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
9) ${sportsRule}

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

function scoreSportsCandidate(candidate) {
  const text = `${candidate.title || ''} ${candidate.snippet || ''}`.toLowerCase();
  let score = 0;
  const mode = String(candidate.sportsFocusMode || 'broad');
  for (const term of SPORTS_UPCOMING_TERMS) {
    if (text.includes(term)) score += 2;
  }
  if (text.includes('dayton flyers') || text.includes('university of dayton')) score += 2;
  if (text.includes('wright state') || text.includes('raiders')) score += 2;
  if (text.includes('miami redhawks') || text.includes('miami university')) score += 2;
  if (text.includes('dayton dragons') || text.includes('dragons')) score += 3;
  if (text.includes('reds') || text.includes('cincinnati reds')) score += 3;
  if (text.includes('cavaliers') || text.includes('cavs')) score += 3;
  if (text.includes('pacers') || text.includes('indiana pacers')) score += 3;
  if (text.includes('bengals') || text.includes('cincinnati bengals')) score += 3;
  if (text.includes('ohio state') || text.includes('buckeyes')) score += 3;
  if (mode === 'college_basketball' && (text.includes('cavaliers') || text.includes('pacers'))) score += 2;
  if (mode === 'nba' && (text.includes('cavaliers') || text.includes('cavs'))) score += 4;
  if (mode === 'nba' && (text.includes('pacers') || text.includes('indiana pacers'))) score += 4;
  if (mode === 'nba' && (text.includes('flyers') || text.includes('raiders') || text.includes('redhawks'))) score += 1;
  if (mode === 'nba' && (text.includes('dragons') || text.includes('reds') || text.includes('baseball'))) score -= 2;
  if (mode === 'baseball' && (text.includes('dayton dragons') || text.includes('dragons'))) score += 4;
  if (mode === 'baseball' && (text.includes('reds') || text.includes('cincinnati reds'))) score += 4;
  if (mode === 'baseball' && (text.includes('wright state') || text.includes('raiders'))) score += 3;
  if (mode === 'baseball' && (text.includes('dayton flyers') || text.includes('university of dayton'))) score += 2;
  if (mode === 'baseball' && (text.includes('flyers') || text.includes('raiders') || text.includes('redhawks'))) score -= 1;
  if (mode === 'football' && (text.includes('bengals') || text.includes('cincinnati bengals'))) score += 4;
  if (mode === 'football' && (text.includes('ohio state') || text.includes('buckeyes'))) score += 4;
  if (mode === 'football' && (text.includes('dragons') || text.includes('baseball'))) score -= 2;
  for (const term of SPORTS_LOCAL_COMMUNITY_TERMS) {
    if (text.includes(term)) score += 2;
  }
  if (text.includes('dayton') && (text.includes('high school') || text.includes('prep') || text.includes('varsity'))) {
    score += 3;
  }
  if (text.includes('miami valley') && (text.includes('high school') || text.includes('prep') || text.includes('community'))) {
    score += 3;
  }
  if (text.includes('postgame') || text.includes('final score') || text.includes('recap')) score -= 2;
  return score;
}

function isFloridaMiamiSportsNoise(candidate) {
  if (candidate.section !== 'sports') return false;
  const text = `${candidate.title || ''} ${candidate.snippet || ''}`.toLowerCase();
  const hasMiami = text.includes('miami');
  if (!hasMiami) return false;

  const ohioSignals = [
    'miami valley',
    'miami university',
    'redhawks',
    'oxford, ohio',
    'oxford ohio'
  ];
  const floridaSignals = [
    'miami heat',
    'miami hurricanes',
    'inter miami',
    'miami dolphins',
    'miami marlins',
    'fort lauderdale'
  ];

  if (ohioSignals.some((s) => text.includes(s))) return false;
  if (floridaSignals.some((s) => text.includes(s))) return true;
  return false;
}

async function fetchCandidates(runTargets, activeSections, sportsFocusMode) {
  const out = [];
  const seen = new Set();
  const activeSet = new Set(activeSections);
  const feedsBySection = getFeedsBySection(sportsFocusMode);

  for (const section of SECTION_ORDER) {
    if (!activeSet.has(section)) continue;
    if (!runTargets[section]) continue;
    const perSection = Math.max(8, runTargets[section] * 4);
    const sectionCandidates = [];

    for (const feedUrl of (feedsBySection[section] || [])) {
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

          sectionCandidates.push({
            section,
            title,
            url,
            snippet: cleanText(item.contentSnippet || item.content || ''),
            sourcePublishedAt: item.isoDate || item.pubDate || null,
            sportsFocusMode
          });
        }
      } catch (error) {
        console.error(`Feed fetch failed for ${section}:`, error.message);
      }
    }

    if (section === 'sports') {
      sectionCandidates.sort((a, b) => scoreSportsCandidate(b) - scoreSportsCandidate(a));
    }
    out.push(...sectionCandidates);
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

function buildBigrams(tokens) {
  const out = [];
  for (let i = 0; i < tokens.length - 1; i += 1) {
    out.push(`${tokens[i]} ${tokens[i + 1]}`);
  }
  return out;
}

function jaccardSimilarity(aSet, bSet) {
  if (!aSet.size || !bSet.size) return 0;
  let overlap = 0;
  for (const token of aSet) {
    if (bSet.has(token)) overlap += 1;
  }
  return overlap / (aSet.size + bSet.size - overlap);
}

const SPORTS_TEAM_ALIASES = {
  'miami-redhawks': ['miami redhawks', 'redhawks', 'miami university'],
  'wright-state-raiders': ['wright state', 'raiders', 'wsu raiders'],
  'dayton-flyers': ['dayton flyers', 'flyers', 'university of dayton'],
  'dayton-dragons': ['dayton dragons', 'dragons'],
  'cincinnati-reds': ['cincinnati reds', 'reds'],
  'cincinnati-bengals': ['cincinnati bengals', 'bengals'],
  'ohio-state-buckeyes': ['ohio state', 'buckeyes'],
  'cleveland-cavaliers': ['cleveland cavaliers', 'cavs', 'cavaliers'],
  'indiana-pacers': ['indiana pacers', 'pacers'],
  'bowling-green-falcons': ['bowling green', 'falcons'],
  'dayton-dutch-lions': ['dayton dutch lions', 'dutch lions']
};

function detectSportsTeams(text) {
  const lower = String(text || '').toLowerCase();
  const teams = [];
  for (const [canonical, aliases] of Object.entries(SPORTS_TEAM_ALIASES)) {
    if (aliases.some((alias) => lower.includes(alias))) {
      teams.push(canonical);
    }
  }
  return teams.sort();
}

function buildSportsEventKeys(text) {
  const lower = String(text || '').toLowerCase();
  const teams = detectSportsTeams(lower);
  const keys = new Set();

  for (let i = 0; i < teams.length; i += 1) {
    for (let j = i + 1; j < teams.length; j += 1) {
      keys.add(`matchup:${teams[i]}|${teams[j]}`);
    }
  }
  for (const team of teams) {
    keys.add(`team:${team}`);
  }

  // Extra signal to catch same-game rewrites around common recap verbs.
  if (
    (lower.includes('beat') || lower.includes('defeat') || lower.includes('win') || lower.includes('falls to')) &&
    teams.length >= 2
  ) {
    keys.add(`result:${teams[0]}|${teams[1]}`);
  }

  return Array.from(keys);
}

function hasAnyKeyIntersection(candidateKeys, existingKeySet) {
  for (const key of candidateKeys) {
    if (existingKeySet.has(key)) return true;
  }
  return false;
}

function isNearDuplicateTitle(candidateTitle, existingTitles) {
  const cand = normalizeTitleForCompare(candidateTitle);
  if (!cand.length) return false;
  const candSet = new Set(cand);
  const candBigrams = new Set(buildBigrams(cand));

  for (const title of existingTitles) {
    const other = normalizeTitleForCompare(title);
    if (!other.length) continue;
    const otherSet = new Set(other);
    const otherBigrams = new Set(buildBigrams(other));
    let overlap = 0;
    for (const token of candSet) {
      if (otherSet.has(token)) overlap += 1;
    }
    const tokenJaccard = jaccardSimilarity(candSet, otherSet);
    const bigramJaccard = jaccardSimilarity(candBigrams, otherBigrams);
    const overlapByMin = overlap / Math.max(1, Math.min(candSet.size, otherSet.size));

    // Catch same-story rewrites where headline wording changes but key entities remain.
    if (tokenJaccard >= 0.5 || bigramJaccard >= 0.35 || (overlap >= 4 && overlapByMin >= 0.75)) {
      return true;
    }
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
    const sportsFocusMode = resolveSportsFocusMode(req.body?.sportsFocusMode || req.query.sportsFocusMode, etNowParts);
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
    const candidates = await fetchCandidates(runTargets, activeSections, sportsFocusMode);
    const created = [];
    const skipped = [];
    let runTokensConsumed = 0;
    const createdBySection = {};
    for (const section of SECTION_ORDER) createdBySection[section] = 0;
    const existingTitleRows = await sql`
      SELECT section, title AS compare_text FROM articles
      WHERE pub_date >= NOW() - INTERVAL '30 days'
      UNION ALL
      SELECT section, title AS compare_text FROM article_drafts
      WHERE created_at >= NOW() - INTERVAL '30 days'
      UNION ALL
      SELECT section, source_title AS compare_text FROM article_drafts
      WHERE created_at >= NOW() - INTERVAL '30 days'
        AND source_title IS NOT NULL
        AND source_title != ''
    `;
    const existingTitles = Array.from(
      new Set(
        existingTitleRows
          .map((r) => cleanText(r.compare_text))
          .filter(Boolean)
      )
    );
    const existingSportsEventKeys = new Set();
    for (const row of existingTitleRows) {
      if (normalizeSection(row.section) !== 'sports') continue;
      const compareText = cleanText(row.compare_text);
      if (!compareText) continue;
      for (const key of buildSportsEventKeys(compareText)) {
        existingSportsEventKeys.add(key);
      }
    }
    const runTitles = [];
    const runSportsEventKeys = new Set();

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
      if (isFloridaMiamiSportsNoise(candidate)) {
        skipped.push({ reason: 'non_local_miami_noise', title: candidate.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(candidate.title, existingTitles) || isNearDuplicateTitle(candidate.title, runTitles)) {
        skipped.push({ reason: 'near_duplicate_title', title: candidate.title, url: candidate.url });
        continue;
      }
      if (candidate.section === 'sports') {
        const candidateEventKeys = buildSportsEventKeys(`${candidate.title} ${candidate.snippet || ''}`);
        if (
          candidateEventKeys.length &&
          (hasAnyKeyIntersection(candidateEventKeys, existingSportsEventKeys) ||
            hasAnyKeyIntersection(candidateEventKeys, runSportsEventKeys))
        ) {
          skipped.push({ reason: 'duplicate_sports_event', title: candidate.title, url: candidate.url });
          continue;
        }
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
      if (draft.section === 'sports') {
        const draftEventKeys = buildSportsEventKeys(`${draft.title} ${draft.description || ''}`);
        if (
          draftEventKeys.length &&
          (hasAnyKeyIntersection(draftEventKeys, existingSportsEventKeys) ||
            hasAnyKeyIntersection(draftEventKeys, runSportsEventKeys))
        ) {
          skipped.push({ reason: 'duplicate_sports_event_draft', title: draft.title, url: candidate.url });
          continue;
        }
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
      if (draft.section === 'sports') {
        const eventKeys = buildSportsEventKeys(`${draft.title} ${draft.description || ''}`);
        for (const key of eventKeys) {
          runSportsEventKeys.add(key);
        }
      }
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
      sportsFocusMode,
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
