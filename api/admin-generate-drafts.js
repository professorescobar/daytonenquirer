const { neon } = require('@neondatabase/serverless');
const Parser = require('rss-parser');
const { requireAdmin } = require('./_admin-auth');
const { getDailyTokenBudgets } = require('./_admin-settings');
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

const MIN_ARTICLE_WORDS = 550;
const TARGET_ARTICLE_WORDS = 700;
const DEFAULT_MAX_OUTPUT_TOKENS = 2600;
const RETRY_MIN_INITIAL_WORDS = 300;
const DEFAULT_MEMORY_SUPPRESSION_ENABLED = false;
const DEFAULT_CLAUDE_PROMPT_MODE = 'relaxed';

const ET_TIME_ZONE = 'America/New_York';
const TRACK_SCHEDULES_BY_PROVIDER = {
  anthropic: {
    multiSlots: ['06:05', '08:05', '10:05', '12:05', '14:05', '16:05', '18:05', '22:05'],
    singleSlots: ['10:05', '14:05'],
    multiCountBySlot: {
      '06:05': 4,
      '08:05': 4,
      '10:05': 2,
      '12:05': 2,
      '14:05': 2,
      '16:05': 4,
      '18:05': 4,
      '22:05': 1
    },
    singleCountBySlot: {
      '10:05': 1,
      '14:05': 1
    }
  },
  openai: {
    multiSlots: ['06:25', '08:25', '10:25', '12:25', '14:25', '16:25', '18:25', '22:25'],
    singleSlots: ['10:25', '14:25'],
    multiCountBySlot: {
      '06:25': 4,
      '08:25': 4,
      '10:25': 2,
      '12:25': 2,
      '14:25': 2,
      '16:25': 4,
      '18:25': 4,
      '22:25': 1
    },
    singleCountBySlot: {
      '10:25': 1,
      '14:25': 1
    }
  },
  gemini: {
    multiSlots: ['06:45', '08:45', '10:45', '12:45', '14:45', '16:45', '18:45', '22:45'],
    singleSlots: ['10:45', '14:45'],
    multiCountBySlot: {
      '06:45': 4,
      '08:45': 4,
      '10:45': 2,
      '12:45': 2,
      '14:45': 2,
      '16:45': 4,
      '18:45': 4,
      '22:45': 1
    },
    singleCountBySlot: {
      '10:45': 1,
      '14:45': 1
    }
  },
  grok: {
    multiSlots: ['06:55', '08:55', '10:55', '12:55', '14:55', '16:55', '18:55', '22:55'],
    singleSlots: ['10:55', '14:55'],
    multiCountBySlot: {
      '06:55': 4,
      '08:55': 4,
      '10:55': 2,
      '12:55': 2,
      '14:55': 2,
      '16:55': 4,
      '18:55': 4,
      '22:55': 1
    },
    singleCountBySlot: {
      '10:55': 1,
      '14:55': 1
    }
  }
};
const CATCH_UP_END_HOUR_ET = 22;
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
const SPORTS_HOCKEY_TERMS = [
  'hockey',
  'ice hockey',
  'nhl',
  'echl',
  'puck drop',
  'power play',
  'penalty kill',
  'overtime',
  'shootout',
  'dayton bombers',
  'flyers club hockey',
  'dayton stealth',
  'raha'
];
const SPORTS_US_OLYMPICS_TERMS = [
  'team usa',
  'u.s. olympic',
  'us olympic',
  'olympic trials',
  'usa olympic',
  'team usa olympics'
];
const SPORTS_RACKET_TERMS = [
  'tennis',
  'pickleball',
  'cincinnati open',
  'western & southern open',
  'atp',
  'wta',
  'hard court'
];
const SPORTS_TENNIS_TERMS = [
  'tennis',
  'cincinnati open',
  'western & southern open',
  'atp',
  'wta',
  'hard court',
  'dayton flyers tennis',
  'university of dayton tennis'
];
const LOCAL_CITY_TERMS = [
  'dayton',
  'oakwood',
  'kettering',
  'moraine',
  'trotwood',
  'huber heights',
  'beavercreek',
  'bellbrook',
  'fairborn',
  'xenia',
  'springfield',
  'troy',
  'tipp city',
  'miamisburg',
  'centerville',
  'springboro',
  'franklin',
  'middletown',
  'miami valley',
  'montgomery county'
];
const LOCAL_INSTITUTION_TERMS = [
  'sinclair community college',
  'kettering college',
  'central state university',
  'antioch university midwest',
  'cedarville university',
  'soche',
  'strategic ohio council for higher education',
  'wright-patterson',
  'wright patt',
  'fuyao',
  'fuyao glass',
  'fuyao glass america',
  'dayton international airport',
  'james m. cox dayton international airport',
  '2nd street market',
  'second street market',
  'dayton food truck rally',
  'oregon district',
  'stivers school of the arts',
  'lexisnexis',
  'reynolds and reynolds',
  'honda anna'
];
const HEALTH_INSTITUTION_TERMS = [
  'dayton children',
  'dayton children’s',
  "dayton children's",
  'dayton childrens',
  'kettering health',
  'premier health',
  'premier health network',
  'dayton va',
  'dayton va hospital',
  'dayton veterans affairs medical center',
  'va dayton',
  'care source',
  'caresource',
  'dayton & montgomery county public health',
  'dayton and montgomery county public health',
  'greater dayton area hospital association',
  'gdaha',
  'community health centers of greater dayton',
  'five rivers health centers',
  'ohio department of health',
  'ohiohealth newsroom',
  'dayton daily news health',
  'hospital',
  'medical center',
  'health network'
];
const NATIONAL_NON_OHIO_STATE_TERMS = [
  'california', 'texas', 'florida', 'new york', 'georgia', 'north carolina', 'virginia',
  'pennsylvania', 'michigan', 'illinois', 'indiana', 'wisconsin', 'minnesota', 'missouri',
  'arizona', 'colorado', 'washington', 'oregon', 'massachusetts', 'new jersey', 'tennessee',
  'alabama', 'louisiana', 'kentucky', 'south carolina', 'north dakota', 'south dakota',
  'nevada', 'utah', 'kansas', 'oklahoma', 'iowa', 'nebraska', 'montana', 'idaho', 'wyoming',
  'new mexico', 'maine', 'vermont', 'new hampshire', 'connecticut', 'rhode island', 'maryland',
  'west virginia', 'delaware', 'alaska', 'hawaii', 'arkansas', 'mississippi'
];
const NATIONAL_LOW_PRIORITY_POLITICS_TERMS = [
  'poll', 'polling', 'approval rating', 'campaign trail', 'rally', 'partisan',
  'democrat vs republican', 'gop', 'dnc', 'rnc', 'culture war'
];
const NATIONAL_EXCLUDED_STATE = 'ohio';
const BUSINESS_MARKET_UPDATE_SLOTS_ET = ['06:05', '08:05'];
const BUSINESS_LOCAL_DAILY_MIN = 1;
const BUSINESS_DAILY_MARKET_UPDATE_MAX = 1;
const BUSINESS_MARKET_UPDATE_TERMS = [
  'market update', 'market outlook', 'futures', 's&p 500', 'nasdaq', 'dow', 'treasury yield',
  'bond market', 'commodities', 'u.s. dollar', 'dxy', 'market wrap', 'closing bell'
];
const BUSINESS_EARNINGS_TERMS = [
  'earnings', 'quarterly results', 'guidance', 'revenue', 'eps', 'profit', 'forecast'
];
const BUSINESS_LARGE_CAP_TERMS = [
  'mega cap', 'large cap', 's&p 500', 'dow', 'nasdaq 100', 'blue chip'
];
const BUSINESS_SMALL_CAP_NOISE_TERMS = [
  'penny stock', 'microcap', 'small-cap', 'otc', 'otcmkts', 'pink sheets'
];
const ENTERTAINMENT_EVENT_TERMS = [
  'festival', 'concert', 'show', 'live music', 'exhibit', 'gallery', 'theater',
  'things to do', 'weekend events', 'arts', 'performance', 'downtown events',
  'food truck', 'market', 'fair', 'nightlife'
];
const ENTERTAINMENT_SURROUNDING_CITY_TERMS = [
  'oakwood', 'kettering', 'moraine', 'trotwood', 'huber heights', 'beavercreek',
  'bellbrook', 'fairborn', 'xenia', 'springfield', 'troy', 'tipp city', 'miamisburg',
  'centerville', 'springboro', 'franklin', 'middletown'
];
const TECHNOLOGY_LOCAL_TERMS = [
  'sinclair', 'sinclair community college', 'university of dayton', 'dayton flyers',
  'wright state', 'miami valley career technology center', 'mvctc', 'wright-patterson',
  'wright patt', 'dayton inno', 'ohio tech news', 'ohiox', 'technology first',
  'dayton development coalition', 'the entrepreneurs center', "entrepreneur's center",
  'startup', 'venture capital', 'government contract', 'sbir', 'tech transfer'
];
const WORLD_REQUIRED_DAILY_REGIONS = ['europe', 'southeast_asia'];
const WORLD_SPREAD_REGIONS = [
  'southeast_asia',
  'europe',
  'south_america',
  'africa',
  'india',
  'russia',
  'australia',
  'middle_east'
];
const WORLD_REGION_TERMS = {
  southeast_asia: [
    'southeast asia', 'asean', 'indonesia', 'thailand', 'vietnam', 'philippines',
    'malaysia', 'singapore', 'myanmar', 'cambodia', 'laos', 'brunei', 'timor-leste'
  ],
  europe: [
    'europe', 'eu ', 'european union', 'uk ', 'united kingdom', 'germany', 'france',
    'italy', 'spain', 'poland', 'netherlands', 'sweden', 'norway', 'finland', 'greece',
    'ukraine', 'belgium', 'ireland', 'switzerland', 'austria', 'portugal'
  ],
  south_america: [
    'south america', 'brazil', 'argentina', 'chile', 'colombia', 'peru', 'ecuador',
    'uruguay', 'paraguay', 'bolivia', 'venezuela', 'guyana', 'suriname'
  ],
  africa: [
    'africa', 'nigeria', 'south africa', 'kenya', 'egypt', 'ethiopia', 'ghana',
    'tanzania', 'uganda', 'morocco', 'algeria', 'tunisia', 'senegal', 'sudan'
  ],
  india: ['india', 'indian'],
  russia: ['russia', 'russian'],
  australia: ['australia', 'australian', 'new zealand'],
  middle_east: [
    'middle east', 'israel', 'gaza', 'palestinian', 'iran', 'iraq', 'syria', 'lebanon',
    'saudi arabia', 'yemen', 'jordan', 'qatar', 'uae', 'united arab emirates', 'oman',
    'bahrain', 'kuwait'
  ]
};
const WORLD_US_DIPLOMACY_TERMS = [
  'u.s. diplomacy', 'us diplomacy', 'u.s. embassy', 'us embassy', 'state department',
  'secretary of state', 'american diplomat', 'u.s. envoy', 'us envoy', 'bilateral talks with the u.s.',
  'white house says', 'u.s. sanctions', 'us sanctions', 'u.s. foreign policy', 'us foreign policy'
];
const WORLD_US_CENTRIC_TERMS = [
  'united states',
  'u.s.',
  'us ',
  'white house',
  'washington',
  'u.s. congress',
  'us congress',
  'u.s. senate',
  'us senate',
  'u.s. house',
  'us house',
  'u.s. election',
  'us election',
  'u.s. president',
  'us president'
];
const POLITICAL_TOPIC_TERMS = [
  'election',
  'vote',
  'voter',
  'congress',
  'senate',
  'house',
  'governor',
  'president',
  'supreme court',
  'legislation',
  'bill',
  'executive order',
  'immigration',
  'border',
  'crime policy',
  'public safety policy',
  'foreign policy',
  'tax policy'
];
const OPINION_STYLE_TERMS = [
  'op-ed',
  'op ed',
  'opinion',
  'editorial',
  'column',
  'commentary',
  'analysis:'
];
const LOCAL_SPORTS_NOISE_TERMS = [
  'athletics',
  'basketball',
  'football',
  'baseball',
  'softball',
  'soccer',
  'hockey',
  'tennis',
  'volleyball',
  'wrestling',
  'matchup',
  'vs',
  'game preview',
  'game recap',
  'tipoff',
  'kickoff'
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

function getTrackSchedule(writerProvider) {
  return TRACK_SCHEDULES_BY_PROVIDER[writerProvider] || TRACK_SCHEDULES_BY_PROVIDER.anthropic;
}

function shouldRunScheduledTrack(track, etTime, writerProvider) {
  const schedule = getTrackSchedule(writerProvider);
  if (track === 'single') return schedule.singleSlots.includes(etTime);
  if (track === 'multi') return schedule.multiSlots.includes(etTime);
  return false;
}

function getScheduledRequestedCount(track, etTime, writerProvider) {
  const schedule = getTrackSchedule(writerProvider);
  if (track === 'single') return schedule.singleCountBySlot[etTime] || null;
  if (track === 'multi') return schedule.multiCountBySlot[etTime] || null;
  return null;
}

function getRemainingScheduledSlots(track, etTime, writerProvider) {
  const schedule = getTrackSchedule(writerProvider);
  const slots = track === 'single'
    ? schedule.singleSlots
    : (track === 'multi' ? schedule.multiSlots : []);
  const idx = slots.indexOf(etTime);
  if (idx < 0) return 0;
  return Math.max(0, slots.length - idx);
}

function shouldApplyCatchUpForEtTime(etTime) {
  const hour = Number(String(etTime || '').split(':')[0]);
  if (!Number.isFinite(hour)) return false;
  return hour < CATCH_UP_END_HOUR_ET;
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
    'https://news.google.com/rss/search?q=Dayton+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Kettering+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Beavercreek+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Centerville+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Miamisburg+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Fairborn+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Springfield+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Xenia+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Troy+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Huber+Heights+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  national: [
    'https://news.google.com/rss/search?q=national+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=us+national+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wisconsin+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=california+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=texas+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=florida+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=new+york+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=kentucky+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=michigan+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=pennsylvania+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=indiana+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  world: [
    'https://news.google.com/rss/search?q=world+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=international+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=middle+east+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=africa+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=india+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=russia+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=uk+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=germany+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=brazil+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=argentina+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=japan+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=south+korea+news+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  business: [
    'https://news.google.com/rss/search?q=earnings+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=stock+market+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=market+movers+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=analyst+upgrade+downgrade+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=large+cap+company+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=nasdaq+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=s%26p+500+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dow+jones+news+when:1d&hl=en-US&gl=US&ceid=US:en'
  ],
  sports: [],
  health: [
    'https://news.google.com/rss/search?q=health+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=breaking+health+news+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+health+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+children%27s+hospital+news+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=kettering+health+news+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=premier+health+dayton+news+when:7d&hl=en-US&gl=US&ceid=US:en'
  ],
  entertainment: [
    'https://news.google.com/rss/search?q=dayton+entertainment+events+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+live+events+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=schuster+center+dayton+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=victoria+theatre+dayton+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=fraze+pavilion+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=rose+music+center+huber+heights+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+art+institute+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=oregon+district+events+dayton+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  technology: [
    'https://news.google.com/rss/search?q=dayton+technology+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+patt+technology+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=afrl+dayton+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=university+of+dayton+research+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+engineering+research+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=sinclair+community+college+technology+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+startup+funding+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=ohio+technology+innovation+when:3d&hl=en-US&gl=US&ceid=US:en'
  ]
};

const SPORTS_FEEDS_BY_MODE = {
  college_basketball: [
    'https://news.google.com/rss/search?q=dayton+flyers+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+raiders+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+redhawks+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+ohio+high+school+sports+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+dragons+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+university+hockey+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=ud+tennis+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+baseball+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  nba: [
    'https://news.google.com/rss/search?q=dayton+flyers+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+raiders+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+redhawks+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+ohio+high+school+sports+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+dragons+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+university+hockey+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=ud+tennis+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+baseball+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  baseball: [
    'https://news.google.com/rss/search?q=dayton+flyers+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+raiders+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+redhawks+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+ohio+high+school+sports+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+dragons+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+university+hockey+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=ud+tennis+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+baseball+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  football: [
    'https://news.google.com/rss/search?q=dayton+flyers+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+raiders+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+redhawks+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+ohio+high+school+sports+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+dragons+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+university+hockey+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=ud+tennis+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+baseball+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  broad: [
    'https://news.google.com/rss/search?q=dayton+flyers+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+raiders+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+redhawks+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+ohio+high+school+sports+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=dayton+dragons+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=miami+university+hockey+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=ud+tennis+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=wright+state+baseball+when:3d&hl=en-US&gl=US&ceid=US:en'
  ]
};

function getFeedsBySection(sportsFocusMode) {
  const modeSports = SPORTS_FEEDS_BY_MODE[sportsFocusMode] || SPORTS_FEEDS_BY_MODE.broad;
  return {
    ...BASE_FEEDS_BY_SECTION,
    sports: modeSports
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

function getModelMaxOutputTokens() {
  const configured = parseInt(
    process.env.ANTHROPIC_MAX_OUTPUT_TOKENS || String(DEFAULT_MAX_OUTPUT_TOKENS),
    10
  );
  if (!Number.isFinite(configured) || configured <= 0) return DEFAULT_MAX_OUTPUT_TOKENS;
  return Math.min(configured, 8192);
}

function isMemorySuppressionEnabled(rawValue) {
  if (rawValue === undefined || rawValue === null || rawValue === '') {
    return DEFAULT_MEMORY_SUPPRESSION_ENABLED;
  }
  return String(rawValue).trim().toLowerCase() === 'true';
}

function getClaudePromptMode(rawValue) {
  const mode = String(rawValue || DEFAULT_CLAUDE_PROMPT_MODE).trim().toLowerCase();
  return mode === 'strict' ? 'strict' : 'relaxed';
}

function buildDraftPrompt(candidate, writerProvider = 'anthropic') {
  const claudePromptMode = getClaudePromptMode(process.env.CLAUDE_PROMPT_MODE);
  const isClaudeRelaxed = writerProvider === 'anthropic' && claudePromptMode === 'relaxed';
  const sectionVoice = ({
    local: 'You are a feature reporter for a local Dayton, Ohio news publication.',
    sports: 'You are a sports news contributor for a local Dayton, Ohio news publication.',
    national: 'You are a contributor for a U.S. national news source.',
    world: 'You are a contributor for an international news publication.',
    business: 'You are the editorial desk of a financial news publication.',
    health: 'You are a contributor for a health news publication.',
    entertainment: 'You are a contributor for an entertainment news publication.',
    technology: 'You are a contributor for a science and engineering publication.'
  })[candidate.section] || 'You are a contributor for a general news publication.';

  const sectionMission = ({
    local: 'Cover local-area stories with strong time-based relevance in a thought-provoking, detail-oriented, and concise style without being overly wordy or verbose, and avoid repeat coverage of the same event unless there is a clearly new development.',
    sports: 'Prioritize time-relevant sports coverage centered on two different story types: results for games that just happened and excitement-building previews for games about to happen, with Dayton/regional stories preferred when quality is comparable.',
    national: 'Prioritize timely U.S.-domestic stories from states outside Ohio, focusing on substantive developments and avoiding partisan noise or commentary cycles, and avoid repeat coverage of the same event unless there is a clearly new development.',
    world: 'Prioritize recent globally relevant stories with broad geographic spread and meaningful geopolitical context, and avoid repeat coverage of the same event unless there is a clearly new development.',
    business: 'Prioritize recent business coverage in three formats: earnings reactions, notable price-move explanations, and high-impact company events, written in a concise analytical style without hype, always opening with the current event and then explaining why the market reacted.',
    health: 'Prioritize recent health stories with direct practical relevance to readers, focusing on evidence-based reporting about what changed, who is affected, and why it matters now, with Dayton/regional relevance preferred when quality is comparable.',
    entertainment: 'Prioritize recent entertainment stories with high audience interest, focusing first on video games, then TV/movies, then music, then art, and using pop culture as a secondary fallback when stronger options are limited, with Dayton/regional relevance preferred when quality is comparable.',
    technology: 'Prioritize recent technology stories with high intellectual and audience interest, focusing on meaningful innovation, engineering breakthroughs, and scientific discoveries, with Dayton/regional relevance preferred when quality is comparable.'
  })[candidate.section] || '';

  const sharedStyleRule =
    'Write in a thought-provoking, detail-oriented, and concise style without being overly wordy or verbose.';

  const localRule = candidate.section === 'local'
    ? 'For the local section, keep the story anchored to the local area and avoid routing sports-focused coverage into local.'
    : '';
  const healthRule = candidate.section === 'health'
    ? 'For the health section, prioritize practical reader value with evidence-based framing. Explain what changed, who is affected, and why it matters now without alarmist language.'
    : '';
  const nationalRule = candidate.section === 'national'
    ? 'For the national section, keep focus on U.S.-domestic developments from states outside Ohio. Deprioritize international-diplomacy framing and partisan commentary cycles.'
    : '';
  const businessRule = candidate.section === 'business'
    ? `For the business section:
- Stay inside three formats: earnings reactions, notable price-move explanations, and high-impact company events.
- Open with the current event, then explain why the market reacted.
- No financial recommendations, no price targets, no buy/sell/hold calls.
- Keep tone analytical and concise; avoid hype or sentiment-chasing language.`
    : '';
  const technologyRule = candidate.section === 'technology'
    ? 'For the technology section, focus on meaningful innovation, engineering breakthroughs, and scientific discoveries with substantive technical depth.'
    : '';
  const marketUpdateFormatRule = candidate.section === 'business' && candidate.businessMode === 'daily_market_update'
    ? `This is the required daily market structure update. Use this exact editorial approach:
- Focus only on verified market facts and cross-asset structure.
- Required coverage blocks: (1) major indexes, (2) representative large-cap leaders/laggards, (3) bond market and yields, (4) commodities, (5) U.S. dollar.
- For each block, explicitly anchor observations to: 5-day, 30-day, 3-month, 6-month, and 1-year context.
- Explain what changed in the latest session and where that sits inside each broader window.
- Keep tone analytical, concise, and non-dramatic.
- Prohibited style: "bulls", "bears", "risk-on/risk-off", hype, emotional adjectives, or narrative fluff.
- Do not repeat generic template phrases from prior days.`
    : '';
  const politicsStyleRule =
    'If the story involves politics, use straight-news language focused on verified facts and practical consequences. Avoid partisan advocacy, ideological sloganeering, and opinion framing.';
  const sportsRule = candidate.section === 'sports'
    ? `For sports, stay within two different story types: results for games that just happened and excitement-building previews for games about to happen. Prefer Dayton/regional coverage when quality is comparable.`
    : '';
  const externalDuplicateTitles = Array.isArray(candidate.externalDuplicateTitles)
    ? candidate.externalDuplicateTitles.filter(Boolean).slice(0, 8).map((title) => truncate(cleanText(title), 120))
    : [];
  const externalDuplicateRule = externalDuplicateTitles.length
    ? `External duplicate memory (do not mirror these source story angles/headlines): ${externalDuplicateTitles.join(' | ')}`
    : 'External duplicate memory: none for this section in current memory window.';
  const claudeRelaxedStyleBlock = isClaudeRelaxed
    ? `Claude style priority:
- Write as one cohesive story, not a summary list.
- Maintain a clear narrative thread from first paragraph to last.
- Each paragraph must add a new detail or implication and connect logically to the previous one.
- Avoid repetition: do not restate the same point in new wording.
- Vary sentence structure and paragraph openings.
- Keep tone energetic and readable, but measured and non-promotional.
- Use concrete names, organizations, locations, dates, and numbers whenever available in the source context.
- Do not replace known specifics with generic wording.`
    : '';
  const verificationRule = isClaudeRelaxed
    ? 'Use source-grounded specificity and avoid generic phrasing when concrete details are available.'
    : 'Do not include fake quotes or unverifiable claims. If details are uncertain, state uncertainty clearly.';

  return `
${sectionVoice}
Write a fully original publication-ready draft for The Dayton Enquirer.

SECTION: ${candidate.section}
HEADLINE: ${candidate.title}
SOURCE URL: ${candidate.url}
SOURCE SNIPPET: ${candidate.snippet || 'N/A'}
SECTION MISSION: ${sectionMission}

Requirements:
1) Return valid JSON only.
2) JSON keys: title, description, content, section.
3) title: brief, attention-grabbing newsroom headline (8-14 words preferred, no clickbait).
   - Never copy or lightly rephrase the source headline. Write a distinctly different headline.
4) description: concise 2-4 sentence summary.
5) content: detailed long-form article in plain HTML-friendly text with paragraph breaks using \\n\\n.
   - Minimum ${MIN_ARTICLE_WORDS} words.
   - Target ${TARGET_ARTICLE_WORDS}-${TARGET_ARTICLE_WORDS + 300} words.
6) Writing quality:
   - Open with the current event immediately: what happened, where/when it happened, and who is involved.
   - Transition quickly to why it matters now, then support that with specific reported details.
   - Use concrete facts, not vague generalizations; avoid generic scene-setting and empty transitions.
   - Keep momentum and paragraph-level clarity: each paragraph should add a distinct new detail or implication.
   - Avoid cliches, AI-sounding filler, and repetitive phrasing.
7) ${sharedStyleRule}
8) section must be one of: local, national, world, business, sports, health, entertainment, technology.
9) ${verificationRule}
10) ${sportsRule}
11) ${localRule}
12) ${healthRule}
13) ${nationalRule}
14) ${politicsStyleRule}
15) ${businessRule}
16) ${marketUpdateFormatRule}
17) ${technologyRule}
18) ${externalDuplicateRule}
19) ${claudeRelaxedStyleBlock}

Return only JSON.
`;
}

function resolveWriterProvider(raw) {
  const provider = String(raw || process.env.DRAFT_WRITER_PROVIDER || 'anthropic').trim().toLowerCase();
  if (provider === 'openai') return 'openai';
  if (provider === 'gemini') return 'gemini';
  if (provider === 'grok') return 'grok';
  return 'anthropic';
}

function getWriterModelForProvider(writerProvider) {
  if (writerProvider === 'openai') return process.env.OPENAI_MODEL || 'gpt-5';
  if (writerProvider === 'gemini') return process.env.GEMINI_MODEL || 'gemini-3-pro-preview';
  if (writerProvider === 'grok') return process.env.GROK_MODEL || 'grok-4';
  return process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6';
}

async function callAnthropicForDraft(candidate) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    throw new Error('Missing ANTHROPIC_API_KEY');
  }

  const model = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6';
  const maxOutputTokens = getModelMaxOutputTokens();
  const prompt = buildDraftPrompt(candidate, 'anthropic');

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
    title: cleanText(parsed.title),
    description: truncate(cleanText(parsed.description), 800),
    content: cleanText(parsed.content),
    section: normalizeSection(candidate.section),
    model,
    inputTokens,
    outputTokens,
    totalTokens
  };
}

async function callOpenAiForDraft(candidate) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error('Missing OPENAI_API_KEY');
  }

  const model = process.env.OPENAI_MODEL || 'gpt-5';
  const maxOutputTokens = getModelMaxOutputTokens();
  const prompt = buildDraftPrompt(candidate, 'openai');
  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model,
      temperature: 0.4,
      max_completion_tokens: maxOutputTokens,
      response_format: { type: 'json_object' },
      messages: [{ role: 'user', content: prompt }]
    })
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`OpenAI API error ${response.status}: ${body.slice(0, 200)}`);
  }

  const data = await response.json();
  const text = data?.choices?.[0]?.message?.content || '';
  const usage = data?.usage || {};
  const inputTokens = Number(usage.prompt_tokens || 0);
  const outputTokens = Number(usage.completion_tokens || 0);
  const totalTokens = Number(usage.total_tokens || (inputTokens + outputTokens));
  const parsed = safeJsonParse(text);
  if (!parsed) {
    throw new Error('Model did not return valid JSON');
  }

  return {
    title: cleanText(parsed.title),
    description: truncate(cleanText(parsed.description), 800),
    content: cleanText(parsed.content),
    section: normalizeSection(candidate.section),
    model,
    inputTokens,
    outputTokens,
    totalTokens
  };
}

async function callGeminiForDraft(candidate) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) {
    throw new Error('Missing GEMINI_API_KEY');
  }

  const model = process.env.GEMINI_MODEL || 'gemini-3-pro-preview';
  const maxOutputTokens = getModelMaxOutputTokens();
  const prompt = buildDraftPrompt(candidate, 'gemini');
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0.4,
        maxOutputTokens,
        responseMimeType: 'application/json'
      }
    })
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Gemini API error ${response.status}: ${body.slice(0, 200)}`);
  }

  const data = await response.json();
  const text = data?.candidates?.[0]?.content?.parts?.map((part) => part?.text || '').join('') || '';
  const usage = data?.usageMetadata || {};
  const inputTokens = Number(usage.promptTokenCount || 0);
  const outputTokens = Number(usage.candidatesTokenCount || 0);
  const totalTokens = Number(usage.totalTokenCount || (inputTokens + outputTokens));
  const parsed = safeJsonParse(text);
  if (!parsed) {
    const finishReason = String(data?.candidates?.[0]?.finishReason || '').trim();
    const preview = String(text || '').slice(0, 180);
    const suffix = finishReason ? ` (finishReason=${finishReason})` : '';
    throw new Error(`Model did not return valid JSON${suffix}${preview ? `: ${preview}` : ''}`);
  }

  return {
    title: cleanText(parsed.title),
    description: truncate(cleanText(parsed.description), 800),
    content: cleanText(parsed.content),
    section: normalizeSection(candidate.section),
    model,
    inputTokens,
    outputTokens,
    totalTokens
  };
}

async function callGrokForDraft(candidate) {
  const apiKey = process.env.GROK_API_KEY;
  if (!apiKey) {
    throw new Error('Missing GROK_API_KEY');
  }

  const model = process.env.GROK_MODEL || 'grok-4';
  const maxOutputTokens = getModelMaxOutputTokens();
  const prompt = buildDraftPrompt(candidate, 'grok');
  const response = await fetch('https://api.x.ai/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model,
      temperature: 0.4,
      max_tokens: maxOutputTokens,
      messages: [{ role: 'user', content: prompt }]
    })
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Grok API error ${response.status}: ${body.slice(0, 200)}`);
  }

  const data = await response.json();
  const text = data?.choices?.[0]?.message?.content || '';
  const usage = data?.usage || {};
  const inputTokens = Number(usage.prompt_tokens || 0);
  const outputTokens = Number(usage.completion_tokens || 0);
  const totalTokens = Number(usage.total_tokens || (inputTokens + outputTokens));
  const parsed = safeJsonParse(text);
  if (!parsed) {
    throw new Error('Model did not return valid JSON');
  }

  return {
    title: cleanText(parsed.title),
    description: truncate(cleanText(parsed.description), 800),
    content: cleanText(parsed.content),
    section: normalizeSection(candidate.section),
    model,
    inputTokens,
    outputTokens,
    totalTokens
  };
}

async function callWriterForDraft(candidate, writerProvider) {
  if (writerProvider === 'openai') {
    return callOpenAiForDraft(candidate);
  }
  if (writerProvider === 'gemini') {
    return callGeminiForDraft(candidate);
  }
  if (writerProvider === 'grok') {
    return callGrokForDraft(candidate);
  }
  return callAnthropicForDraft(candidate);
}

async function buildDraftWithMinWords(candidate, writerProvider) {
  let draft = await callWriterForDraft(candidate, writerProvider);
  let words = countWords(draft.content);
  if (words >= MIN_ARTICLE_WORDS) return { draft, words };

  // Retry once with stricter length guidance.
  draft = await callWriterForDraft({
    ...candidate,
    title: `${candidate.title} (expand with more verified context and impact details)`
  }, writerProvider);
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
  if (text.includes('blue jackets') || text.includes('columbus blue jackets')) score += 3;
  if (text.includes('cyclones') || text.includes('cincinnati cyclones')) score += 3;
  if (text.includes('dayton bombers') || (text.includes('bombers') && text.includes('hockey'))) score += 3;
  if (text.includes('flyers') && text.includes('club') && text.includes('hockey')) score += 3;
  if (text.includes('dayton stealth')) score += 3;
  if (text.includes('raha') || text.includes('recreational amateur hockey association')) score += 3;
  if (text.includes('cincinnati open') || text.includes('western & southern open')) score += 4;
  if (text.includes('tennis')) score += 2;
  if (text.includes('pickleball')) score += 2;
  if (text.includes('team usa') && text.includes('olympic')) score += 3;
  if (text.includes('u.s.') && text.includes('olympic')) score += 2;
  if (text.includes('bengals') || text.includes('cincinnati bengals')) score += 3;
  if (text.includes('ohio state') || text.includes('buckeyes')) score += 3;
  if (mode === 'college_basketball' && (text.includes('cavaliers') || text.includes('pacers'))) score += 2;
  if (mode === 'college_basketball' && (text.includes('blue jackets') || text.includes('cyclones') || text.includes('hockey'))) score += 2;
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
  for (const term of SPORTS_HOCKEY_TERMS) {
    if (text.includes(term)) score += 1;
  }
  if (text.includes('blue jackets') || text.includes('cyclones')) score += 1;
  if (text.includes('dayton bombers') || (text.includes('bombers') && text.includes('hockey'))) score += 2;
  if (text.includes('flyers') && text.includes('club') && text.includes('hockey')) score += 2;
  if (text.includes('dayton stealth')) score += 2;
  if (text.includes('raha') || text.includes('recreational amateur hockey association')) score += 2;
  if (text.includes('miami redhawks') && text.includes('hockey')) score += 2;
  if (text.includes('ohio state') && text.includes('hockey')) score += 2;
  if (text.includes('bowling green') && text.includes('hockey')) score += 2;
  for (const term of SPORTS_US_OLYMPICS_TERMS) {
    if (text.includes(term)) score += 1;
  }
  for (const term of SPORTS_RACKET_TERMS) {
    if (text.includes(term)) score += 1;
  }
  if (text.includes('university of dayton') && text.includes('tennis')) score += 3;
  if (text.includes('dayton flyers') && text.includes('tennis')) score += 3;
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

function scoreLocalCandidate(candidate) {
  const text = `${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`.toLowerCase();
  let score = 0;

  for (const term of LOCAL_CITY_TERMS) {
    if (text.includes(term)) score += 1;
  }
  for (const term of LOCAL_INSTITUTION_TERMS) {
    if (text.includes(term)) score += 3;
  }
  if (text.includes('daytondailynews.com')) score += 2;
  if (text.includes('wdtn.com')) score += 2;
  if (text.includes('wright-patterson') || text.includes('wright patt')) score += 3;

  return score;
}

function scoreHealthCandidate(candidate) {
  const text = `${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`.toLowerCase();
  let score = 0;

  for (const term of HEALTH_INSTITUTION_TERMS) {
    if (text.includes(term)) score += 3;
  }
  if (text.includes('dayton children')) score += 2;
  if (text.includes('kettering health') || text.includes('premier health')) score += 2;
  if (text.includes('dayton va') || text.includes('veterans affairs')) score += 2;
  if (text.includes('public health') || text.includes('health department')) score += 1;

  return score;
}

function detectNationalStates(text) {
  const lower = String(text || '').toLowerCase();
  const states = [];
  for (const state of NATIONAL_NON_OHIO_STATE_TERMS) {
    if (lower.includes(state)) states.push(state);
  }
  if (lower.includes(NATIONAL_EXCLUDED_STATE)) states.push(NATIONAL_EXCLUDED_STATE);
  return Array.from(new Set(states));
}

function scoreNationalCandidate(candidate) {
  const text = `${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`.toLowerCase();
  let score = 0;

  const states = detectNationalStates(text);
  for (const state of states) {
    if (state === NATIONAL_EXCLUDED_STATE) continue;
    score += 2;
  }

  if (text.includes('public safety') || text.includes('court') || text.includes('policy') || text.includes('economy')) {
    score += 1;
  }
  for (const term of NATIONAL_LOW_PRIORITY_POLITICS_TERMS) {
    if (text.includes(term)) score -= 1;
  }
  if (states.includes(NATIONAL_EXCLUDED_STATE) && states.length === 1) {
    score -= 3;
  }

  return score;
}

function detectWorldRegions(text) {
  const lower = String(text || '').toLowerCase();
  const regions = [];
  for (const [region, terms] of Object.entries(WORLD_REGION_TERMS)) {
    if (terms.some((term) => lower.includes(term))) {
      regions.push(region);
    }
  }
  return Array.from(new Set(regions));
}

function isUsDiplomacyTopic(text) {
  const lower = String(text || '').toLowerCase();
  return WORLD_US_DIPLOMACY_TERMS.some((term) => lower.includes(term));
}

function isWorldUsCentricTopic(text) {
  const lower = String(text || '').toLowerCase();
  if (isUsDiplomacyTopic(lower)) return true;
  return WORLD_US_CENTRIC_TERMS.some((term) => lower.includes(term));
}

function scoreWorldCandidate(candidate) {
  const text = `${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`.toLowerCase();
  const regions = detectWorldRegions(text);
  let score = 0;

  for (const region of regions) {
    if (WORLD_REQUIRED_DAILY_REGIONS.includes(region)) score += 4;
    else if (WORLD_SPREAD_REGIONS.includes(region)) score += 2;
  }
  if (isUsDiplomacyTopic(text)) score -= 2;
  if (regions.includes('middle_east')) score -= 1;
  if (regions.length === 0) score -= 3;

  return score;
}

function isBusinessMarketUpdateTopic(text) {
  const lower = String(text || '').toLowerCase();
  return BUSINESS_MARKET_UPDATE_TERMS.some((term) => lower.includes(term));
}

function isBusinessSmallCapNoise(text) {
  const lower = String(text || '').toLowerCase();
  return BUSINESS_SMALL_CAP_NOISE_TERMS.some((term) => lower.includes(term));
}

function isBusinessLocalTopic(text) {
  const lower = String(text || '').toLowerCase();
  const hasLocal = LOCAL_CITY_TERMS.some((term) => lower.includes(term));
  const hasLocalInstitution = LOCAL_INSTITUTION_TERMS.some((term) => lower.includes(term));
  return hasLocal || hasLocalInstitution;
}

function scoreBusinessCandidate(candidate) {
  const text = `${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`.toLowerCase();
  let score = 0;

  if (candidate.businessMode === 'daily_market_update') score += 100;
  if (isBusinessLocalTopic(text)) score += 4;
  if (BUSINESS_EARNINGS_TERMS.some((term) => text.includes(term))) score += 3;
  if (BUSINESS_LARGE_CAP_TERMS.some((term) => text.includes(term))) score += 2;
  if (isBusinessMarketUpdateTopic(text)) score += 2;
  if (text.includes('cnbc.com')) score += 2;
  if (text.includes('finance.yahoo.com')) score += 2;
  if (isBusinessSmallCapNoise(text)) score -= 6;

  return score;
}

function scoreTechnologyCandidate(candidate) {
  const text = `${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`.toLowerCase();
  let score = 0;

  for (const term of TECHNOLOGY_LOCAL_TERMS) {
    if (text.includes(term)) score += 2;
  }
  if (text.includes('dayton') || text.includes('miami valley')) score += 2;
  if (text.includes('startup') || text.includes('venture capital')) score += 1;
  if (text.includes('government contract') || text.includes('sbir')) score += 1;

  return score;
}

function getEntertainmentGeoTier(text) {
  const lower = String(text || '').toLowerCase();
  if (lower.includes('dayton') || lower.includes('miami valley')) return 4;
  if (ENTERTAINMENT_SURROUNDING_CITY_TERMS.some((term) => lower.includes(term))) return 3;
  if (lower.includes('cincinnati')) return 2;
  if (lower.includes('columbus')) return 1;
  return 0;
}

function scoreEntertainmentCandidate(candidate) {
  const text = `${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`.toLowerCase();
  let score = getEntertainmentGeoTier(text) * 10;
  for (const term of ENTERTAINMENT_EVENT_TERMS) {
    if (text.includes(term)) score += 1;
  }
  if (text.includes('things to do')) score += 2;
  return score;
}

function isLocalSportsNoise(candidate) {
  if (candidate.section !== 'local') return false;
  const text = `${candidate.title || ''} ${candidate.snippet || ''}`.toLowerCase();
  const hasUniversityContext =
    text.includes('university of dayton') ||
    text.includes('dayton flyers') ||
    text.includes('wright state') ||
    text.includes('raiders') ||
    text.includes('miami university') ||
    text.includes('miami redhawks');
  const hasSportsTerm = LOCAL_SPORTS_NOISE_TERMS.some((term) => text.includes(term));
  return hasUniversityContext && hasSportsTerm;
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

async function fetchCandidates(runTargets, activeSections, sportsFocusMode, etTime) {
  const out = [];
  const seen = new Set();
  const activeSet = new Set(activeSections);
  const feedsBySection = getFeedsBySection(sportsFocusMode);
  const etDateKey = getEtDateKey(getNowInEtParts());

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
    } else if (section === 'local') {
      sectionCandidates.sort((a, b) => scoreLocalCandidate(b) - scoreLocalCandidate(a));
    } else if (section === 'national') {
      sectionCandidates.sort((a, b) => scoreNationalCandidate(b) - scoreNationalCandidate(a));
    } else if (section === 'world') {
      sectionCandidates.sort((a, b) => scoreWorldCandidate(b) - scoreWorldCandidate(a));
    } else if (section === 'health') {
      sectionCandidates.sort((a, b) => scoreHealthCandidate(b) - scoreHealthCandidate(a));
    } else if (section === 'technology') {
      sectionCandidates.sort((a, b) => scoreTechnologyCandidate(b) - scoreTechnologyCandidate(a));
    } else if (section === 'entertainment') {
      sectionCandidates.sort((a, b) => scoreEntertainmentCandidate(b) - scoreEntertainmentCandidate(a));
    } else if (section === 'business') {
      // Ensure a dependable daily market outlook candidate appears in early AM ET slots.
      if (BUSINESS_MARKET_UPDATE_SLOTS_ET.includes(etTime)) {
        sectionCandidates.unshift({
          section: 'business',
          title: `U.S. Markets Daily Update (${etDateKey}): Multi-Asset Context Before Futures`,
          url: `internal://business-daily-market-update/${etDateKey}`,
          snippet: 'Assess prior-day patterns in stocks, major indexes, bond market, commodities, and the U.S. dollar across 5-day, 30-day, 3-month, 6-month, and 1-year contexts. No recommendations.',
          sourcePublishedAt: null,
          sportsFocusMode,
          businessMode: 'daily_market_update'
        });
      }
      sectionCandidates.sort((a, b) => scoreBusinessCandidate(b) - scoreBusinessCandidate(a));
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

function normalizeComparableTitle(title) {
  return String(title || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function isSourceHeadlineCopy(draftTitle, sourceTitle) {
  const draftNorm = normalizeComparableTitle(draftTitle);
  const sourceNorm = normalizeComparableTitle(sourceTitle);
  if (!draftNorm || !sourceNorm) return false;
  if (draftNorm === sourceNorm) return true;
  if (draftNorm.includes(sourceNorm) || sourceNorm.includes(draftNorm)) return true;

  const draftTokenList = normalizeTitleForCompare(draftNorm);
  const sourceTokenList = normalizeTitleForCompare(sourceNorm);
  const draftTokens = new Set(draftTokenList);
  const sourceTokens = new Set(sourceTokenList);
  const draftBigrams = new Set(buildBigrams(draftTokenList));
  const sourceBigrams = new Set(buildBigrams(sourceTokenList));
  const tokenJaccard = jaccardSimilarity(draftTokens, sourceTokens);
  const bigramJaccard = jaccardSimilarity(draftBigrams, sourceBigrams);
  return tokenJaccard >= 0.8 || bigramJaccard >= 0.7;
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
  'dayton-dutch-lions': ['dayton dutch lions', 'dutch lions'],
  'dayton-bombers': ['dayton bombers', 'bombers'],
  'dayton-flyers-club-hockey': ['dayton flyers club hockey', 'flyers club hockey'],
  'dayton-stealth': ['dayton stealth'],
  'raha-dayton': ['raha', 'recreational amateur hockey association'],
  'cincinnati-open': ['cincinnati open', 'western & southern open'],
  'dayton-flyers-tennis': ['dayton flyers tennis', 'university of dayton tennis', 'dayton tennis'],
  'dayton-pickleball': ['dayton pickleball', 'miami valley pickleball', 'pickleball'],
  'columbus-blue-jackets': ['columbus blue jackets', 'blue jackets'],
  'cincinnati-cyclones': ['cincinnati cyclones', 'cyclones'],
  'miami-redhawks-hockey': ['miami redhawks hockey', 'miami hockey'],
  'ohio-state-buckeyes-hockey': ['ohio state hockey', 'buckeyes hockey'],
  'bowling-green-falcons-hockey': ['bowling green hockey', 'falcons hockey']
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

function buildGeneralEventKeys(text) {
  const tokens = normalizeTitleForCompare(text);
  if (!tokens.length) return [];
  const bigrams = buildBigrams(tokens).slice(0, 8);
  const keys = new Set();
  for (const bg of bigrams) {
    keys.add(`bg:${bg}`);
  }
  return Array.from(keys);
}

function buildSectionEventKeys(section, text) {
  if (normalizeSection(section) === 'sports') {
    return buildSportsEventKeys(text);
  }
  return buildGeneralEventKeys(text);
}

function hasAnyKeyIntersection(candidateKeys, existingKeySet) {
  for (const key of candidateKeys) {
    if (existingKeySet.has(key)) return true;
  }
  return false;
}

function countKeyIntersection(candidateKeys, existingKeySet) {
  let count = 0;
  for (const key of candidateKeys) {
    if (existingKeySet.has(key)) count += 1;
  }
  return count;
}

function isLikelyPoliticalTopic(text) {
  const lower = String(text || '').toLowerCase();
  return POLITICAL_TOPIC_TERMS.some((term) => lower.includes(term));
}

function isOpinionStyleContent(text) {
  const lower = String(text || '').toLowerCase();
  return OPINION_STYLE_TERMS.some((term) => lower.includes(term));
}

function isTennisTopic(text) {
  const lower = String(text || '').toLowerCase();
  return SPORTS_TENNIS_TERMS.some((term) => lower.includes(term));
}

function isSportsTopic(text) {
  const lower = String(text || '').toLowerCase();
  if (detectSportsTeams(lower).length > 0) return true;
  return LOCAL_SPORTS_NOISE_TERMS.some((term) => lower.includes(term));
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
    if (tokenJaccard >= 0.6 || bigramJaccard >= 0.45 || (overlap >= 5 && overlapByMin >= 0.8)) {
      return true;
    }
  }
  return false;
}

async function alreadyExists(sql, candidate) {
  const slug = String(generateSlug(candidate.title) || "").trim();
  const rows = await sql`
    SELECT EXISTS(
      SELECT 1 FROM articles
      WHERE (
          ${slug} <> ''
          AND lower(trim(slug)) = lower(trim(${slug}))
        )
         OR lower(title) = lower(${candidate.title})
      UNION ALL
      SELECT 1 FROM article_drafts
      WHERE source_url = ${candidate.url}
         OR lower(title) = lower(${candidate.title})
    ) AS "exists"
  `;
  return !!rows?.[0]?.exists;
}

function stableSlugSuffix(seed) {
  const text = String(seed || 'draft').trim().toLowerCase();
  let hash = 0;
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  }
  return Math.abs(hash || 1).toString(36);
}

function ensureNonEmptyDraftSlug(draftTitle, candidate) {
  const primary = String(generateSlug(draftTitle) || '').trim();
  if (primary) return primary;
  const sourceTitle = String(generateSlug(candidate?.title || '') || '').trim();
  if (sourceTitle) return sourceTitle;
  const seed = `${candidate?.url || ''}|${candidate?.title || ''}|${candidate?.section || ''}`;
  return `draft-${stableSlugSuffix(seed)}`;
}

async function ensureDuplicateReportsTable() {}
async function ensureEditorialRejectionsTable() {}
async function ensureModelTrackingReset() {}

async function ensureDraftGenerationRunsTable(sql) {
  const tableRows = await sql`SELECT to_regclass('public.draft_generation_runs') AS name`;
  if (!tableRows[0]?.name) {
    const error = new Error('Schema not ready: missing draft_generation_runs. Apply migration 20260309_26.');
    error.statusCode = 503;
    throw error;
  }
  const columnRows = await sql`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'draft_generation_runs'
  `;
  const existingColumns = new Set(columnRows.map((row) => String(row.column_name || '')));
  const requiredColumns = ['writer_provider', 'writer_model', 'top_skip_reasons', 'run_status', 'run_at'];
  const missingColumns = requiredColumns.filter((columnName) => !existingColumns.has(columnName));
  if (missingColumns.length) {
    const error = new Error(`Schema not ready: missing draft_generation_runs columns: ${missingColumns.join(', ')}`);
    error.statusCode = 503;
    throw error;
  }
}

function getTopSkipReasons(skipped, limit = 10) {
  const counts = new Map();
  for (const row of skipped || []) {
    const reason = String(row?.reason || 'unknown');
    counts.set(reason, (counts.get(reason) || 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([reason, count]) => ({ reason, count }));
}

async function logDraftGenerationRun(sql, payload) {
  if (!sql) return;
  const topSkipReasons = JSON.stringify(payload.topSkipReasons || []);
  await sql`
    INSERT INTO draft_generation_runs (
      run_status,
      run_reason,
      schedule_mode,
      track,
      run_mode,
      created_via,
      dry_run,
      include_sections,
      exclude_sections,
      active_sections,
      et_date,
      et_time,
      requested_count,
      target_count,
      created_count,
      skipped_count,
      daily_token_budget,
      tokens_used_today,
      run_tokens_consumed,
      writer_provider,
      writer_model,
      top_skip_reasons
    )
    VALUES (
      ${payload.runStatus || 'ok'},
      ${payload.runReason || null},
      ${payload.scheduleMode || null},
      ${payload.track || null},
      ${payload.runMode || null},
      ${payload.createdVia || null},
      ${Boolean(payload.dryRun)},
      ${payload.includeSections || null},
      ${payload.excludeSections || null},
      ${payload.activeSections || null},
      ${payload.etDate || null},
      ${payload.etTime || null},
      ${Number.isFinite(payload.requestedCount) ? payload.requestedCount : null},
      ${Number.isFinite(payload.targetCount) ? payload.targetCount : null},
      ${Number.isFinite(payload.createdCount) ? payload.createdCount : 0},
      ${Number.isFinite(payload.skippedCount) ? payload.skippedCount : 0},
      ${Number.isFinite(payload.dailyTokenBudget) ? payload.dailyTokenBudget : null},
      ${Number.isFinite(payload.tokensUsedToday) ? payload.tokensUsedToday : 0},
      ${Number.isFinite(payload.runTokensConsumed) ? payload.runTokensConsumed : 0},
      ${payload.writerProvider || null},
      ${payload.writerModelForRun || null},
      ${topSkipReasons}::jsonb
    )
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (!['POST', 'GET'].includes(String(req.method || '').toUpperCase())) {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  let requestedCount = Math.min(parseInt(req.body?.count || req.query.count || '6', 10), 50);
  const dryRun = String(req.body?.dryRun || req.query.dryRun || 'false') === 'true';
  const dailyTokenBudgetOverride = req.body?.dailyTokenBudget || req.query.dailyTokenBudget;
  const scheduleMode = String(req.body?.schedule || req.query.schedule || '').toLowerCase();
  const requestedRunMode = String(req.body?.runMode || req.query.runMode || 'manual').toLowerCase();
  const writerProvider = resolveWriterProvider(req.body?.provider || req.query.provider);
  const writerModelForRun = getWriterModelForProvider(writerProvider);
  const memorySuppressionEnabled = isMemorySuppressionEnabled(
    req.body?.memorySuppressionEnabled ?? req.query.memorySuppressionEnabled ?? process.env.DRAFT_MEMORY_SUPPRESSION
  );
  const track = String(req.body?.track || req.query.track || '').toLowerCase();
  const runMode = 'manual';
  const createdVia = 'manual';
  const includeSections = parseSectionList(req.body?.includeSections || req.query.includeSections);
  const excludeSections = parseSectionList(req.body?.excludeSections || req.query.excludeSections);
  const activeSections = resolveActiveSections(includeSections, excludeSections);

  if (!activeSections.length) {
    return res.status(400).json({ error: 'No active sections after include/exclude filters' });
  }

  if (scheduleMode === 'auto') {
    return res.status(400).json({ error: 'Automatic scheduled runs are disabled. Use manual mode only.' });
  }

  if (requestedRunMode && requestedRunMode !== 'manual') {
    return res.status(400).json({ error: 'Only runMode=manual is supported.' });
  }

  let sql = null;
  try {
    sql = neon(process.env.DATABASE_URL);
    await ensureDuplicateReportsTable(sql);
    await ensureEditorialRejectionsTable(sql);
    await ensureModelTrackingReset(sql);
    await ensureDraftGenerationRunsTable(sql);
    let dailyTokenBudget = 0;
    if (dailyTokenBudgetOverride) {
      dailyTokenBudget = Math.max(1, Math.min(parseInt(String(dailyTokenBudgetOverride), 10), 1000000));
    } else {
      const budgets = await getDailyTokenBudgets(sql);
      dailyTokenBudget = budgets.manual;
    }
    const etNowParts = getNowInEtParts();
    const sportsFocusMode = resolveSportsFocusMode(req.body?.sportsFocusMode || req.query.sportsFocusMode, etNowParts);
    const etTime = getEtTimeKey(etNowParts);
    const etDate = getEtDateKey(etNowParts);

    if (scheduleMode === 'auto') {
      if (!['multi', 'single'].includes(track)) {
        await logDraftGenerationRun(sql, {
          runStatus: 'invalid_request',
          runReason: 'schedule=auto requires track=multi|single',
          scheduleMode,
          track,
          runMode,
          createdVia,
          dryRun,
          includeSections: includeSections?.join(',') || '',
          excludeSections: excludeSections?.join(',') || '',
          activeSections: activeSections.join(','),
          etDate,
          etTime,
          requestedCount,
          writerProvider,
          writerModelForRun
        });
        return res.status(400).json({ error: 'schedule=auto requires track=multi|single' });
      }
      if (!shouldRunScheduledTrack(track, etTime, writerProvider)) {
        await logDraftGenerationRun(sql, {
          runStatus: 'skipped',
          runReason: `Not a scheduled ${track} slot`,
          scheduleMode,
          track,
          runMode,
          createdVia,
          dryRun,
          includeSections: includeSections?.join(',') || '',
          excludeSections: excludeSections?.join(',') || '',
          activeSections: activeSections.join(','),
          etDate,
          etTime,
          requestedCount,
          writerProvider,
          writerModelForRun
        });
        return res.status(200).json({
          ok: true,
          skipped: true,
          reason: `Not a scheduled ${track} slot`,
          etDate,
          etTime,
          track,
          writerProvider,
          writerModelForRun
        });
      }
      const scheduledCount = getScheduledRequestedCount(track, etTime, writerProvider);
      if (scheduledCount && scheduledCount > 0) {
        requestedCount = Math.min(scheduledCount, 50);
      }
    }

    const todayBySection = await sql`
      SELECT section, COUNT(*)::int AS "count"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date = ${etDate}
        AND created_via = 'auto'
      GROUP BY section
    `;

    const todayTokensRows = await sql`
      SELECT COALESCE(SUM(total_tokens), 0)::int AS "tokens"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date = ${etDate}
        AND created_via = 'auto'
    `;
    const tokensUsedToday = todayTokensRows?.[0]?.tokens || 0;
    if (!dryRun && runMode === 'auto' && tokensUsedToday >= dailyTokenBudget) {
      await logDraftGenerationRun(sql, {
        runStatus: 'skipped',
        runReason: 'Daily token budget reached',
        scheduleMode,
        track,
        runMode,
        createdVia,
        dryRun,
        includeSections: includeSections?.join(',') || '',
        excludeSections: excludeSections?.join(',') || '',
        activeSections: activeSections.join(','),
        etDate,
        etTime,
        requestedCount,
        dailyTokenBudget,
        tokensUsedToday,
        writerProvider,
        writerModelForRun
      });
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

    let effectiveRequestedCount = requestedCount;
    if (
      !dryRun &&
      runMode === 'auto' &&
      scheduleMode === 'auto' &&
      ['multi', 'single'].includes(track) &&
      shouldApplyCatchUpForEtTime(etTime)
    ) {
      const slotsLeft = getRemainingScheduledSlots(track, etTime, writerProvider);
      if (slotsLeft > 0) {
        const catchUpCount = Math.ceil(remainingToday / slotsLeft);
        if (Number.isFinite(catchUpCount) && catchUpCount > 0) {
          effectiveRequestedCount = Math.min(50, Math.max(requestedCount, catchUpCount));
        }
      }
    }

    const targetCount = dryRun
      ? effectiveRequestedCount
      : (runMode === 'manual' ? effectiveRequestedCount : Math.min(effectiveRequestedCount, remainingToday));

    if (targetCount <= 0) {
      await logDraftGenerationRun(sql, {
        runStatus: 'skipped',
        runReason: 'Daily section quotas reached',
        scheduleMode,
        track,
        runMode,
        createdVia,
        dryRun,
        includeSections: includeSections?.join(',') || '',
        excludeSections: excludeSections?.join(',') || '',
        activeSections: activeSections.join(','),
        etDate,
        etTime,
        requestedCount: effectiveRequestedCount,
        targetCount,
        dailyTokenBudget,
        tokensUsedToday,
        writerProvider,
        writerModelForRun
      });
      return res.status(200).json({
        ok: true,
        dryRun,
        requested: effectiveRequestedCount,
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

    const manualRunRemainingBySection = {};
    for (const section of SECTION_ORDER) {
      manualRunRemainingBySection[section] = activeSections.includes(section) ? requestedCount : 0;
    }
    const runTargets = buildRunTargets(
      runMode === 'manual' ? manualRunRemainingBySection : remainingBySection,
      targetCount,
      activeSections
    );
    const candidates = await fetchCandidates(runTargets, activeSections, sportsFocusMode, etTime);
    const created = [];
    const skipped = [];
    let runTokensConsumed = 0;
    const tennisDailyCap = 1;
    const createdBySection = {};
    for (const section of SECTION_ORDER) createdBySection[section] = 0;
    const todaySportsTopicRows = await sql`
      SELECT title, description, content, source_title as "sourceTitle"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date = ${etDate}
        AND created_via = 'auto'
        AND section = 'sports'
    `;
    const tennisCreatedToday = todaySportsTopicRows.filter((row) =>
      isTennisTopic(`${row.title || ''} ${row.description || ''} ${row.content || ''} ${row.sourceTitle || ''}`)
    ).length;
    let tennisCreatedThisRun = 0;
    const todayBusinessTopicRows = await sql`
      SELECT title, description, content, source_title as "sourceTitle", source_url as "sourceUrl"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date = ${etDate}
        AND created_via = 'auto'
        AND section = 'business'
    `;
    const businessLocalCreatedToday = todayBusinessTopicRows.filter((row) =>
      isBusinessLocalTopic(`${row.title || ''} ${row.description || ''} ${row.content || ''} ${row.sourceTitle || ''}`)
    ).length;
    const businessMarketUpdateCreatedToday = todayBusinessTopicRows.filter((row) =>
      isBusinessMarketUpdateTopic(`${row.title || ''} ${row.description || ''} ${row.content || ''} ${row.sourceTitle || ''} ${row.sourceUrl || ''}`)
    ).length;
    let businessLocalCreatedThisRun = 0;
    let businessMarketUpdateCreatedThisRun = 0;
    const worldRegionDailyCap = 2;
    const worldUsDiplomacyDailyCap = 2;
    const worldMiddleEastDailyCap = 2;
    const todayWorldTopicRows = await sql`
      SELECT title, description, content, source_title as "sourceTitle"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date = ${etDate}
        AND created_via = 'auto'
        AND section = 'world'
    `;
    const worldRegionCountsToday = {};
    for (const region of WORLD_SPREAD_REGIONS) worldRegionCountsToday[region] = 0;
    let worldUsDiplomacyToday = 0;
    let worldMiddleEastToday = 0;
    for (const row of todayWorldTopicRows) {
      const text = `${row.title || ''} ${row.description || ''} ${row.content || ''} ${row.sourceTitle || ''}`;
      const regions = detectWorldRegions(text);
      for (const region of regions) {
        if (worldRegionCountsToday[region] !== undefined) {
          worldRegionCountsToday[region] += 1;
        }
      }
      if (regions.includes('middle_east')) worldMiddleEastToday += 1;
      if (isUsDiplomacyTopic(text)) worldUsDiplomacyToday += 1;
    }
    const worldRegionCountsThisRun = {};
    for (const region of WORLD_SPREAD_REGIONS) worldRegionCountsThisRun[region] = 0;
    let worldUsDiplomacyThisRun = 0;
    let worldMiddleEastThisRun = 0;
    const nationalStateDailyCap = 2;
    const todayNationalTopicRows = await sql`
      SELECT title, description, content, source_title as "sourceTitle"
      FROM article_drafts
      WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE ${ET_TIME_ZONE})::date = ${etDate}
        AND created_via = 'auto'
        AND section = 'national'
    `;
    const nationalStatesUsedToday = new Set();
    for (const row of todayNationalTopicRows) {
      const states = detectNationalStates(`${row.title || ''} ${row.description || ''} ${row.content || ''} ${row.sourceTitle || ''}`);
      for (const state of states) {
        if (state !== NATIONAL_EXCLUDED_STATE) nationalStatesUsedToday.add(state);
      }
    }
    const nationalStatesUsedThisRun = new Set();
    const reportedDuplicateRows = [];
    const reportedDuplicateTitles = Array.from(
      new Set(
        reportedDuplicateRows
          .flatMap((row) => [row.draftTitle, row.sourceTitle])
          .map((title) => cleanText(title))
          .filter(Boolean)
      )
    );
    const reportedDuplicateNormalizedTitles = new Set(
      reportedDuplicateTitles.map((title) => normalizeComparableTitle(title)).filter(Boolean)
    );
    const reportedDuplicateSourceUrls = new Set(
      reportedDuplicateRows.map((row) => cleanText(row.sourceUrl)).filter(Boolean)
    );
    const reportedExternalDuplicateRows = reportedDuplicateRows.filter(
      (row) => String(row.duplicateType || 'internal').toLowerCase() === 'external'
    );
    const reportedExternalDuplicateTitles = Array.from(
      new Set(
        reportedExternalDuplicateRows
          .flatMap((row) => [row.draftTitle, row.sourceTitle])
          .map((title) => cleanText(title))
          .filter(Boolean)
      )
    );
    const reportedExternalDuplicateNormalizedTitles = new Set(
      reportedExternalDuplicateTitles.map((title) => normalizeComparableTitle(title)).filter(Boolean)
    );
    const reportedExternalDuplicateSourceUrls = new Set(
      reportedExternalDuplicateRows.map((row) => cleanText(row.sourceUrl)).filter(Boolean)
    );
    const reportedExternalDuplicateTitlesBySection = {};
    for (const section of SECTION_ORDER) reportedExternalDuplicateTitlesBySection[section] = [];
    for (const row of reportedExternalDuplicateRows) {
      const section = normalizeSection(row.section) || 'local';
      const titles = [row.draftTitle, row.sourceTitle].map((v) => cleanText(v)).filter(Boolean);
      if (titles.length) {
        reportedExternalDuplicateTitlesBySection[section].push(...titles);
      }
    }
    for (const section of SECTION_ORDER) {
      reportedExternalDuplicateTitlesBySection[section] = Array.from(
        new Set(reportedExternalDuplicateTitlesBySection[section])
      );
    }
    const editorialRejectRows = [];
    const editorialRejectedTitlesBySection = {};
    for (const section of SECTION_ORDER) editorialRejectedTitlesBySection[section] = [];
    const editorialRejectedSourceUrls = new Set();
    for (const row of editorialRejectRows) {
      const section = normalizeSection(row.section) || 'local';
      const reason = String(row.rejectReason || '').trim();
      if (!['stale_or_not_time_relevant', 'low_newsworthiness_or_thin', 'style_mismatch'].includes(reason)) {
        continue;
      }
      const titles = [row.draftTitle, row.sourceTitle].map((v) => cleanText(v)).filter(Boolean);
      if (titles.length) {
        editorialRejectedTitlesBySection[section].push(...titles);
      }
      const sourceUrl = cleanText(row.sourceUrl);
      if (sourceUrl) editorialRejectedSourceUrls.add(sourceUrl);
    }
    for (const section of SECTION_ORDER) {
      editorialRejectedTitlesBySection[section] = Array.from(new Set(editorialRejectedTitlesBySection[section]));
    }

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
        [
          ...existingTitleRows.map((r) => cleanText(r.compare_text)),
          ...reportedDuplicateTitles
        ]
          .map((t) => cleanText(t))
          .filter(Boolean)
      )
    );
    const existingEventKeysBySection = {};
    for (const section of SECTION_ORDER) {
      existingEventKeysBySection[section] = new Set();
    }
    for (const row of existingTitleRows) {
      const section = normalizeSection(row.section) || 'local';
      if (!existingEventKeysBySection[section]) {
        existingEventKeysBySection[section] = new Set();
      }
      const compareText = cleanText(row.compare_text);
      if (!compareText) continue;
      for (const key of buildSectionEventKeys(section, compareText)) {
        existingEventKeysBySection[section].add(key);
      }
    }
    const runTitles = [];
    const runEventKeysBySection = {};
    for (const section of SECTION_ORDER) {
      runEventKeysBySection[section] = new Set();
    }

    for (const candidate of candidates) {
      if (!dryRun && runMode === 'auto' && (tokensUsedToday + runTokensConsumed) >= dailyTokenBudget) {
        skipped.push({ reason: 'daily_token_budget_reached', title: candidate.title, url: candidate.url });
        break;
      }
      if (created.length >= targetCount) break;
      if (createdBySection[candidate.section] >= (runTargets[candidate.section] || 0)) {
        continue;
      }
      if (candidate.section === 'national') {
        if (isSportsTopic(`${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`)) {
          skipped.push({ reason: 'national_sports_topic_filtered', title: candidate.title, url: candidate.url });
          continue;
        }
        const candidateStates = detectNationalStates(`${candidate.title || ''} ${candidate.snippet || ''}`);
        const nonOhioStates = candidateStates.filter((s) => s !== NATIONAL_EXCLUDED_STATE);
        if (!nonOhioStates.length) {
          skipped.push({ reason: 'national_state_outside_ohio_required', title: candidate.title, url: candidate.url });
          continue;
        }
        const hasAvailableState = nonOhioStates.some((state) => {
          const usedCount = (nationalStatesUsedToday.has(state) ? 1 : 0) + (nationalStatesUsedThisRun.has(state) ? 1 : 0);
          return usedCount < nationalStateDailyCap;
        });
        if (!hasAvailableState) {
          skipped.push({ reason: 'national_state_daily_cap_reached', title: candidate.title, url: candidate.url });
          continue;
        }
      }
      if (candidate.section === 'world') {
        const candidateText = `${candidate.title || ''} ${candidate.snippet || ''}`;
        if (isWorldUsCentricTopic(candidateText)) {
          skipped.push({ reason: 'world_us_centric_filtered', title: candidate.title, url: candidate.url });
          continue;
        }
        const candidateRegions = detectWorldRegions(candidateText);
        if (!candidateRegions.length) {
          skipped.push({ reason: 'world_region_unclassified', title: candidate.title, url: candidate.url });
          continue;
        }
        const missingRequiredRegions = WORLD_REQUIRED_DAILY_REGIONS.filter((region) =>
          ((worldRegionCountsToday[region] || 0) + (worldRegionCountsThisRun[region] || 0)) < 1
        );
        if (missingRequiredRegions.length && !candidateRegions.some((r) => missingRequiredRegions.includes(r))) {
          skipped.push({ reason: 'world_required_region_pending', title: candidate.title, url: candidate.url });
          continue;
        }
        const hasAvailableRegion = candidateRegions.some((region) => {
          if (!WORLD_SPREAD_REGIONS.includes(region)) return false;
          const used = (worldRegionCountsToday[region] || 0) + (worldRegionCountsThisRun[region] || 0);
          return used < worldRegionDailyCap;
        });
        if (!hasAvailableRegion) {
          skipped.push({ reason: 'world_region_daily_cap_reached', title: candidate.title, url: candidate.url });
          continue;
        }
        if (candidateRegions.includes('middle_east') &&
            (worldMiddleEastToday + worldMiddleEastThisRun) >= worldMiddleEastDailyCap) {
          skipped.push({ reason: 'world_middle_east_daily_cap_reached', title: candidate.title, url: candidate.url });
          continue;
        }
        if (isUsDiplomacyTopic(candidateText) &&
            (worldUsDiplomacyToday + worldUsDiplomacyThisRun) >= worldUsDiplomacyDailyCap) {
          skipped.push({ reason: 'world_us_diplomacy_daily_cap_reached', title: candidate.title, url: candidate.url });
          continue;
        }
      }
      if (candidate.section === 'entertainment') {
        const tier = getEntertainmentGeoTier(`${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`);
        if (tier <= 0) {
          skipped.push({ reason: 'entertainment_geo_outside_scope', title: candidate.title, url: candidate.url });
          continue;
        }
      }
      if (candidate.section === 'business') {
        const candidateText = `${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`;
        const isLocalBusinessCandidate = isBusinessLocalTopic(candidateText);
        const isMarketUpdateCandidate = candidate.businessMode === 'daily_market_update' || isBusinessMarketUpdateTopic(candidateText);
        if (isBusinessSmallCapNoise(candidateText)) {
          skipped.push({ reason: 'business_small_cap_filtered', title: candidate.title, url: candidate.url });
          continue;
        }
        if (
          runMode === 'auto' &&
          (businessMarketUpdateCreatedToday + businessMarketUpdateCreatedThisRun) >= BUSINESS_DAILY_MARKET_UPDATE_MAX &&
          isMarketUpdateCandidate
        ) {
          skipped.push({ reason: 'business_daily_market_update_cap_reached', title: candidate.title, url: candidate.url });
          continue;
        }
        if (
          runMode === 'auto' &&
          (businessLocalCreatedToday + businessLocalCreatedThisRun) < BUSINESS_LOCAL_DAILY_MIN &&
          !isLocalBusinessCandidate &&
          !isMarketUpdateCandidate
        ) {
          skipped.push({ reason: 'business_local_required_pending', title: candidate.title, url: candidate.url });
          continue;
        }
      }
      if (
        candidate.section === 'sports' &&
        isTennisTopic(`${candidate.title || ''} ${candidate.snippet || ''}`) &&
        (tennisCreatedToday + tennisCreatedThisRun) >= tennisDailyCap
      ) {
        skipped.push({ reason: 'daily_tennis_cap_reached', title: candidate.title, url: candidate.url });
        continue;
      }

      const normalizedCandidateTitle = normalizeComparableTitle(candidate.title);
      if (normalizedCandidateTitle && reportedExternalDuplicateNormalizedTitles.has(normalizedCandidateTitle)) {
        skipped.push({ reason: 'external_duplicate_exact_title', title: candidate.title, url: candidate.url });
        continue;
      }
      if (normalizedCandidateTitle && reportedDuplicateNormalizedTitles.has(normalizedCandidateTitle)) {
        skipped.push({ reason: 'reported_duplicate_exact_title', title: candidate.title, url: candidate.url });
        continue;
      }
      if (candidate.url && reportedExternalDuplicateSourceUrls.has(candidate.url)) {
        skipped.push({ reason: 'external_duplicate_source', title: candidate.title, url: candidate.url });
        continue;
      }
      if (candidate.url && reportedDuplicateSourceUrls.has(candidate.url)) {
        skipped.push({ reason: 'reported_duplicate_source', title: candidate.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(candidate.title, reportedExternalDuplicateTitles)) {
        skipped.push({ reason: 'external_duplicate_near_title', title: candidate.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(candidate.title, reportedDuplicateTitles)) {
        skipped.push({ reason: 'reported_duplicate_near_title', title: candidate.title, url: candidate.url });
        continue;
      }
      {
        const section = normalizeSection(candidate.section) || 'local';
        const rejectedTitles = editorialRejectedTitlesBySection[section] || [];
        if (candidate.url && editorialRejectedSourceUrls.has(candidate.url)) {
          skipped.push({ reason: 'editorial_reject_source_history', title: candidate.title, url: candidate.url });
          continue;
        }
        if (rejectedTitles.length && isNearDuplicateTitle(candidate.title, rejectedTitles)) {
          skipped.push({ reason: 'editorial_reject_title_history', title: candidate.title, url: candidate.url });
          continue;
        }
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
      if (isLocalSportsNoise(candidate)) {
        skipped.push({ reason: 'local_sports_noise', title: candidate.title, url: candidate.url });
        continue;
      }
      if (isLikelyPoliticalTopic(`${candidate.title || ''} ${candidate.snippet || ''}`) &&
          isOpinionStyleContent(`${candidate.title || ''} ${candidate.snippet || ''} ${candidate.url || ''}`)) {
        skipped.push({ reason: 'opinion_politics_filtered', title: candidate.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(candidate.title, existingTitles) || isNearDuplicateTitle(candidate.title, runTitles)) {
        skipped.push({ reason: 'near_duplicate_title', title: candidate.title, url: candidate.url });
        continue;
      }
      {
        const section = normalizeSection(candidate.section) || 'local';
        const candidateEventKeys = buildSectionEventKeys(section, `${candidate.title} ${candidate.snippet || ''}`);
        const existingSet = existingEventKeysBySection[section] || new Set();
        const runSet = runEventKeysBySection[section] || new Set();
        if (candidateEventKeys.length) {
          if (section === 'sports') {
            if (hasAnyKeyIntersection(candidateEventKeys, existingSet) || hasAnyKeyIntersection(candidateEventKeys, runSet)) {
              skipped.push({ reason: 'duplicate_sports_event', title: candidate.title, url: candidate.url });
              continue;
            }
          } else {
            const overlap = Math.max(
              countKeyIntersection(candidateEventKeys, existingSet),
              countKeyIntersection(candidateEventKeys, runSet)
            );
            if (overlap >= 2) {
              skipped.push({ reason: 'duplicate_section_event', title: candidate.title, url: candidate.url });
              continue;
            }
          }
        }
      }

      const sectionKey = normalizeSection(candidate.section) || 'local';
      let draft = null;
      let words = 0;
      try {
        const result = await buildDraftWithMinWords({
          ...candidate,
          externalDuplicateTitles: reportedExternalDuplicateTitlesBySection[sectionKey] || []
        }, writerProvider);
        draft = result.draft;
        words = result.words;
      } catch (err) {
        skipped.push({
          reason: 'writer_call_failed',
          title: candidate.title,
          url: candidate.url,
          error: String(err?.message || 'unknown_error').slice(0, 180)
        });
        continue;
      }
      if (!draft.title || !draft.content) {
        skipped.push({ reason: 'rejected_non_local_or_empty', title: candidate.title, url: candidate.url });
        continue;
      }
      if (isSourceHeadlineCopy(draft.title, candidate.title)) {
        skipped.push({ reason: 'source_headline_copy_draft', title: draft.title, url: candidate.url });
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
      const normalizedDraftTitle = normalizeComparableTitle(draft.title);
      if (normalizedDraftTitle && reportedExternalDuplicateNormalizedTitles.has(normalizedDraftTitle)) {
        skipped.push({ reason: 'external_duplicate_exact_draft_title', title: draft.title, url: candidate.url });
        continue;
      }
      if (normalizedDraftTitle && reportedDuplicateNormalizedTitles.has(normalizedDraftTitle)) {
        skipped.push({ reason: 'reported_duplicate_exact_draft_title', title: draft.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(draft.title, reportedExternalDuplicateTitles)) {
        skipped.push({ reason: 'external_duplicate_near_draft_title', title: draft.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(draft.title, reportedDuplicateTitles)) {
        skipped.push({ reason: 'reported_duplicate_near_draft_title', title: draft.title, url: candidate.url });
        continue;
      }
      {
        const section = normalizeSection(draft.section) || 'local';
        const rejectedTitles = editorialRejectedTitlesBySection[section] || [];
        if (rejectedTitles.length && isNearDuplicateTitle(draft.title, rejectedTitles)) {
          skipped.push({ reason: 'editorial_reject_draft_title_history', title: draft.title, url: candidate.url });
          continue;
        }
      }
      if (isLikelyPoliticalTopic(`${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`) &&
          isOpinionStyleContent(`${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`)) {
        skipped.push({ reason: 'opinion_politics_filtered_draft', title: draft.title, url: candidate.url });
        continue;
      }
      if (draft.section === 'local' && isLocalSportsNoise({ ...candidate, title: draft.title, snippet: `${draft.description || ''} ${draft.content || ''}` })) {
        skipped.push({ reason: 'local_sports_noise_draft', title: draft.title, url: candidate.url });
        continue;
      }
      if (draft.section === 'entertainment') {
        const tier = getEntertainmentGeoTier(`${draft.title || ''} ${draft.description || ''} ${draft.content || ''} ${candidate.url || ''}`);
        if (tier <= 0) {
          skipped.push({ reason: 'entertainment_geo_outside_scope_draft', title: draft.title, url: candidate.url });
          continue;
        }
      }
      if (draft.section === 'business') {
        const draftText = `${draft.title || ''} ${draft.description || ''} ${draft.content || ''} ${candidate.url || ''}`;
        const isLocalBusinessDraft = isBusinessLocalTopic(draftText);
        const isMarketUpdateDraft = candidate.businessMode === 'daily_market_update' || isBusinessMarketUpdateTopic(draftText);
        if (isBusinessSmallCapNoise(draftText)) {
          skipped.push({ reason: 'business_small_cap_filtered_draft', title: draft.title, url: candidate.url });
          continue;
        }
        if (
          runMode === 'auto' &&
          (businessMarketUpdateCreatedToday + businessMarketUpdateCreatedThisRun) >= BUSINESS_DAILY_MARKET_UPDATE_MAX &&
          isMarketUpdateDraft
        ) {
          skipped.push({ reason: 'business_daily_market_update_cap_reached_draft', title: draft.title, url: candidate.url });
          continue;
        }
        if (
          runMode === 'auto' &&
          (businessLocalCreatedToday + businessLocalCreatedThisRun) < BUSINESS_LOCAL_DAILY_MIN &&
          !isLocalBusinessDraft &&
          !isMarketUpdateDraft
        ) {
          skipped.push({ reason: 'business_local_required_pending_draft', title: draft.title, url: candidate.url });
          continue;
        }
      }
      if (draft.section === 'world') {
        const draftText = `${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`;
        if (isWorldUsCentricTopic(draftText)) {
          skipped.push({ reason: 'world_us_centric_filtered_draft', title: draft.title, url: candidate.url });
          continue;
        }
        const draftRegions = detectWorldRegions(draftText);
        if (!draftRegions.length) {
          skipped.push({ reason: 'world_region_unclassified_draft', title: draft.title, url: candidate.url });
          continue;
        }
        const missingRequiredRegions = WORLD_REQUIRED_DAILY_REGIONS.filter((region) =>
          ((worldRegionCountsToday[region] || 0) + (worldRegionCountsThisRun[region] || 0)) < 1
        );
        if (missingRequiredRegions.length && !draftRegions.some((r) => missingRequiredRegions.includes(r))) {
          skipped.push({ reason: 'world_required_region_pending_draft', title: draft.title, url: candidate.url });
          continue;
        }
        const hasAvailableRegion = draftRegions.some((region) => {
          if (!WORLD_SPREAD_REGIONS.includes(region)) return false;
          const used = (worldRegionCountsToday[region] || 0) + (worldRegionCountsThisRun[region] || 0);
          return used < worldRegionDailyCap;
        });
        if (!hasAvailableRegion) {
          skipped.push({ reason: 'world_region_daily_cap_reached_draft', title: draft.title, url: candidate.url });
          continue;
        }
        if (draftRegions.includes('middle_east') &&
            (worldMiddleEastToday + worldMiddleEastThisRun) >= worldMiddleEastDailyCap) {
          skipped.push({ reason: 'world_middle_east_daily_cap_reached_draft', title: draft.title, url: candidate.url });
          continue;
        }
        if (isUsDiplomacyTopic(draftText) &&
            (worldUsDiplomacyToday + worldUsDiplomacyThisRun) >= worldUsDiplomacyDailyCap) {
          skipped.push({ reason: 'world_us_diplomacy_daily_cap_reached_draft', title: draft.title, url: candidate.url });
          continue;
        }
      }
      if (
        draft.section === 'sports' &&
        isTennisTopic(`${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`) &&
        (tennisCreatedToday + tennisCreatedThisRun) >= tennisDailyCap
      ) {
        skipped.push({ reason: 'daily_tennis_cap_reached', title: draft.title, url: candidate.url });
        continue;
      }
      if (draft.section === 'national') {
        if (isSportsTopic(`${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`)) {
          skipped.push({ reason: 'national_sports_topic_filtered_draft', title: draft.title, url: candidate.url });
          continue;
        }
        const draftStates = detectNationalStates(`${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`);
        const nonOhioStates = draftStates.filter((s) => s !== NATIONAL_EXCLUDED_STATE);
        if (!nonOhioStates.length) {
          skipped.push({ reason: 'national_state_outside_ohio_required', title: draft.title, url: candidate.url });
          continue;
        }
        const hasAvailableState = nonOhioStates.some((state) => {
          const usedCount = (nationalStatesUsedToday.has(state) ? 1 : 0) + (nationalStatesUsedThisRun.has(state) ? 1 : 0);
          return usedCount < nationalStateDailyCap;
        });
        if (!hasAvailableState) {
          skipped.push({ reason: 'national_state_daily_cap_reached', title: draft.title, url: candidate.url });
          continue;
        }
      }
      {
        const section = normalizeSection(draft.section) || 'local';
        const draftEventKeys = buildSectionEventKeys(section, `${draft.title} ${draft.description || ''}`);
        const existingSet = existingEventKeysBySection[section] || new Set();
        const runSet = runEventKeysBySection[section] || new Set();
        if (draftEventKeys.length) {
          if (section === 'sports') {
            if (hasAnyKeyIntersection(draftEventKeys, existingSet) || hasAnyKeyIntersection(draftEventKeys, runSet)) {
              skipped.push({ reason: 'duplicate_sports_event_draft', title: draft.title, url: candidate.url });
              continue;
            }
          } else {
            const overlap = Math.max(
              countKeyIntersection(draftEventKeys, existingSet),
              countKeyIntersection(draftEventKeys, runSet)
            );
            if (overlap >= 2) {
              skipped.push({ reason: 'duplicate_section_event_draft', title: draft.title, url: candidate.url });
              continue;
            }
          }
        }
      }
      const slug = ensureNonEmptyDraftSlug(draft.title, candidate);

      if (!dryRun) {
        const inserted = await sql`
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
            ${createdVia},
            'pending_review'
          )
          ON CONFLICT (slug) DO NOTHING
          RETURNING id
        `;
        if (!inserted?.length) {
          skipped.push({ reason: 'duplicate_slug_conflict', title: draft.title, url: candidate.url });
          continue;
        }
      }

      createdBySection[draft.section] = (createdBySection[draft.section] || 0) + 1;
      runTokensConsumed += Number(draft.totalTokens || 0);
      runTitles.push(draft.title);
      {
        const section = normalizeSection(draft.section) || 'local';
        const eventKeys = buildSectionEventKeys(section, `${draft.title} ${draft.description || ''}`);
        if (!runEventKeysBySection[section]) runEventKeysBySection[section] = new Set();
        for (const key of eventKeys) {
          runEventKeysBySection[section].add(key);
        }
      }
      if (draft.section === 'sports' && isTennisTopic(`${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`)) {
        tennisCreatedThisRun += 1;
      }
      if (draft.section === 'business') {
        const text = `${draft.title || ''} ${draft.description || ''} ${draft.content || ''} ${candidate.url || ''}`;
        if (isBusinessLocalTopic(text)) businessLocalCreatedThisRun += 1;
        if (candidate.businessMode === 'daily_market_update' || isBusinessMarketUpdateTopic(text)) {
          businessMarketUpdateCreatedThisRun += 1;
        }
      }
      if (draft.section === 'national') {
        const states = detectNationalStates(`${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`);
        for (const state of states) {
          if (state !== NATIONAL_EXCLUDED_STATE) nationalStatesUsedThisRun.add(state);
        }
      }
      if (draft.section === 'world') {
        const text = `${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`;
        const regions = detectWorldRegions(text);
        for (const region of regions) {
          if (worldRegionCountsThisRun[region] !== undefined) {
            worldRegionCountsThisRun[region] += 1;
          }
        }
        if (regions.includes('middle_east')) worldMiddleEastThisRun += 1;
        if (isUsDiplomacyTopic(text)) worldUsDiplomacyThisRun += 1;
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

    const topSkipReasons = getTopSkipReasons(skipped);
    await logDraftGenerationRun(sql, {
      runStatus: 'ok',
      runReason: created.length < targetCount ? 'underfilled' : 'filled',
      scheduleMode,
      track,
      runMode,
      createdVia,
      dryRun,
      includeSections: includeSections?.join(',') || '',
      excludeSections: excludeSections?.join(',') || '',
      activeSections: activeSections.join(','),
      etDate,
      etTime,
      requestedCount: effectiveRequestedCount,
      targetCount,
      createdCount: created.length,
      skippedCount: skipped.length,
      dailyTokenBudget,
      tokensUsedToday,
      runTokensConsumed,
      writerProvider,
      writerModelForRun,
      topSkipReasons
    });
    return res.status(200).json({
      ok: true,
      dryRun,
      requested: effectiveRequestedCount,
      dailyTokenBudget,
      tokensUsedToday,
      runTokensConsumed,
      tokensUsedAfterRun: tokensUsedToday + runTokensConsumed,
      scheduleMode,
      track,
      runMode,
      createdVia,
      writerProvider,
      writerModelForRun,
      memorySuppressionEnabled,
      sportsFocusMode,
      etDate,
      etTime,
      includeSections,
      excludeSections,
      activeSections,
      targetCount,
      minArticleWords: MIN_ARTICLE_WORDS,
      targetArticleWords: TARGET_ARTICLE_WORDS,
      modelMaxOutputTokens: getModelMaxOutputTokens(),
      retryMinInitialWords: RETRY_MIN_INITIAL_WORDS,
      reportedDuplicateCount: reportedDuplicateRows.length,
      reportedExternalDuplicateCount: reportedExternalDuplicateRows.length,
      reportedDuplicateSourceCount: reportedDuplicateSourceUrls.size,
      reportedExternalDuplicateSourceCount: reportedExternalDuplicateSourceUrls.size,
      editorialRejectCount: editorialRejectRows.length,
      editorialRejectSourceCount: editorialRejectedSourceUrls.size,
      sectionTargets: SECTION_DAILY_TARGETS,
      runTargets,
      remainingBySection,
      tennisDailyCap,
      tennisCreatedToday,
      tennisCreatedThisRun,
      businessLocalDailyMin: BUSINESS_LOCAL_DAILY_MIN,
      businessLocalCreatedToday,
      businessLocalCreatedThisRun,
      businessDailyMarketUpdateMax: BUSINESS_DAILY_MARKET_UPDATE_MAX,
      businessMarketUpdateCreatedToday,
      businessMarketUpdateCreatedThisRun,
      nationalStateDailyCap,
      nationalStatesUsedToday: Array.from(nationalStatesUsedToday),
      nationalStatesUsedThisRun: Array.from(nationalStatesUsedThisRun),
      worldRequiredDailyRegions: WORLD_REQUIRED_DAILY_REGIONS,
      worldRegionDailyCap,
      worldUsDiplomacyDailyCap,
      worldMiddleEastDailyCap,
      worldRegionCountsToday,
      worldRegionCountsThisRun,
      worldUsDiplomacyToday,
      worldUsDiplomacyThisRun,
      worldMiddleEastToday,
      worldMiddleEastThisRun,
      createdBySection,
      createdCount: created.length,
      skippedCount: skipped.length,
      created,
      skipped: skipped.slice(0, 20),
      topSkipReasons
    });
  } catch (error) {
    console.error('Generate drafts error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    try {
      await logDraftGenerationRun(sql, {
        runStatus: 'error',
        runReason: error.message || 'unknown_error',
        scheduleMode,
        track,
        runMode,
        createdVia,
        dryRun,
        includeSections: includeSections?.join(',') || '',
        excludeSections: excludeSections?.join(',') || '',
        activeSections: activeSections.join(','),
        writerProvider,
        writerModelForRun
      });
    } catch (logError) {
      console.error('Run logging error:', logError);
    }
    return res.status(500).json({ error: 'Failed to generate drafts', details: error.message });
  }
};
