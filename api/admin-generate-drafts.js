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
const TARGET_ARTICLE_WORDS = 700;
const DEFAULT_MAX_OUTPUT_TOKENS = 2600;
const RETRY_MIN_INITIAL_WORDS = 450;

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
const BUSINESS_MARKET_UPDATE_SLOT_ET = '06:05';
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
const WORLD_REQUIRED_DAILY_REGIONS = ['southeast_asia', 'europe', 'south_america'];
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
    'https://news.google.com/rss/search?q=Montgomery+County+Ohio+breaking+news+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:daytondailynews.com+Dayton+local+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:wdtn.com+Dayton+local+news+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Sinclair+Community+College+Dayton+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Kettering+College+Dayton+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Central+State+University+Ohio+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Antioch+University+Midwest+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Cedarville+University+Ohio+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=SOCHE+Ohio+higher+education+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:udayton.edu+University+of+Dayton+-athletics+-basketball+-football+-baseball+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:wright.edu+Wright+State+-athletics+-basketball+-football+-baseball+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:miamioh.edu+Miami+University+Oxford+Ohio+-athletics+-basketball+-football+-baseball+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Wright-Patterson+Air+Force+Base+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Fuyao+Glass+America+Moraine+Ohio+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+International+Airport+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=2nd+Street+Market+Dayton+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Food+Truck+Rally+when:30d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Oregon+District+Dayton+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Stivers+School+of+the+Arts+Dayton+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=LexisNexis+Dayton+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Reynolds+and+Reynolds+Dayton+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Honda+Anna+Ohio+plant+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Oakwood+Kettering+Moraine+Trotwood+Huber+Heights+Beavercreek+Bellbrook+Fairborn+Xenia+Springfield+Troy+Tipp+City+Miamisburg+Centerville+Springboro+Franklin+Middletown+news+when:3d&hl=en-US&gl=US&ceid=US:en'
  ],
  national: [
    'https://news.google.com/rss/headlines/section/topic/NATION?hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=United+States+state+news+-Ohio+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=state+government+update+United+States+-Ohio+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=state+supreme+court+decision+United+States+-Ohio+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=governor+announces+United+States+-Ohio+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=statewide+emergency+United+States+-Ohio+when:5d&hl=en-US&gl=US&ceid=US:en'
  ],
  world: [
    'https://news.google.com/rss/headlines/section/topic/WORLD?hl=en-US&gl=US&ceid=US:en'
  ],
  business: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+business+economy+jobs+when:2d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:bizjournals.com/dayton+Dayton+Business+Journal+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:daytondailynews.com+business+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:cnbc.com+earnings+large+cap+market+movers+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:finance.yahoo.com+earnings+large+cap+market+movers+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=S%26P+500+market+wrap+bond+yields+commodities+dollar+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=largest+market+cap+companies+earnings+when:2d&hl=en-US&gl=US&ceid=US:en'
  ],
  sports: [],
  health: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+health+hospital+medical+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Children%27s+Hospital+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Kettering+Health+Dayton+when:10d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Premier+Health+Dayton+when:10d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=CareSource+Dayton+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+VA+Medical+Center+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Miami+Valley+hospital+network+healthcare+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+%26+Montgomery+County+Public+Health+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Greater+Dayton+Area+Hospital+Association+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Community+Health+Centers+of+Greater+Dayton+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Ohio+Department+of+Health+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=OhioHealth+newsroom+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Daily+News+health+section+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Five+Rivers+Health+Centers+Dayton+when:21d&hl=en-US&gl=US&ceid=US:en'
  ],
  entertainment: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+entertainment+arts+music+events+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+things+to+do+this+weekend+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Oakwood+Kettering+Beavercreek+Centerville+Springboro+events+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Springfield+Troy+Miamisburg+Middletown+events+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Cincinnati+events+things+to+do+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Columbus+events+things+to+do+when:7d&hl=en-US&gl=US&ceid=US:en'
  ],
  technology: [
    'https://news.google.com/rss/search?q=Dayton+Ohio+Miami+Valley+technology+startup+innovation+when:3d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:sinclair.edu+technology+innovation+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:udayton.edu+technology+research+innovation+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:wright.edu+technology+research+innovation+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Miami+Valley+Career+Technology+Center+technology+when:30d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Wright-Patterson+technology+contract+innovation+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:bizjournals.com/dayton+Dayton+Inno+startup+venture+capital+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Ohio+tech+news+startup+venture+capital+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:daytondailynews.com+business+technology+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:wdtn.com+technology+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=OhioX+technology+startup+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Technology+First+Dayton+when:30d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Development+Coalition+technology+when:30d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=The+Entrepreneurs+Center+Dayton+startup+when:30d&hl=en-US&gl=US&ceid=US:en'
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
    'https://news.google.com/rss/search?q=Miami+University+Ohio+basketball+preview+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Columbus+Blue+Jackets+preview+matchup+when:5d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Cincinnati+Cyclones+preview+matchup+when:5d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Flyers+club+hockey+preview+when:10d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Stealth+hockey+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=RAHA+Dayton+hockey+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Team+USA+Olympics+Ohio+athletes+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Cincinnati+Open+tennis+when:30d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=University+of+Dayton+tennis+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Ohio+pickleball+tournament+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Miami+RedHawks+hockey+preview+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Ohio+State+hockey+preview+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Bowling+Green+hockey+preview+when:7d&hl=en-US&gl=US&ceid=US:en'
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
    'https://news.google.com/rss/search?q=Miami+University+Ohio+athletics+preview+schedule+when:7d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Columbus+Blue+Jackets+Dayton+Ohio+when:5d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Cincinnati+Cyclones+Dayton+Ohio+when:5d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Flyers+club+hockey+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Stealth+hockey+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=RAHA+Dayton+Recreational+Amateur+Hockey+Association+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Team+USA+Olympics+when:14d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=U.S.+Olympic+trials+Ohio+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Cincinnati+Open+tennis+Ohio+when:30d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Western+%26+Southern+Open+tennis+when:30d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:daytonflyers.com+tennis+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Dayton+Ohio+pickleball+when:21d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=Miami+RedHawks+Ohio+State+Bowling+Green+hockey+when:7d&hl=en-US&gl=US&ceid=US:en'
  ]
};

const SPORTS_LOCAL_COMMUNITY_FEEDS = [
  'https://news.google.com/rss/search?q=Dayton+Ohio+high+school+sports+when:7d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Miami+Valley+high+school+sports+when:7d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Dayton+area+prep+sports+when:7d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=GWOC+sports+Dayton+when:14d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Miami+Valley+League+sports+Ohio+when:14d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Dayton+community+sports+league+when:14d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Dayton+Bombers+hockey+history+Dayton+Ohio+when:30d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Dayton+Stealth+youth+hockey+when:21d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=RAHA+Dayton+hockey+league+when:30d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Dayton+pickleball+league+when:30d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Miami+Valley+pickleball+when:30d&hl=en-US&gl=US&ceid=US:en',
  'https://news.google.com/rss/search?q=Cincinnati+Open+history+oldest+tennis+tournament+US+when:60d&hl=en-US&gl=US&ceid=US:en'
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

function getModelMaxOutputTokens() {
  const configured = parseInt(
    process.env.ANTHROPIC_MAX_OUTPUT_TOKENS || String(DEFAULT_MAX_OUTPUT_TOKENS),
    10
  );
  if (!Number.isFinite(configured) || configured <= 0) return DEFAULT_MAX_OUTPUT_TOKENS;
  return Math.min(configured, 8192);
}

async function callAnthropicForDraft(candidate) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    throw new Error('Missing ANTHROPIC_API_KEY');
  }

  const localityRule = LOCAL_SCOPE_SECTIONS.has(candidate.section)
    ? 'This article MUST be focused on Dayton, Ohio and/or the Miami Valley region. "Miami" in this context means Miami Valley or Miami University (Oxford, Ohio), not Miami, Florida. If the source is not locally relevant, reject it by returning JSON with empty title/description/content.'
    : 'This article may cover broader non-local scope.';
  const localRule = candidate.section === 'local'
    ? 'For the local section, prioritize civic/institutional/community reporting in Dayton and nearby cities. Avoid sports coverage in local section (sports belong in the sports section).'
    : '';
  const healthRule = candidate.section === 'health'
    ? 'For the health section, prioritize Dayton/Miami Valley healthcare coverage: hospital systems, patient care updates, public health advisories, medical research, healthcare access and policy impacts. Include Dayton Children\'s, Kettering Health, Premier Health, CareSource, and Dayton VA when relevant.'
    : '';
  const nationalRule = candidate.section === 'national'
    ? 'For the national section, prioritize impactful stories from U.S. states outside Ohio. Avoid over-indexing on divisive political narrative content unless the story has clear broad public impact.'
    : '';
  const businessRule = candidate.section === 'business'
    ? `For the business section:
- Include at least one local business/economy story per day (Dayton/Miami Valley focus).
- Produce one daily market update that evaluates prior trading action in individual stocks, index funds, bonds, commodities, and the U.S. dollar over 5-day, 30-day, 3-month, 6-month, and 1-year context windows.
- No financial recommendations, no price targets, no calls to buy/sell/hold.
- Avoid cliches like "bulls and bears" and avoid sentiment-chasing language.
- Favor earnings, major large-cap developments, and notable market movers with clear reasons.
- Exclude penny stocks and microcap-focused stories unless there is extraordinary broad impact.`
    : '';
  const technologyRule = candidate.section === 'technology'
    ? 'For technology section, prioritize Dayton/Miami Valley innovation coverage: local university research, workforce-tech programs, startups, venture capital, government contracts, and regional innovation organizations (Sinclair, UD, Wright State, MVCTC, Wright-Patt, Dayton Inno, OhioX, Technology First, Dayton Development Coalition, The Entrepreneurs Center).'
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
    'If the story is political, write in a straight-news, centrist, non-inflammatory voice. Avoid partisan advocacy, ideological sloganeering, and culture-war framing. Focus on verified facts, constitutional process, rule of law, public safety, and practical impacts on everyday Americans. Do not write opinion or activist-style content.';
  const sportsRule = candidate.section === 'sports'
    ? `Prioritize upcoming local game coverage (previews, schedules, matchup context, stakes, and what to watch) when available. Current sports focus mode is "${candidate.sportsFocusMode || 'broad'}". Also prioritize Dayton/Miami Valley high school and community sports whenever strong local coverage is available. Avoid writing a second article on the same recent matchup unless there is clearly new and material information.`
    : '';

  const model = process.env.ANTHROPIC_MODEL || 'claude-3-5-sonnet-latest';
  const maxOutputTokens = getModelMaxOutputTokens();
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
   - Make the piece thought-provoking by surfacing deeper implications and unresolved questions grounded in facts.
   - Be thorough and specific, but concise. Do not ramble or repeat.
   - Avoid fluff and generic filler language.
6) section must be one of: local, national, world, business, sports, health, entertainment, technology.
7) Do not include fake quotes or unverifiable claims. If details are uncertain, state uncertainty clearly.
8) ${localityRule}
9) ${sportsRule}
10) ${localRule}
11) ${healthRule}
12) ${nationalRule}
13) ${politicsStyleRule}
14) ${businessRule}
15) ${marketUpdateFormatRule}
16) ${technologyRule}

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
  if (words < RETRY_MIN_INITIAL_WORDS) return { draft, words };

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
      // Ensure a dependable daily market outlook candidate appears at 6:05 ET.
      if (etTime === BUSINESS_MARKET_UPDATE_SLOT_ET) {
        sectionCandidates.unshift({
          section: 'business',
          title: 'U.S. Markets Daily Update: Multi-Asset Context Before Futures',
          url: 'internal://business-daily-market-update',
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

async function ensureDuplicateReportsTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS duplicate_reports (
      id SERIAL PRIMARY KEY,
      draft_id INTEGER,
      draft_slug TEXT,
      draft_title TEXT NOT NULL,
      section TEXT,
      source_url TEXT,
      source_title TEXT,
      report_reason TEXT DEFAULT 'manual_duplicate',
      notes TEXT,
      reported_by TEXT DEFAULT 'admin_ui',
      reported_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_duplicate_reports_draft_id_unique
    ON duplicate_reports(draft_id)
    WHERE draft_id IS NOT NULL
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_duplicate_reports_reported_at
    ON duplicate_reports(reported_at DESC)
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_duplicate_reports_source_url
    ON duplicate_reports(source_url)
    WHERE source_url IS NOT NULL
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (!['POST', 'GET'].includes(String(req.method || '').toUpperCase())) {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const requestedCount = Math.min(parseInt(req.body?.count || req.query.count || '6', 10), 50);
  const dryRun = String(req.body?.dryRun || req.query.dryRun || 'false') === 'true';
  const dailyTokenBudgetOverride = req.body?.dailyTokenBudget || req.query.dailyTokenBudget;
  const scheduleMode = String(req.body?.schedule || req.query.schedule || '').toLowerCase();
  const requestedRunMode = String(req.body?.runMode || req.query.runMode || 'auto').toLowerCase();
  const track = String(req.body?.track || req.query.track || '').toLowerCase();
  const runMode = scheduleMode === 'auto'
    ? 'auto'
    : (requestedRunMode === 'manual' ? 'manual' : 'auto');
  const createdVia = runMode === 'manual' ? 'manual' : 'auto';
  const includeSections = parseSectionList(req.body?.includeSections || req.query.includeSections);
  const excludeSections = parseSectionList(req.body?.excludeSections || req.query.excludeSections);
  const activeSections = resolveActiveSections(includeSections, excludeSections);

  if (!activeSections.length) {
    return res.status(400).json({ error: 'No active sections after include/exclude filters' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureDuplicateReportsTable(sql);
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
    if (!dryRun && runMode === 'auto' && tokensUsedToday >= dailyTokenBudget) {
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

    const targetCount = dryRun
      ? requestedCount
      : (runMode === 'manual' ? requestedCount : Math.min(requestedCount, remainingToday));

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
      WHERE created_at >= date_trunc('day', now())
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
      WHERE created_at >= date_trunc('day', now())
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
    const worldRegionDailyCap = 1;
    const worldUsDiplomacyDailyCap = 1;
    const worldMiddleEastDailyCap = 1;
    const todayWorldTopicRows = await sql`
      SELECT title, description, content, source_title as "sourceTitle"
      FROM article_drafts
      WHERE created_at >= date_trunc('day', now())
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
    const nationalStateDailyCap = 1;
    const todayNationalTopicRows = await sql`
      SELECT title, description, content, source_title as "sourceTitle"
      FROM article_drafts
      WHERE created_at >= date_trunc('day', now())
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
    const reportedDuplicateRows = await sql`
      SELECT
        section,
        draft_title as "draftTitle",
        source_title as "sourceTitle",
        source_url as "sourceUrl"
      FROM duplicate_reports
      WHERE reported_at >= NOW() - INTERVAL '365 days'
    `;
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
        if ((businessMarketUpdateCreatedToday + businessMarketUpdateCreatedThisRun) >= BUSINESS_DAILY_MARKET_UPDATE_MAX && isMarketUpdateCandidate) {
          skipped.push({ reason: 'business_daily_market_update_cap_reached', title: candidate.title, url: candidate.url });
          continue;
        }
        if (
          (businessLocalCreatedToday + businessLocalCreatedThisRun) < BUSINESS_LOCAL_DAILY_MIN &&
          !isLocalBusinessCandidate
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
      if (normalizedCandidateTitle && reportedDuplicateNormalizedTitles.has(normalizedCandidateTitle)) {
        skipped.push({ reason: 'reported_duplicate_exact_title', title: candidate.title, url: candidate.url });
        continue;
      }
      if (candidate.url && reportedDuplicateSourceUrls.has(candidate.url)) {
        skipped.push({ reason: 'reported_duplicate_source', title: candidate.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(candidate.title, reportedDuplicateTitles)) {
        skipped.push({ reason: 'reported_duplicate_near_title', title: candidate.title, url: candidate.url });
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
      const normalizedDraftTitle = normalizeComparableTitle(draft.title);
      if (normalizedDraftTitle && reportedDuplicateNormalizedTitles.has(normalizedDraftTitle)) {
        skipped.push({ reason: 'reported_duplicate_exact_draft_title', title: draft.title, url: candidate.url });
        continue;
      }
      if (isNearDuplicateTitle(draft.title, reportedDuplicateTitles)) {
        skipped.push({ reason: 'reported_duplicate_near_draft_title', title: draft.title, url: candidate.url });
        continue;
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
        if ((businessMarketUpdateCreatedToday + businessMarketUpdateCreatedThisRun) >= BUSINESS_DAILY_MARKET_UPDATE_MAX && isMarketUpdateDraft) {
          skipped.push({ reason: 'business_daily_market_update_cap_reached_draft', title: draft.title, url: candidate.url });
          continue;
        }
        if (
          (businessLocalCreatedToday + businessLocalCreatedThisRun) < BUSINESS_LOCAL_DAILY_MIN &&
          !isLocalBusinessDraft
        ) {
          skipped.push({ reason: 'business_local_required_pending_draft', title: draft.title, url: candidate.url });
          continue;
        }
      }
      if (draft.section === 'world') {
        const draftText = `${draft.title || ''} ${draft.description || ''} ${draft.content || ''}`;
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
      const slug = generateSlug(draft.title);

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
      runMode,
      createdVia,
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
      reportedDuplicateSourceCount: reportedDuplicateSourceUrls.size,
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
      skipped: skipped.slice(0, 20)
    });
  } catch (error) {
    console.error('Generate drafts error:', error);
    return res.status(500).json({ error: 'Failed to generate drafts', details: error.message });
  }
};
