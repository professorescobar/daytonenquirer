const PERSONA_OPTIONS_BY_BEAT = {
  'general-local': [{ value: 'local-reporter', label: 'Local Reporter' }],
  government: [{ value: 'city-hall-reporter', label: 'City Hall Beat Reporter' }],
  crime: [{ value: 'crime-justice-reporter', label: 'Crime & Justice Reporter' }],
  education: [{ value: 'education-reporter', label: 'Education Beat Reporter' }],
  'general-national': [{ value: 'national-correspondent', label: 'National Correspondent' }],
  politics: [{ value: 'political-analyst', label: 'Political Analyst' }],
  'social-issues': [{ value: 'feature-writer-human-interest', label: 'Feature Writer (Human Interest)' }],
  'general-world': [{ value: 'foreign-correspondent', label: 'Foreign Correspondent' }],
  conflict: [{ value: 'conflict-zone-reporter', label: 'Conflict Zone Reporter' }],
  diplomacy: [{ value: 'diplomatic-correspondent', label: 'Diplomatic Correspondent' }],
  'general-business': [{ value: 'business-reporter', label: 'Business Reporter' }],
  'local-business': [{ value: 'local-business-reporter', label: 'Local Business Reporter' }],
  markets: [{ value: 'financial-analyst', label: 'Financial Analyst' }],
  'real-estate': [{ value: 'real-estate-analyst', label: 'Real Estate Analyst' }],
  'general-sports': [{ value: 'sports-reporter', label: 'Sports Reporter' }],
  'high-school': [{ value: 'local-sports-writer', label: 'Local Sports Writer' }],
  college: [{ value: 'ncaa-beat-writer', label: 'NCAA Beat Writer' }],
  professional: [{ value: 'pro-sports-analyst', label: 'Pro Sports Analyst' }],
  'general-health': [{ value: 'health-science-reporter', label: 'Health & Science Reporter' }],
  'local-health': [{ value: 'local-health-reporter', label: 'Local Health Reporter' }],
  wellness: [{ value: 'wellness-lifestyle-writer', label: 'Wellness & Lifestyle Writer' }],
  'medical-research': [{ value: 'medical-journal-analyst', label: 'Medical Journal Analyst' }],
  'general-entertainment': [{ value: 'entertainment-reporter', label: 'Entertainment Reporter' }],
  'local-entertainment': [{ value: 'local-culture-critic', label: 'Local Culture Critic' }],
  movies: [{ value: 'film-critic', label: 'Film Critic' }],
  music: [{ value: 'music-critic', label: 'Music Critic' }],
  gaming: [{ value: 'tsuki-tamara', label: 'Tsuki Tamara (Gaming Journalist)' }],
  'general-technology': [{ value: 'tech-reporter', label: 'Tech Reporter' }],
  'local-tech': [{ value: 'local-tech-reporter', label: 'Local Tech Reporter' }],
  ai: [{ value: 'ai-future-tech-analyst', label: 'AI & Future Tech Analyst' }],
  'consumer-tech': [{ value: 'gadget-reviewer', label: 'Gadget Reviewer' }]
};

const ALL_PERSONAS = new Map();
for (const beat of Object.values(PERSONA_OPTIONS_BY_BEAT)) {
  for (const persona of beat) {
    if (!ALL_PERSONAS.has(persona.value)) {
      ALL_PERSONAS.set(persona.value, persona.label);
    }
  }
}

function getPersonaLabel(id) {
  return ALL_PERSONAS.get(id) || 'The Author';
}

module.exports = {
  PERSONA_OPTIONS_BY_BEAT,
  getPersonaLabel
};
