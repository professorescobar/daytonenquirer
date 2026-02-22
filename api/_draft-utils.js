function generateSlug(title) {
  return String(title || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 120);
}

function normalizeSection(section) {
  const s = String(section || '').toLowerCase().trim();
  const allowed = new Set([
    'local',
    'national',
    'world',
    'business',
    'sports',
    'health',
    'entertainment',
    'technology'
  ]);
  return allowed.has(s) ? s : 'national';
}

function cleanText(text) {
  return String(text || '').trim();
}

function truncate(text, maxLen) {
  return String(text || '').slice(0, maxLen);
}

module.exports = {
  generateSlug,
  normalizeSection,
  cleanText,
  truncate
};
