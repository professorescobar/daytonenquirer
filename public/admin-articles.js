const lockSection = document.getElementById('article-lock');
const appSection = document.getElementById('article-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');

const tokenInput = document.getElementById('admin-token');
const sectionFilterInput = document.getElementById('article-section-filter');
const imageStatusFilterInput = document.getElementById('article-image-status-filter');
const limitInput = document.getElementById('article-list-limit');
const saveTokenBtn = document.getElementById('save-token-btn');
const loadArticlesBtn = document.getElementById('load-articles-btn');
const showAllBtn = document.getElementById('show-all-btn');
const articleSearchInput = document.getElementById('article-search');
const articleTotalCountEl = document.getElementById('article-total-count');
const messageEl = document.getElementById('admin-message');
const listEl = document.getElementById('article-list');
const removeModalEl = document.getElementById('article-remove-modal');
const removeConfirmInput = document.getElementById('article-remove-confirm-input');
const removeConfirmBtn = document.getElementById('article-remove-confirm-btn');
const removeCancelBtn = document.getElementById('article-remove-cancel-btn');

const SECTION_OPTIONS = [
  'local',
  'national',
  'world',
  'business',
  'sports',
  'health',
  'entertainment',
  'technology'
];

const BEAT_OPTIONS_BY_SECTION = {
  local: [
    { value: 'general-local', label: 'General Local' },
    { value: 'government', label: 'Government' },
    { value: 'crime', label: 'Crime' },
    { value: 'education', label: 'Education' }
  ],
  national: [
    { value: 'general-national', label: 'General National' },
    { value: 'politics', label: 'Politics' },
    { value: 'social-issues', label: 'Social Issues' }
  ],
  world: [
    { value: 'general-world', label: 'General World' },
    { value: 'conflict', label: 'Conflict' },
    { value: 'diplomacy', label: 'Diplomacy' }
  ],
  business: [
    { value: 'general-business', label: 'General Business' },
    { value: 'local-business', label: 'Local Business' },
    { value: 'markets', label: 'Markets' },
    { value: 'real-estate', label: 'Real Estate' }
  ],
  sports: [
    { value: 'general-sports', label: 'General Sports' },
    { value: 'high-school', label: 'High School' },
    { value: 'college', label: 'College' },
    { value: 'professional', label: 'Professional' }
  ],
  health: [
    { value: 'general-health', label: 'General Health' },
    { value: 'local-health', label: 'Local Health' },
    { value: 'wellness', label: 'Wellness' },
    { value: 'medical-research', label: 'Medical Research' }
  ],
  entertainment: [
    { value: 'general-entertainment', label: 'General Entertainment' },
    { value: 'local-entertainment', label: 'Local Entertainment' },
    { value: 'movies', label: 'Movies' },
    { value: 'music', label: 'Music' },
    { value: 'gaming', label: 'Gaming' }
  ],
  technology: [
    { value: 'general-technology', label: 'General Technology' },
    { value: 'local-tech', label: 'Local Tech' },
    { value: 'ai', label: 'AI' },
    { value: 'consumer-tech', label: 'Consumer Tech' }
  ]
};

const STATIC_PERSONA_OPTIONS_BY_BEAT = {
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

let unlocked = false;
let loadedArticles = [];
let lastTotalCount = 0;
let totalAllArticles = 0;
let removeTargetArticleId = 0;
let dynamicPersonasByBeat = {};
let dynamicBeatsBySection = {};
const ET_TIME_ZONE = 'America/New_York';
const CLOUDINARY_CLOUD_NAME = 'dtlkzlp87';
const CLOUDINARY_UPLOAD_PRESET = 'dayton-enquirer';
const CLOUDINARY_WIDTH = 1600;

function setSelectOptions(selectEl, options, fallbackOption) {
  if (!selectEl) return;
  const validOptions = (options || []).filter((opt) => opt && opt.value && opt.label);
  if (!validOptions.length && fallbackOption) {
    selectEl.innerHTML = `<option value="${escapeHtml(fallbackOption.value)}">${escapeHtml(fallbackOption.label)}</option>`;
    return;
  }
  selectEl.innerHTML = validOptions.map((opt) => `<option value="${escapeHtml(opt.value)}">${escapeHtml(opt.label)}</option>`).join('');
}

function normalizeDynamicPersonasByBeat(rows) {
  const personasByBeat = {};
  const beatsBySection = {};
  for (const row of Array.isArray(rows) ? rows : []) {
    const id = String(row?.id || '').trim();
    const beat = String(row?.beat || '').trim();
    const section = String(row?.section || '').trim().toLowerCase() || 'local';
    if (!id || !beat) continue;
    const label = String(row?.displayName || '').trim() || id;
    if (!personasByBeat[beat]) personasByBeat[beat] = [];
    personasByBeat[beat].push({ value: id, label });
    if (!beatsBySection[section]) beatsBySection[section] = [];
    if (!beatsBySection[section].some((item) => item.value === beat)) {
      beatsBySection[section].push({ value: beat, label: beat.replace(/-/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase()) });
    }
  }
  return { personasByBeat, beatsBySection };
}

function getBeatOptionsForSection(section) {
  const key = String(section || '').trim().toLowerCase() || 'local';
  const merged = new Map();
  for (const item of BEAT_OPTIONS_BY_SECTION[key] || []) {
    if (!item?.value || !item?.label) continue;
    merged.set(item.value, { value: item.value, label: item.label });
  }
  for (const item of dynamicBeatsBySection[key] || []) {
    if (!item?.value || !item?.label) continue;
    merged.set(item.value, { value: item.value, label: item.label });
  }
  const result = Array.from(merged.values());
  return result.length ? result : (BEAT_OPTIONS_BY_SECTION.local || []);
}

function getPersonasForBeat(beat) {
  const key = String(beat || '').trim();
  const merged = new Map();
  const fallback = STATIC_PERSONA_OPTIONS_BY_BEAT['general-local'] || [];
  for (const option of STATIC_PERSONA_OPTIONS_BY_BEAT[key] || []) {
    if (!option?.value || !option?.label) continue;
    merged.set(option.value, { value: option.value, label: option.label });
  }
  for (const option of dynamicPersonasByBeat[key] || []) {
    if (!option?.value || !option?.label) continue;
    merged.set(option.value, { value: option.value, label: option.label });
  }
  const options = Array.from(merged.values());
  return options.length ? options : fallback;
}

async function loadPersonaDirectory() {
  try {
    const data = await apiRequest('/api/admin-personas');
    const normalized = normalizeDynamicPersonasByBeat(data?.personas || []);
    dynamicPersonasByBeat = normalized.personasByBeat;
    dynamicBeatsBySection = normalized.beatsBySection;
  } catch (_) {
    dynamicPersonasByBeat = {};
    dynamicBeatsBySection = {};
  }
}

function aiModelSelectHtml(defaultValue = 'anthropic:claude-sonnet-4-6') {
  const options = [
    { value: 'anthropic:claude-sonnet-4-6', label: 'Claude Sonnet 4.6' },
    { value: 'openai:gpt-5', label: 'ChatGPT (GPT-5)' },
    { value: 'gemini:gemini-3-pro-preview', label: 'Gemini 3 Pro Preview' },
    { value: 'grok:grok-4', label: 'Grok 4' }
  ];
  return options
    .map((opt) => `<option value="${opt.value}" ${opt.value === defaultValue ? 'selected' : ''}>${opt.label}</option>`)
    .join('');
}

function rewriteModelSelectHtml() {
  return `<option value="" selected>Select model...</option>${aiModelSelectHtml('__none__')}`;
}

const REWRITE_ISSUES = {
  base: {
    headline: [
      { id: 'too_generic', label: 'Too generic' },
      { id: 'not_newsworthy', label: 'Not newsworthy enough' },
      { id: 'unclear', label: 'Unclear' },
      { id: 'too_long', label: 'Too long' },
      { id: 'too_clickbait', label: 'Too clickbait' },
      { id: 'weak_hook', label: 'Weak hook' }
    ],
    description: [
      { id: 'too_vague', label: 'Too vague' },
      { id: 'too_hypey', label: 'Too hypey' },
      { id: 'too_generic', label: 'Too generic' },
      { id: 'too_wordy', label: 'Too wordy' },
      { id: 'weak_seo', label: 'Weak SEO focus' },
      { id: 'weak_hook', label: 'Weak hook' }
    ],
    article: [
      { id: 'not_long_enough', label: 'Not long enough' },
      { id: 'too_much_fluff', label: 'Too much fluff' },
      { id: 'cheesy_corny', label: 'Cheesy/corny tone' },
      { id: 'not_enough_enthusiasm', label: 'Not enough enthusiasm' },
      { id: 'too_much_enthusiasm', label: 'Too much enthusiasm' },
      { id: 'not_thought_provoking', label: 'Not thought-provoking enough' },
      { id: 'repetitive', label: 'Repetitive' },
      { id: 'unclear_structure', label: 'Unclear structure' }
    ]
  },
  provider: {
    anthropic: {
      article: [{ id: 'overcautious', label: 'Overly cautious framing' }]
    },
    gemini: {
      article: [{ id: 'overhedging', label: 'Too much hedging' }]
    },
    openai: {
      article: [{ id: 'surface_level', label: 'Too surface-level' }]
    },
    grok: {
      article: [{ id: 'hot_take_bias', label: 'Too hot-take oriented' }]
    }
  }
};

function getProviderFromModelValue(value) {
  const raw = String(value || '').trim();
  if (!raw.includes(':')) return 'anthropic';
  return raw.split(':')[0] || 'anthropic';
}

function getRewriteIssueOptions(target, provider) {
  const base = REWRITE_ISSUES.base[target] || [];
  const extras = REWRITE_ISSUES.provider[provider]?.[target] || [];
  return [...base, ...extras];
}

function rewriteIssueOptionsHtml(target, provider = 'anthropic') {
  return getRewriteIssueOptions(target, provider)
    .map((issue) => `<option value="${issue.id}">${escapeHtml(issue.label)}</option>`)
    .join('');
}

function setHeadlineOptions(card, headlineList) {
  const wrap = card.querySelector('.headline-options');
  if (!wrap) return;
  const options = Array.isArray(headlineList) ? headlineList.filter(Boolean).slice(0, 3) : [];
  if (!options.length) {
    wrap.innerHTML = '';
    return;
  }
  wrap.innerHTML = `
    <p class="draft-meta">Headline options:</p>
    <div class="admin-actions">
      ${options.map((headline, index) => `
        <button type="button" class="btn ${index === 0 ? 'btn-primary' : ''} btn-headline-option" data-headline="${escapeHtml(headline)}">
          ${index === 0 ? 'Use best: ' : 'Use alt: '}${escapeHtml(headline)}
        </button>
      `).join('')}
    </div>
  `;
}

function syncBeatOptions(card) {
  const section = card.querySelector('.field-section')?.value;
  const beatSelect = card.querySelector('.field-beat');
  const currentBeat = beatSelect?.value;
  const beats = getBeatOptionsForSection(section);
  setSelectOptions(beatSelect, beats, beats[0]);
  if (currentBeat && beats.some((b) => b.value === currentBeat)) {
    beatSelect.value = currentBeat;
  }
}

function syncPersonaOptions(card) {
  const beat = card.querySelector('.field-beat')?.value;
  const personaSelect = card.querySelector('.field-persona');
  const currentPersona = personaSelect?.value;
  const personas = getPersonasForBeat(beat);
  setSelectOptions(personaSelect, personas, personas[0]);
  if (currentPersona && personas.some((p) => p.value === currentPersona)) {
    personaSelect.value = currentPersona;
  }
}

function syncRewriteIssueSelect(card, target) {
  const modelSelect = card.querySelector(`.job-model-rewrite-${target}`);
  const issueSelect = card.querySelector(`.rewrite-issues-${target}`);
  const issueStep = card.querySelector(`.ai-step-issues-${target}`);
  const runStep = card.querySelector(`.ai-step-run-${target}`);
  const runBtn = card.querySelector(`.btn-rewrite-${target}`);
  if (!modelSelect || !issueSelect) return;
  const modelValue = String(modelSelect.value || '').trim();
  const hasModel = modelValue.includes(':');
  if (!hasModel) {
    issueSelect.innerHTML = '<option value="">Select model first</option>';
    issueSelect.disabled = true;
    if (issueStep) issueStep.setAttribute('hidden', '');
    if (runStep) runStep.setAttribute('hidden', '');
    if (runBtn) runBtn.disabled = true;
    return;
  }
  issueSelect.disabled = false;
  if (issueStep) issueStep.removeAttribute('hidden');
  if (runStep) runStep.removeAttribute('hidden');
  const provider = getProviderFromModelValue(modelSelect.value);
  const selected = Array.from(issueSelect.selectedOptions).map((opt) => opt.value);
  issueSelect.innerHTML = rewriteIssueOptionsHtml(target, provider);
  Array.from(issueSelect.options).forEach((opt) => {
    if (selected.includes(opt.value)) opt.selected = true;
  });
  if (runBtn) runBtn.disabled = getSelectedIssues(card, target).length < 1;
}

function enforceIssueLimit(selectEl, max = 3) {
  if (!selectEl) return;
  const selected = Array.from(selectEl.selectedOptions);
  if (selected.length <= max) return;
  selected[selected.length - 1].selected = false;
  setMessage(`Select up to ${max} rewrite issues.`);
}

function getSelectedIssues(card, target) {
  const issueSelect = card.querySelector(`.rewrite-issues-${target}`);
  if (!issueSelect) return [];
  return Array.from(issueSelect.selectedOptions).map((opt) => String(opt.value || '').trim()).filter(Boolean).slice(0, 3);
}

function toggleAiPanel(card, panelClass, anchorButton) {
  if (!card || !panelClass || !anchorButton) return;
  const root = card.querySelector('.draft-form') || card;
  const targetPanel = card.querySelector(`.${panelClass}`);
  if (!targetPanel) return;
  const opening = targetPanel.hasAttribute('hidden') || targetPanel.dataset.anchorClass !== panelClass;
  card.querySelectorAll('.ai-panel').forEach((panel) => panel.setAttribute('hidden', ''));
  if (!opening) return;
  targetPanel.removeAttribute('hidden');
  targetPanel.dataset.anchorClass = panelClass;
  const buttonRect = anchorButton.getBoundingClientRect();
  const isGeneratePanel = panelClass.endsWith('-gen');
  const panelWidth = isGeneratePanel
    ? Math.max(140, Math.round(buttonRect.width))
    : Math.min(360, Math.max(240, Math.round(root.clientWidth * 0.42)));
  targetPanel.classList.toggle('ai-panel-compact', isGeneratePanel);
  targetPanel.style.width = `${panelWidth}px`;
  const parent = targetPanel.offsetParent || root;
  const parentRect = parent.getBoundingClientRect();
  const parentWidth = Math.max(120, parent.clientWidth || root.clientWidth || panelWidth);
  const leftRaw = buttonRect.left - parentRect.left;
  const left = Math.max(8, Math.min(leftRaw, parentWidth - panelWidth - 8));
  const top = Math.max(8, buttonRect.bottom - parentRect.top + 6);
  targetPanel.style.left = `${left}px`;
  targetPanel.style.top = `${top}px`;
}

function closeAllAiPanels() {
  document.querySelectorAll('.ai-panel').forEach((panel) => panel.setAttribute('hidden', ''));
}

function getSelectedModelLabel(card, selectorClass) {
  const select = card?.querySelector(selectorClass);
  const label = select?.selectedOptions?.[0]?.textContent || '';
  return String(label).trim() || 'selected model';
}

function showLoadingOverlay(card, key, targetSelector, label) {
  const root = card?.querySelector('.draft-form') || card;
  const target = card?.querySelector(targetSelector);
  if (!root || !target) return;
  hideLoadingOverlay(card, key);
  const rootRect = root.getBoundingClientRect();
  const rect = target.getBoundingClientRect();
  const overlay = document.createElement('div');
  overlay.className = 'field-loading-overlay';
  overlay.dataset.loadingKey = key;
  overlay.style.left = `${Math.max(0, rect.left - rootRect.left)}px`;
  overlay.style.top = `${Math.max(0, rect.top - rootRect.top)}px`;
  overlay.style.width = `${Math.max(40, rect.width)}px`;
  overlay.style.height = `${Math.max(40, rect.height)}px`;
  overlay.innerHTML = `
    <div class="field-loading-overlay-inner">
      <span class="field-loading-spinner" aria-hidden="true"></span>
      <span>${escapeHtml(label || 'Generating...')}</span>
    </div>
  `;
  root.appendChild(overlay);
}

function hideLoadingOverlay(card, key) {
  card?.querySelectorAll(`.field-loading-overlay[data-loading-key="${key}"]`).forEach((el) => el.remove());
}

function clearLoadingOverlays(card, keys) {
  (keys || []).forEach((key) => hideLoadingOverlay(card, key));
}

function setMessage(text) {
  messageEl.hidden = !text;
  messageEl.textContent = text || '';
}

function scrollToTopStatus() {
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function getToken() {
  return (tokenInput.value || '').trim();
}

function setLockState(value) {
  unlocked = value;
  lockSection.hidden = value;
  appSection.hidden = !value;
}

async function unlock() {
  try {
    const password = (adminUiPasswordInput.value || '').trim();
    if (!password) throw new Error('Enter admin UI password');

    const res = await fetch('/api/admin-ui-auth', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'Unlock failed');

    sessionStorage.setItem('de_admin_unlocked_articles', '1');
    setLockState(true);
    setMessage('Editor unlocked.');
    if (getToken()) {
      await loadArticles();
    }
  } catch (err) {
    setMessage(`Unlock failed: ${err.message}`);
  }
}

async function apiRequest(url, options = {}) {
  if (!unlocked) throw new Error('Editor is locked');
  const token = getToken();
  if (!token) throw new Error('Missing admin token');

  const res = await fetch(url, {
    ...options,
    headers: {
      'x-admin-token': token,
      ...(options.headers || {})
    }
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const message = data.error || `Request failed (${res.status})`;
    throw new Error(data.details ? `${message}: ${data.details}` : message);
  }
  return data;
}

function saveToken() {
  localStorage.setItem('de_admin_token', getToken());
  setMessage('Token saved.');
}

function loadToken() {
  const token = localStorage.getItem('de_admin_token') || '';
  if (token) tokenInput.value = token;
}

function escapeHtml(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatDate(dateString) {
  if (!dateString) return '';
  const d = new Date(dateString);
  if (Number.isNaN(d.getTime())) return dateString;
  return d.toLocaleString();
}

function normalizeImageStatus(article) {
  const raw = String(article?.imageStatus || article?.renderClass || '').trim().toLowerCase();
  if (raw === 'with_image' || raw === 'text_only') return raw;
  return 'text_only';
}

function getEtPartsFromDate(date) {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone: ET_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  const parts = dtf.formatToParts(date);
  const out = {};
  for (const part of parts) {
    if (part.type !== 'literal') out[part.type] = part.value;
  }
  return out;
}

function formatUtcIsoToEtLocalValue(utcIso) {
  if (!utcIso) return '';
  const date = new Date(utcIso);
  if (Number.isNaN(date.getTime())) return '';
  const p = getEtPartsFromDate(date);
  return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}`;
}

function etLocalToUtcIso(localValue) {
  if (!localValue) return null;
  const match = String(localValue).match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/
  );
  if (!match) return null;

  const [, y, mo, d, h, mi] = match;
  const year = Number(y);
  const month = Number(mo);
  const day = Number(d);
  const hour = Number(h);
  const minute = Number(mi);

  // Brute-force a 24h window around a UTC guess to find exact ET wall time.
  const guessUtc = Date.UTC(year, month - 1, day, hour + 5, minute, 0);
  const windowStart = guessUtc - 12 * 60 * 60 * 1000;
  const windowEnd = guessUtc + 12 * 60 * 60 * 1000;

  for (let t = windowStart; t <= windowEnd; t += 60 * 1000) {
    const p = getEtPartsFromDate(new Date(t));
    if (
      Number(p.year) === year &&
      Number(p.month) === month &&
      Number(p.day) === day &&
      Number(p.hour) === hour &&
      Number(p.minute) === minute
    ) {
      return new Date(t).toISOString();
    }
  }

  return null;
}

function sectionSelectHtml(selected) {
  return SECTION_OPTIONS.map((value) => {
    const isSelected = value === selected ? 'selected' : '';
    return `<option value="${value}" ${isSelected}>${value}</option>`;
  }).join('');
}

function normalizeEditorHtml(value) {
  const html = String(value || '').trim();
  return html === '<br>' ? '' : html;
}

function escapeHtmlForEditor(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function applyInlineMarkdownFormatting(text) {
  return String(text || '')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/__(.+?)__/g, '<strong>$1</strong>')
    .replace(/(^|[\s(])\*(?!\*)([^*]+?)\*(?=$|[\s).,!?;:])/g, '$1<em>$2</em>')
    .replace(/(^|[\s(])_(?!_)([^_]+?)_(?=$|[\s).,!?;:])/g, '$1<em>$2</em>');
}

function plainTextToEditorHtml(text) {
  const normalized = String(text || '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .trim();
  if (!normalized) return '';
  const paragraphs = normalized.split(/\n{2,}/).map((chunk) => chunk.trim()).filter(Boolean);
  return paragraphs
    .map((chunk) => {
      const escaped = escapeHtmlForEditor(chunk).replace(/\n/g, '<br>');
      return `<p>${applyInlineMarkdownFormatting(escaped)}</p>`;
    })
    .join('');
}

function normalizeGeneratedContentForEditor(content) {
  const raw = String(content || '').trim();
  if (!raw) return '';
  if (/<\/?[a-z][\s\S]*>/i.test(raw)) return raw;
  return plainTextToEditorHtml(raw);
}

function initializeRichTextEditors(root) {
  if (!root) return;
  root.querySelectorAll('.draft-card').forEach((card) => {
    const textarea = card.querySelector('.field-content');
    const editor = card.querySelector('.field-content-editor');
    if (!textarea || !editor) return;
    editor.innerHTML = textarea.value || '';
  });
}

function getCardContentHtml(card) {
  const textarea = card.querySelector('.field-content');
  const editor = card.querySelector('.field-content-editor');
  if (!textarea) return '';
  if (!editor) return textarea.value;
  const html = normalizeEditorHtml(editor.innerHTML);
  textarea.value = html;
  return html;
}

function applyRichTextCommand(button, card) {
  const editor = card.querySelector('.field-content-editor');
  if (!editor) return;
  const command = String(button.dataset.rteCmd || '');
  const value = button.dataset.rteValue || null;
  if (!command) return;

  editor.focus();
  if (command === 'createLink') {
    const rawUrl = window.prompt('Enter URL');
    if (rawUrl == null) return;
    const url = String(rawUrl).trim();
    if (!url) return;
    document.execCommand(command, false, url);
  } else {
    document.execCommand(command, false, value);
  }
  getCardContentHtml(card);
}

function buildCloudinaryOptimizedUrl(publicId) {
  const safeId = String(publicId || '')
    .split('/')
    .map((segment) => encodeURIComponent(segment))
    .join('/');
  return `https://res.cloudinary.com/${CLOUDINARY_CLOUD_NAME}/image/upload/f_auto,q_auto,c_limit,w_${CLOUDINARY_WIDTH}/${safeId}`;
}

async function uploadImageToCloudinary(file) {
  const form = new FormData();
  form.append('file', file);
  form.append('upload_preset', CLOUDINARY_UPLOAD_PRESET);

  const res = await fetch(
    `https://api.cloudinary.com/v1_1/${CLOUDINARY_CLOUD_NAME}/image/upload`,
    { method: 'POST', body: form }
  );
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error?.message || 'Upload failed');
  }
  if (!data.public_id) {
    throw new Error('Upload succeeded but no public_id returned');
  }
  return {
    optimizedUrl: buildCloudinaryOptimizedUrl(data.public_id),
    secureUrl: data.secure_url || '',
    publicId: data.public_id
  };
}

function setUploadStatus(el, text) {
  if (!el) return;
  el.hidden = !text;
  el.textContent = text || '';
}

async function handleCardImageUpload(card, file) {
  const imageInput = card.querySelector('.field-image');
  const status = card.querySelector('.upload-status');
  const previewWrap = card.querySelector('.upload-preview');
  const previewImg = card.querySelector('.upload-preview img');
  if (!file) return;

  const mime = String(file.type || '');
  if (!mime.startsWith('image/')) {
    setUploadStatus(status, 'Please select an image file.');
    return;
  }

  try {
    setUploadStatus(status, 'Uploading image...');
    const result = await uploadImageToCloudinary(file);
    imageInput.value = result.optimizedUrl;
    if (previewImg) previewImg.src = result.optimizedUrl;
    if (previewWrap) previewWrap.hidden = false;
    setUploadStatus(status, 'Upload complete. Optimized URL applied.');
  } catch (err) {
    setUploadStatus(status, `Upload failed: ${err.message}`);
  }
}

function renderArticles(articles) {
  if (!articles.length) {
    listEl.innerHTML = '<p>No published articles found.</p>';
    return;
  }

  listEl.innerHTML = articles.map((article) => `
    <article class="draft-card" data-id="${article.id}">
      <button class="draft-header draft-toggle btn-reset" type="button">
        <strong>#${article.id} - ${escapeHtml(article.title || '')}</strong>
        <span class="draft-meta">
          section: ${escapeHtml(article.section || '')} |
          image_status: ${escapeHtml(normalizeImageStatus(article))} |
          published: ${escapeHtml(formatDate(article.pubDate))} |
          slug: ${escapeHtml(article.slug || '')}
        </span>
      </button>

      <div class="draft-form article-editor is-collapsed" hidden>
        <div class="headline-options full"></div>
        <div class="full">
          <div class="field-label-row">
            <span>Title</span>
            <span class="inline-tools">
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-headline-gen">Generate headline...</button>
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-headline-rewrite">Rewrite headline...</button>
            </span>
          </div>
          <input class="field-title" type="text" value="${escapeHtml(article.title || '')}" />
        </div>
        <div class="full ai-panel panel-headline-gen" hidden>
          <label>
            Model
            <select class="job-model-headline-gen">
              ${aiModelSelectHtml()}
            </select>
          </label>
          <button type="button" class="btn btn-primary btn-generate-headlines">Run</button>
        </div>
        <div class="full ai-panel panel-headline-rewrite" hidden>
          <div class="ai-step ai-step-model">
            <label>
              Model
              <select class="job-model-rewrite-headline">
                ${rewriteModelSelectHtml()}
              </select>
            </label>
          </div>
          <div class="ai-step ai-step-issues ai-step-issues-headline" hidden>
            <label>
              Reasons (max 3)
              <select class="rewrite-issues-headline" multiple size="4">
                ${rewriteIssueOptionsHtml('headline', 'anthropic')}
              </select>
            </label>
          </div>
          <div class="ai-step ai-step-run ai-step-run-headline" hidden>
            <button type="button" class="btn btn-primary btn-rewrite-headline" disabled>Run</button>
          </div>
        </div>
        <div class="full">
          <div class="field-label-row">
            <span>Description</span>
            <span class="inline-tools">
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-description-gen">Generate description...</button>
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-description-rewrite">Rewrite description...</button>
            </span>
          </div>
          <textarea class="field-description">${escapeHtml(article.description || '')}</textarea>
        </div>
        <div class="full ai-panel panel-description-gen" hidden>
          <label>
            Model
            <select class="job-model-description">
              ${aiModelSelectHtml()}
            </select>
          </label>
          <button type="button" class="btn btn-primary btn-generate-description">Run</button>
        </div>
        <div class="full ai-panel panel-description-rewrite" hidden>
          <div class="ai-step ai-step-model">
            <label>
              Model
              <select class="job-model-rewrite-description">
                ${rewriteModelSelectHtml()}
              </select>
            </label>
          </div>
          <div class="ai-step ai-step-issues ai-step-issues-description" hidden>
            <label>
              Reasons (max 3)
              <select class="rewrite-issues-description" multiple size="4">
                ${rewriteIssueOptionsHtml('description', 'anthropic')}
              </select>
            </label>
          </div>
          <div class="ai-step ai-step-run ai-step-run-description" hidden>
            <button type="button" class="btn btn-primary btn-rewrite-description" disabled>Run</button>
          </div>
        </div>
        <div class="full">
          <div class="field-label-row">
            <span>Research / Notes</span>
            <span class="inline-tools">
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-research-gen">Generate Research...</button>
            </span>
          </div>
          <textarea class="field-research" rows="6">${escapeHtml(article.research || '')}</textarea>
        </div>
        <div class="full ai-panel panel-research-gen" hidden>
          <label>
            Model
            <select class="job-model-research">
              ${aiModelSelectHtml()}
            </select>
          </label>
          <button type="button" class="btn btn-primary btn-generate-research">Run Research</button>
        </div>
        <div class="full">
          <div class="field-label-row">
            <span>Content</span>
          </div>
          <div class="rte-wrap">
            <div class="rte-toolbar" role="toolbar" aria-label="Content formatting">
              <button type="button" class="btn rte-btn" data-rte-cmd="bold"><strong>B</strong></button>
              <button type="button" class="btn rte-btn" data-rte-cmd="italic"><em>I</em></button>
              <button type="button" class="btn rte-btn" data-rte-cmd="underline"><u>U</u></button>
              <button type="button" class="btn rte-btn" data-rte-cmd="insertUnorderedList">Bullets</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="insertOrderedList">Numbers</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="formatBlock" data-rte-value="h2">H2</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="formatBlock" data-rte-value="p">P</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="createLink">Link</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="removeFormat">Clear</button>
              <span class="rte-toolbar-spacer"></span>
              <span class="inline-tools">
                <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-article-gen">Generate article...</button>
                <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-article-rewrite">Rewrite article...</button>
              </span>
            </div>
            <div class="ai-panel ai-panel-inline panel-article-gen" hidden>
              <label>
                Model
                <select class="job-model-article">
                  ${aiModelSelectHtml()}
                </select>
              </label>
              <button type="button" class="btn btn-primary btn-generate-article">Run</button>
            </div>
            <div class="ai-panel ai-panel-inline panel-article-rewrite" hidden>
              <div class="ai-step ai-step-model">
                <label>
                  Model
                  <select class="job-model-rewrite-article">
                    ${rewriteModelSelectHtml()}
                  </select>
                </label>
              </div>
              <div class="ai-step ai-step-issues ai-step-issues-article" hidden>
                <label>
                  Reasons (max 3)
                  <select class="rewrite-issues-article" multiple size="5">
                    ${rewriteIssueOptionsHtml('article', 'anthropic')}
                  </select>
                </label>
              </div>
              <div class="ai-step ai-step-run ai-step-run-article" hidden>
                <button type="button" class="btn btn-primary btn-rewrite-article" disabled>Run</button>
              </div>
            </div>
            <div class="field-content-editor rte-editor" contenteditable="true" role="textbox" aria-multiline="true"></div>
            <textarea class="field-content" hidden>${escapeHtml(article.content || '')}</textarea>
          </div>
        </div>
        <div class="full">
          <div class="field-label-row"><span>Image URL</span> <button type="button" class="btn rte-btn btn-source-image">Auto-Source from Library</button></div>
          <input class="field-image" type="text" value="${escapeHtml(article.image || '')}" />
        </div>
        <div class="full image-uploader">
          <button type="button" class="upload-dropzone btn-reset">
            Drop image here or click to upload
          </button>
          <input class="file-image" type="file" accept="image/*" hidden />
          <p class="upload-hint">Uploads to Cloudinary and auto-fills optimized URL.</p>
          <p class="upload-status" hidden></p>
          <div class="upload-preview" ${article.image ? '' : 'hidden'}>
            <img src="${escapeHtml(article.image || '')}" alt="Uploaded preview" loading="lazy" />
          </div>
        </div>
        <label class="full">
          Image Description / Caption
          <textarea class="field-image-caption">${escapeHtml(article.imageCaption || '')}</textarea>
        </label>
        <label class="full">
          Image Source / Credit
          <input class="field-image-credit" type="text" value="${escapeHtml(article.imageCredit || '')}" />
        </label>
        <label>
          Section
          <select class="field-section">${sectionSelectHtml(article.section)}</select>
        </label>
        <label>
          Beat
          <select class="field-beat">${
            getBeatOptionsForSection(article.section).map(opt =>
              `<option value="${escapeHtml(opt.value)}" ${article.beat === opt.value ? 'selected' : ''}>${escapeHtml(opt.label)}</option>`
            ).join('')
          }</select>
        </label>
        <label>
          Persona
          <select class="field-persona">${
            getPersonasForBeat(article.beat).map(opt =>
              `<option value="${escapeHtml(opt.value)}" ${article.persona === opt.value ? 'selected' : ''}>${escapeHtml(opt.label)}</option>`
            ).join('')
          }</select>
        </label>
        <label>
          Publish Date (ET)
          <input class="field-pubdate" type="datetime-local" value="${escapeHtml(formatUtcIsoToEtLocalValue(article.pubDate))}" />
        </label>
      </div>
      <div class="draft-actions article-editor is-collapsed" hidden>
        <button type="button" class="btn btn-primary btn-save-article">Save Article</button>
        <button type="button" class="btn btn-secondary btn-emergency-replace-image">Emergency Replace Now</button>
        <button type="button" class="btn btn-danger btn-remove-article">Permanently Delete Article</button>
      </div>
    </article>
  `).join('');
  initializeRichTextEditors(listEl);
  listEl.querySelectorAll('.draft-card').forEach((card) => {
    const headlineGenModel = localStorage.getItem('de_job_model_headline_gen') || '';
    const articleModel = localStorage.getItem('de_job_model_article') || '';
    const researchModel = localStorage.getItem('de_job_model_research') || '';
    const descriptionModel = localStorage.getItem('de_job_model_description') || '';
    const rewriteHeadlineModel = localStorage.getItem('de_job_model_rewrite_headline') || '';
    const rewriteArticleModel = localStorage.getItem('de_job_model_rewrite_article') || '';
    const rewriteDescriptionModel = localStorage.getItem('de_job_model_rewrite_description') || '';
    const headlineGenSelect = card.querySelector('.job-model-headline-gen');
    const articleSelect = card.querySelector('.job-model-article');
    const researchSelect = card.querySelector('.job-model-research');
    const descriptionSelect = card.querySelector('.job-model-description');
    const rewriteHeadlineSelect = card.querySelector('.job-model-rewrite-headline');
    const rewriteArticleSelect = card.querySelector('.job-model-rewrite-article');
    const rewriteDescriptionSelect = card.querySelector('.job-model-rewrite-description');
    if (headlineGenSelect && headlineGenModel) headlineGenSelect.value = headlineGenModel;
    if (articleSelect && articleModel) articleSelect.value = articleModel;
    if (researchSelect && researchModel) researchSelect.value = researchModel;
    if (descriptionSelect && descriptionModel) descriptionSelect.value = descriptionModel;
    if (rewriteHeadlineSelect && rewriteHeadlineModel) rewriteHeadlineSelect.value = rewriteHeadlineModel;
    if (rewriteArticleSelect && rewriteArticleModel) rewriteArticleSelect.value = rewriteArticleModel;
    if (rewriteDescriptionSelect && rewriteDescriptionModel) rewriteDescriptionSelect.value = rewriteDescriptionModel;
    syncBeatOptions(card);
    syncPersonaOptions(card);
    syncRewriteIssueSelect(card, 'headline');
    syncRewriteIssueSelect(card, 'article');
    syncRewriteIssueSelect(card, 'description');
  });
}

function articleSearchHaystack(article) {
  const title = String(article.title || '').toLowerCase();
  const section = String(article.section || '').toLowerCase();
  const slug = String(article.slug || '').toLowerCase();
  const imageStatus = String(normalizeImageStatus(article) || '').toLowerCase();
  const iso = String(article.pubDate || '').toLowerCase();
  const pretty = String(formatDate(article.pubDate) || '').toLowerCase();
  return `${title} ${section} ${slug} ${imageStatus} ${iso} ${pretty}`;
}

function applySearchFilter() {
  const q = String(articleSearchInput.value || '').trim().toLowerCase();
  if (!q) {
    renderArticles(loadedArticles);
    articleTotalCountEl.textContent = String(totalAllArticles || lastTotalCount || loadedArticles.length);
    return;
  }
  const filtered = loadedArticles.filter((a) => articleSearchHaystack(a).includes(q));
  renderArticles(filtered);
  articleTotalCountEl.textContent = String(totalAllArticles || lastTotalCount || loadedArticles.length);
}

async function loadArticles() {
  try {
    setMessage('Loading published articles...');
    await loadPersonaDirectory();
    const section = encodeURIComponent(sectionFilterInput.value || 'all');
    const imageStatus = encodeURIComponent(imageStatusFilterInput?.value || 'all');
    const limit = encodeURIComponent(limitInput.value || '50');
    const sort = imageStatus === 'text_only' ? 'text_only_follow_up' : '';
    const data = await apiRequest(`/api/admin-articles?section=${section}&image_status=${imageStatus}&sort=${encodeURIComponent(sort)}&limit=${limit}`);
    loadedArticles = data.articles || [];
    lastTotalCount = Number(data.totalCount || loadedArticles.length || 0);
    totalAllArticles = Number(data.totalAllCount || totalAllArticles || lastTotalCount);
    articleTotalCountEl.textContent = String(totalAllArticles);
    applySearchFilter();
    setMessage(`Loaded ${data.count || 0} article(s) from ${lastTotalCount} in section filter (${totalAllArticles} total published).`);
  } catch (err) {
    setMessage(`Load failed: ${err.message}`);
  }
}

async function showAllArticles() {
  try {
    setMessage('Loading all matching published articles...');
    const section = encodeURIComponent(sectionFilterInput.value || 'all');
    const total = Math.max(25, Math.min(5000, Number(lastTotalCount || 5000)));
    limitInput.value = String(total);
    await loadArticles();
  } catch (err) {
    setMessage(`Show all failed: ${err.message}`);
  }
}

async function saveArticle(card) {
  const id = Number(card.dataset.id);
  const pubDateRaw = card.querySelector('.field-pubdate').value;
  const pubDate = pubDateRaw ? etLocalToUtcIso(pubDateRaw) : null;
  if (pubDateRaw && !pubDate) {
    throw new Error('Invalid ET publish date format');
  }

  const imageValue = card.querySelector('.field-image').value;
  const imageStatus = String(imageValue || '').trim() ? 'with_image' : 'text_only';
  const placementEligible = imageStatus === 'with_image'
    ? ['main', 'top', 'carousel', 'grid', 'sidebar', 'extra_headlines']
    : ['sidebar', 'extra_headlines'];

  await apiRequest('/api/admin-update-article', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id,
      title: card.querySelector('.field-title').value,
      description: card.querySelector('.field-description').value,
      research: card.querySelector('.field-research').value,
      content: getCardContentHtml(card),
      beat: card.querySelector('.field-beat').value,
      persona: card.querySelector('.field-persona').value,
      section: card.querySelector('.field-section').value,
      image: imageValue,
      imageCaption: card.querySelector('.field-image-caption').value,
      imageCredit: card.querySelector('.field-image-credit').value,
      imageStatus,
      placementEligible,
      pubDate
    })
  });
}

function openRemoveModal(articleId) {
  removeTargetArticleId = Number(articleId || 0);
  if (!removeTargetArticleId || !removeModalEl) return;
  if (removeConfirmInput) removeConfirmInput.value = '';
  removeModalEl.hidden = false;
}

function closeRemoveModal() {
  removeTargetArticleId = 0;
  if (!removeModalEl) return;
  removeModalEl.hidden = true;
}

async function removeArticle() {
  const confirmation = String(removeConfirmInput?.value || '').trim();
  if (!removeTargetArticleId) throw new Error('Missing article target');
  if (confirmation !== 'DELETE') {
    throw new Error('Type DELETE to confirm permanent deletion');
  }

  await apiRequest('/api/admin-remove-article', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: removeTargetArticleId
    })
  });
}

async function emergencyReplaceImage(card) {
  const id = Number(card?.dataset?.id || 0);
  if (!id) throw new Error('Missing article id');
  const reason = String(window.prompt('Reason code for immutable audit log (required):', 'editor_emergency_replace') || '').trim();
  if (!reason) throw new Error('Reason code is required');
  const confirmed = window.confirm('Emergency Replace Now will overwrite image fields immediately. Continue?');
  if (!confirmed) return;
  await apiRequest('/api/admin-emergency-image-replace', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ articleId: id, reasonCode: reason })
  });
}

function getJobModelSelection(card, selectorClass) {
  const raw = String(card.querySelector(selectorClass)?.value || '').trim();
  const [provider, ...modelParts] = raw.split(':');
  const model = modelParts.join(':').trim();
  if (!provider || !model) throw new Error('Select a model first');
  return { provider: provider.trim(), model };
}

async function generateResearchForCard(card) {
  const title = String(card.querySelector('.field-title')?.value || '').trim();
  const section = String(card.querySelector('.field-section')?.value || 'local').trim();
  if (!title) throw new Error('Title is required before generating research');
  const { provider, model } = getJobModelSelection(card, '.job-model-research');
  const beat = card.querySelector('.field-beat')?.value || '';
  const persona = card.querySelector('.field-persona')?.value || '';
  localStorage.setItem('de_job_model_research', `${provider}:${model}`);

  const data = await apiRequest('/api/admin-generate-research', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title, section, beat, persona, provider, model })
  });

  const research = String(data?.research || '').trim();
  if (!research) throw new Error('Generation returned empty research');
  const researchField = card.querySelector('.field-research');
  if (researchField) researchField.value = research;
}

async function generateArticleForCard(card) {
  const title = String(card.querySelector('.field-title')?.value || '').trim();
  const section = String(card.querySelector('.field-section')?.value || 'local').trim();
  if (!title) throw new Error('Title is required before generating article');
  const { provider, model } = getJobModelSelection(card, '.job-model-article');
  const beat = card.querySelector('.field-beat')?.value || '';
  const persona = card.querySelector('.field-persona')?.value || '';
  const research = card.querySelector('.field-research')?.value || '';
  localStorage.setItem('de_job_model_article', `${provider}:${model}`);
  localStorage.setItem('de_job_beat_article', beat);
  localStorage.setItem('de_job_persona_article', persona);

  const data = await apiRequest('/api/admin-generate-article', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title, section, beat, persona, research, provider, model })
  });

  const description = String(data?.article?.description || '').trim();
  const content = String(data?.article?.content || '').trim();
  if (!content) throw new Error('Generation returned empty article content');
  const descriptionField = card.querySelector('.field-description');
  const contentEditor = card.querySelector('.field-content-editor');
  const contentTextarea = card.querySelector('.field-content');
  if (descriptionField && description) descriptionField.value = description;
  if (contentEditor) contentEditor.innerHTML = normalizeGeneratedContentForEditor(content);
  if (contentTextarea) contentTextarea.value = content;
}

async function generateDescriptionForCard(card) {
  const title = String(card.querySelector('.field-title')?.value || '').trim();
  const content = getCardContentHtml(card);
  const section = String(card.querySelector('.field-section')?.value || 'local').trim();
  if (!title) throw new Error('Title is required before generating description');
  if (!String(content || '').trim()) throw new Error('Content is required before generating description');
  const { provider, model } = getJobModelSelection(card, '.job-model-description');
  localStorage.setItem('de_job_model_description', `${provider}:${model}`);

  const data = await apiRequest('/api/admin-generate-description', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title, content, section, provider, model })
  });

  const description = String(data?.description || '').trim();
  if (!description) throw new Error('Generation returned empty description');
  const descriptionField = card.querySelector('.field-description');
  if (descriptionField) descriptionField.value = description;
}

async function generateHeadlinesForCard(card) {
  const topic = String(card.querySelector('.field-title')?.value || '').trim();
  const section = String(card.querySelector('.field-section')?.value || 'local').trim();
  const beat = card.querySelector('.field-beat')?.value || '';
  const persona = card.querySelector('.field-persona')?.value || '';
  const { provider, model } = getJobModelSelection(card, '.job-model-headline-gen');
  localStorage.setItem('de_job_model_headline_gen', `${provider}:${model}`);

  const data = await apiRequest('/api/admin-generate-headlines', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ topic, title: topic, section, beat, persona, provider, model })
  });

  const best = String(data?.bestHeadline || '').trim();
  const alternates = Array.isArray(data?.alternates) ? data.alternates.map((v) => String(v || '').trim()).filter(Boolean) : [];
  const titleField = card.querySelector('.field-title');
  if (titleField && best) titleField.value = best;
  setHeadlineOptions(card, [best, ...alternates]);
}

async function sourceImageForCard(card) {
  const title = String(card.querySelector('.field-title')?.value || '').trim();
  const section = String(card.querySelector('.field-section')?.value || 'local').trim();
  if (!title) throw new Error('Title is required to source an image');

  const data = await apiRequest('/api/admin-source-image', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title, section })
  });

  if (!data.imageUrl) throw new Error('No matching image found in library');

  const imageInput = card.querySelector('.field-image');
  const previewWrap = card.querySelector('.upload-preview');
  const previewImg = card.querySelector('.upload-preview img');
  if (imageInput) imageInput.value = data.imageUrl;
  if (previewImg) previewImg.src = data.imageUrl;
  if (previewWrap) previewWrap.hidden = false;
}

async function rewriteCardContent(card, target) {
  const title = String(card.querySelector('.field-title')?.value || '').trim();
  const description = String(card.querySelector('.field-description')?.value || '').trim();
  const content = getCardContentHtml(card);
  const issues = getSelectedIssues(card, target);
  if (issues.length < 1 || issues.length > 3) throw new Error('Select 1 to 3 rewrite issues');
  const { provider, model } = getJobModelSelection(card, `.job-model-rewrite-${target}`);
  localStorage.setItem(`de_job_model_rewrite_${target}`, `${provider}:${model}`);

  const data = await apiRequest('/api/admin-rewrite-content', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ target, title, description, content, issues, provider, model })
  });

  if (target === 'headline') {
    const nextTitle = String(data?.headline || '').trim();
    if (!nextTitle) throw new Error('Rewrite returned empty headline');
    const titleField = card.querySelector('.field-title');
    if (titleField) titleField.value = nextTitle;
    return;
  }
  if (target === 'description') {
    const nextDescription = String(data?.description || '').trim();
    if (!nextDescription) throw new Error('Rewrite returned empty description');
    const descriptionField = card.querySelector('.field-description');
    if (descriptionField) descriptionField.value = nextDescription;
    return;
  }
  const nextContent = String(data?.content || '').trim();
  if (!nextContent) throw new Error('Rewrite returned empty article content');
  const contentEditor = card.querySelector('.field-content-editor');
  const contentTextarea = card.querySelector('.field-content');
  if (contentEditor) contentEditor.innerHTML = normalizeGeneratedContentForEditor(nextContent);
  if (contentTextarea) contentTextarea.value = nextContent;
}

function onListClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  const card = target.closest('.draft-card');
  if (!card) return;
  const button = target.closest('button');
  if (!button) return;

  if (button.classList.contains('ai-action-toggle')) {
    toggleAiPanel(card, String(button.dataset.panel || ''), button);
    return;
  }

  if (button.classList.contains('rte-btn')) {
    applyRichTextCommand(button, card);
    return;
  }

  if (button.classList.contains('draft-toggle')) {
    const editors = card.querySelectorAll('.article-editor');
    const isHidden = editors[0]?.hasAttribute('hidden');
    editors.forEach((el) => {
      if (isHidden) {
        el.removeAttribute('hidden');
        el.classList.remove('is-collapsed');
      } else {
        el.setAttribute('hidden', '');
        el.classList.add('is-collapsed');
      }
    });
    return;
  }

  if (button.classList.contains('btn-save-article')) {
    saveArticle(card)
      .then(async () => {
        await loadArticles();
        setMessage(`Article #${card.dataset.id} saved.`);
        window.scrollTo({ top: 0, behavior: 'smooth' });
      })
      .catch((err) => setMessage(`Save failed: ${err.message}`));
    return;
  }

  if (button.classList.contains('btn-emergency-replace-image')) {
    emergencyReplaceImage(card)
      .then(async () => {
        await loadArticles();
        setMessage(`Article #${card.dataset.id}: emergency replace completed.`);
        window.scrollTo({ top: 0, behavior: 'smooth' });
      })
      .catch((err) => setMessage(`Emergency replace failed: ${err.message}`));
    return;
  }

  if (button.classList.contains('btn-generate-article')) {
    const modelLabel = getSelectedModelLabel(card, '.job-model-article');
    setMessage(`Generating article with ${modelLabel}...`);
    button.disabled = true;
    showLoadingOverlay(card, 'desc-gen', '.field-description', 'Generating description...');
    showLoadingOverlay(card, 'content-gen', '.field-content-editor', 'Generating article...');
    generateArticleForCard(card)
      .then(() => {
        closeAllAiPanels();
        setMessage(`Article #${card.dataset.id}: generated new article copy.`);
      })
      .catch((err) => setMessage(`Generate article failed: ${err.message}`))
      .finally(() => {
        button.disabled = false;
        clearLoadingOverlays(card, ['desc-gen', 'content-gen']);
      });
    return;
  }

  if (button.classList.contains('btn-generate-research')) {
    const modelLabel = getSelectedModelLabel(card, '.job-model-research');
    setMessage(`Generating research with ${modelLabel}...`);
    button.disabled = true;
    showLoadingOverlay(card, 'research-gen', '.field-research', 'Researching...');
    generateResearchForCard(card)
      .then(() => {
        closeAllAiPanels();
        setMessage(`Article #${card.dataset.id}: generated research.`);
      })
      .catch((err) => setMessage(`Generate research failed: ${err.message}`))
      .finally(() => {
        button.disabled = false;
        clearLoadingOverlays(card, ['research-gen']);
      });
    return;
  }

  if (button.classList.contains('btn-generate-headlines')) {
    const modelLabel = getSelectedModelLabel(card, '.job-model-headline-gen');
    setMessage(`Generating headlines with ${modelLabel}...`);
    button.disabled = true;
    generateHeadlinesForCard(card)
      .then(() => {
        closeAllAiPanels();
        setMessage(`Article #${card.dataset.id}: generated headline options.`);
      })
      .catch((err) => setMessage(`Generate headlines failed: ${err.message}`))
      .finally(() => {
        button.disabled = false;
      });
    return;
  }

  if (button.classList.contains('btn-generate-description')) {
    const modelLabel = getSelectedModelLabel(card, '.job-model-description');
    setMessage(`Generating description with ${modelLabel}...`);
    button.disabled = true;
    showLoadingOverlay(card, 'desc-gen', '.field-description', 'Generating description...');
    generateDescriptionForCard(card)
      .then(() => {
        closeAllAiPanels();
        setMessage(`Article #${card.dataset.id}: generated description.`);
      })
      .catch((err) => setMessage(`Generate description failed: ${err.message}`))
      .finally(() => {
        button.disabled = false;
        clearLoadingOverlays(card, ['desc-gen']);
      });
    return;
  }

  if (button.classList.contains('btn-rewrite-headline')) {
    const modelLabel = getSelectedModelLabel(card, '.job-model-rewrite-headline');
    setMessage(`Rewriting headline with ${modelLabel}...`);
    button.disabled = true;
    rewriteCardContent(card, 'headline')
      .then(() => {
        closeAllAiPanels();
        setMessage(`Article #${card.dataset.id}: headline rewritten.`);
      })
      .catch((err) => setMessage(`Rewrite headline failed: ${err.message}`))
      .finally(() => {
        button.disabled = false;
      });
    return;
  }

  if (button.classList.contains('btn-rewrite-description')) {
    const modelLabel = getSelectedModelLabel(card, '.job-model-rewrite-description');
    setMessage(`Rewriting description with ${modelLabel}...`);
    button.disabled = true;
    showLoadingOverlay(card, 'desc-gen', '.field-description', 'Rewriting description...');
    rewriteCardContent(card, 'description')
      .then(() => {
        closeAllAiPanels();
        setMessage(`Article #${card.dataset.id}: description rewritten.`);
      })
      .catch((err) => setMessage(`Rewrite description failed: ${err.message}`))
      .finally(() => {
        button.disabled = false;
        clearLoadingOverlays(card, ['desc-gen']);
      });
    return;
  }

  if (button.classList.contains('btn-rewrite-article')) {
    const modelLabel = getSelectedModelLabel(card, '.job-model-rewrite-article');
    setMessage(`Rewriting article with ${modelLabel}...`);
    button.disabled = true;
    showLoadingOverlay(card, 'content-gen', '.field-content-editor', 'Rewriting article...');
    rewriteCardContent(card, 'article')
      .then(() => {
        closeAllAiPanels();
        setMessage(`Article #${card.dataset.id}: article rewritten.`);
      })
      .catch((err) => setMessage(`Rewrite article failed: ${err.message}`))
      .finally(() => {
        button.disabled = false;
        clearLoadingOverlays(card, ['content-gen']);
      });
    return;
  }

  if (button.classList.contains('btn-remove-article')) {
    openRemoveModal(card.dataset.id);
    return;
  }

  if (button.classList.contains('upload-dropzone')) {
    const fileInput = card.querySelector('.file-image');
    if (fileInput) fileInput.click();
    return;
  }

  if (button.classList.contains('btn-headline-option')) {
    const titleField = card.querySelector('.field-title');
    const headline = String(button.dataset.headline || '').trim();
    if (titleField && headline) titleField.value = headline;
  }

  if (button.classList.contains('btn-source-image')) {
    setMessage('Searching image library...');
    button.disabled = true;
    sourceImageForCard(card)
      .then(() => setMessage('Image sourced from library.'))
      .catch((err) => setMessage(`Source image failed: ${err.message}`))
      .finally(() => { button.disabled = false; });
  }
}

function onRemoveConfirm() {
  removeArticle()
    .then(async () => {
      const id = removeTargetArticleId;
      closeRemoveModal();
      setMessage(`Article #${id} permanently deleted.`);
      scrollToTopStatus();
      await loadArticles();
    })
    .catch((err) => {
      setMessage(`Remove failed: ${err.message}`);
      scrollToTopStatus();
    });
}

function onListChange(event) {
  const input = event.target instanceof Element ? event.target : null;
  if (!input) return;
  if (input.classList.contains('job-model-headline-gen')) {
    localStorage.setItem('de_job_model_headline_gen', String(input.value || ''));
    return;
  }
  if (input.classList.contains('job-model-article')) {
    localStorage.setItem('de_job_model_article', String(input.value || ''));
    return;
  }
  if (input.classList.contains('job-model-research')) {
    localStorage.setItem('de_job_model_research', String(input.value || ''));
    return;
  }
  if (input.classList.contains('job-model-description')) {
    localStorage.setItem('de_job_model_description', String(input.value || ''));
    return;
  }
  if (input.classList.contains('field-section')) {
    const card = input.closest('.draft-card');
    if (card) {
      syncBeatOptions(card);
      syncPersonaOptions(card);
    }
    return;
  }
  if (input.classList.contains('field-beat')) {
    const card = input.closest('.draft-card');
    if (card) syncPersonaOptions(card);
    return;
  }
  if (input.classList.contains('job-model-rewrite-headline')) {
    localStorage.setItem('de_job_model_rewrite_headline', String(input.value || ''));
    const card = input.closest('.draft-card');
    if (card) syncRewriteIssueSelect(card, 'headline');
    return;
  }
  if (input.classList.contains('job-model-rewrite-article')) {
    localStorage.setItem('de_job_model_rewrite_article', String(input.value || ''));
    const card = input.closest('.draft-card');
    if (card) syncRewriteIssueSelect(card, 'article');
    return;
  }
  if (input.classList.contains('job-model-rewrite-description')) {
    localStorage.setItem('de_job_model_rewrite_description', String(input.value || ''));
    const card = input.closest('.draft-card');
    if (card) syncRewriteIssueSelect(card, 'description');
    return;
  }
  if (input.classList.contains('rewrite-issues-headline') || input.classList.contains('rewrite-issues-article') || input.classList.contains('rewrite-issues-description')) {
    enforceIssueLimit(input, 3);
    const card = input.closest('.draft-card');
    if (card && input.classList.contains('rewrite-issues-headline')) {
      const runBtn = card.querySelector('.btn-rewrite-headline');
      if (runBtn) runBtn.disabled = getSelectedIssues(card, 'headline').length < 1;
    }
    if (card && input.classList.contains('rewrite-issues-description')) {
      const runBtn = card.querySelector('.btn-rewrite-description');
      if (runBtn) runBtn.disabled = getSelectedIssues(card, 'description').length < 1;
    }
    if (card && input.classList.contains('rewrite-issues-article')) {
      const runBtn = card.querySelector('.btn-rewrite-article');
      if (runBtn) runBtn.disabled = getSelectedIssues(card, 'article').length < 1;
    }
    return;
  }
  if (!input.classList.contains('file-image')) return;
  const card = input.closest('.draft-card');
  if (!card) return;
  const file = input.files && input.files[0];
  handleCardImageUpload(card, file);
}

function onListDragOver(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  const zone = target.closest('.upload-dropzone');
  if (!zone) return;
  event.preventDefault();
  zone.classList.add('is-drag-over');
}

function onListDragLeave(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  const zone = target.closest('.upload-dropzone');
  if (!zone) return;
  zone.classList.remove('is-drag-over');
}

function onListDrop(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  const zone = target.closest('.upload-dropzone');
  if (!zone) return;
  event.preventDefault();
  zone.classList.remove('is-drag-over');

  const card = zone.closest('.draft-card');
  const file = event.dataTransfer?.files?.[0];
  if (card && file) {
    handleCardImageUpload(card, file);
  }
}

saveTokenBtn.addEventListener('click', saveToken);
loadArticlesBtn.addEventListener('click', loadArticles);
unlockAdminBtn.addEventListener('click', unlock);
listEl.addEventListener('click', onListClick);
listEl.addEventListener('change', onListChange);
listEl.addEventListener('dragover', onListDragOver);
listEl.addEventListener('dragleave', onListDragLeave);
listEl.addEventListener('drop', onListDrop);
articleSearchInput.addEventListener('input', applySearchFilter);
showAllBtn.addEventListener('click', showAllArticles);
if (imageStatusFilterInput) {
  imageStatusFilterInput.addEventListener('change', () => {
    loadArticles().catch((err) => setMessage(`Load failed: ${err.message}`));
  });
}
document.addEventListener('click', (event) => {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  if (target.closest('.ai-panel') || target.closest('.ai-action-toggle')) return;
  closeAllAiPanels();
});
if (removeConfirmBtn) removeConfirmBtn.addEventListener('click', onRemoveConfirm);
if (removeCancelBtn) removeCancelBtn.addEventListener('click', closeRemoveModal);
if (removeModalEl) {
  removeModalEl.addEventListener('click', (event) => {
    const target = event.target;
    if (target && target.classList && target.classList.contains('admin-modal-backdrop')) {
      closeRemoveModal();
    }
  });
}
limitInput.addEventListener('input', () => {
  const raw = Number(limitInput.value || 50);
  const stepped = Math.max(25, Math.min(5000, Math.round(raw / 25) * 25));
  if (stepped !== raw) limitInput.value = String(stepped);
});

loadToken();
setLockState(sessionStorage.getItem('de_admin_unlocked_articles') === '1');
if (unlocked && getToken()) {
  loadArticles().catch((err) => setMessage(`Load failed: ${err.message}`));
}
