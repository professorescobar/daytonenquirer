const lockSection = document.getElementById('settings-lock');
const appSection = document.getElementById('settings-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');
const tokenInput = document.getElementById('admin-token');
const saveTokenBtn = document.getElementById('save-token-btn');
const messageEl = document.getElementById('settings-message');

let unlocked = false;
let signalPage = 1;
let signalPageSize = 25;
let signalSortBy = 'created_at';
let signalSortDir = 'desc';
let currentSignalFilters = {
  personaId: '',
  action: '',
  reviewDecision: ''
};
let latestPipelineRuns = [];
let loadedPersonaDisplayNames = new Map();
let loadedPersonas = [];
let customBeatsBySection = {};
let promptLayersByKey = new Map();
const ADD_NEW_BEAT_VALUE = '__add_new__';
const PROMPT_SCOPE_GLOBAL = 'global';
const PROMPT_SCOPE_SECTION = 'section';

function setMessage(text) {
  if (!messageEl) return;
  messageEl.hidden = !text;
  messageEl.textContent = text || '';
}

function getToken() {
  return (tokenInput.value || '').trim();
}

function getBrowserTimezone() {
  try {
    const tz = Intl?.DateTimeFormat?.().resolvedOptions?.().timeZone;
    return String(tz || '').trim();
  } catch (_) {
    return '';
  }
}

async function syncAdminTimezoneFromBrowser() {
  const timezone = getBrowserTimezone();
  if (!timezone) return;

  const syncKey = 'de_admin_timezone_synced';
  const alreadySynced = localStorage.getItem(syncKey) || '';
  if (alreadySynced === timezone) return;

  try {
    await apiRequest('/api/admin-timezone', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ timezone })
    });
    localStorage.setItem(syncKey, timezone);
  } catch (err) {
    console.warn('Timezone sync skipped:', err?.message || err);
  }
}

function setLockState(value) {
  unlocked = value;
  lockSection.hidden = value;
  appSection.hidden = !value;
}

const CLOUDINARY_CLOUD_NAME = 'dtlkzlp87';
const CLOUDINARY_UPLOAD_PRESET = 'dayton-enquirer';
const CLOUDINARY_WIDTH = 1600;
const TOPIC_ENGINE_STAGES = [
  'topic_qualification',
  'research_discovery',
  'evidence_extraction',
  'story_planning',
  'draft_writing',
  'final_review'
];
const STAGE_LABELS = {
  topic_qualification: 'Topic Qualification',
  quota_pacing: 'Quota + Pacing',
  research_discovery: 'Research Discovery',
  evidence_extraction: 'Evidence Extraction',
  story_planning: 'Story Planning',
  draft_writing: 'Draft Writing',
  final_review: 'Final Review'
};
const STAGE_EXPLANATIONS = {
  topic_qualification: {
    summary: 'Decides whether this lead should be ignored, monitored, or moved forward.',
    details: [
      'Checks if the lead looks like a duplicate or weak local fit.',
      'Applies policy and confidence guardrails before any expensive stages.',
      'Outputs a clear action: reject, watch, or promote.'
    ]
  },
  research_discovery: {
    summary: 'Collects the best supporting sources for this story candidate.',
    details: [
      'Builds targeted search queries from the lead context.',
      'Runs external retrieval and ranks source quality.',
      'Stores a focused source set for the next phase.'
    ]
  },
  evidence_extraction: {
    summary: 'Turns source material into traceable evidence claims.',
    details: [
      'Extracts claims that can be tied to specific source URLs.',
      'Keeps attribution so editorial review is auditable.',
      'Stores structured evidence for planning and drafting.'
    ]
  },
  story_planning: {
    summary: 'Builds a clear article blueprint before writing starts.',
    details: [
      'Defines the angle, section order, and what matters most.',
      'Keeps the plan aligned with verified evidence.',
      'Creates stable inputs for draft generation.'
    ]
  },
  draft_writing: {
    summary: 'Generates the first complete draft for editorial review.',
    details: [
      'Writes from approved plan + evidence constraints.',
      'Applies persona voice and local framing.',
      'Stores draft output for final review.'
    ]
  },
  final_review: {
    summary: 'Runs final quality control before publish gating.',
    details: [
      'Checks clarity, factual grounding, and policy alignment.',
      'Can approve, request edits, or block publication.',
      'Sets final publish readiness status.'
    ]
  }
};
const HARD_CODED_STAGE_STACK = {
  topic_qualification: {
    runnerType: 'llm',
    provider: 'google',
    modelOrEndpoint: 'gemini-1.5-flash'
  },
  research_discovery: {
    runnerType: 'api_workflow',
    provider: 'tavily',
    modelOrEndpoint: 'https://api.tavily.com/search'
  },
  evidence_extraction: {
    runnerType: 'llm',
    provider: 'google',
    modelOrEndpoint: 'gemini-1.5-pro'
  },
  story_planning: {
    runnerType: 'llm',
    provider: 'openai',
    modelOrEndpoint: 'gpt-4o-mini'
  },
  draft_writing: {
    runnerType: 'llm',
    provider: 'openai',
    modelOrEndpoint: 'gpt-4o-mini'
  },
  final_review: {
    runnerType: 'llm',
    provider: 'openai',
    modelOrEndpoint: 'gpt-4o'
  }
};
const DRAFT_WRITING_PROVIDER_OPTIONS = ['openai', 'anthropic', 'gemini', 'grok'];
const DRAFT_WRITING_PROVIDER_LABELS = {
  anthropic: 'Anthropic',
  openai: 'OpenAI',
  gemini: 'Gemini',
  grok: 'Grok'
};
const DEFAULT_DRAFT_WRITING_MODEL_BY_PROVIDER = {
  openai: 'gpt-4o-mini',
  anthropic: 'claude-haiku-4-5',
  gemini: 'gemini-3.1-flash-lite-preview',
  grok: 'grok-4-1-fast-non-reasoning'
};
let loadedDraftWritingModelByProvider = { ...DEFAULT_DRAFT_WRITING_MODEL_BY_PROVIDER };

const BEAT_OPTIONS_BY_SECTION = {
  local: [{ value: 'general-local', label: 'General Local' }],
  national: [{ value: 'general-national', label: 'General National' }],
  world: [{ value: 'general-world', label: 'General World' }],
  business: [
    { value: 'general-business', label: 'General Business' },
    { value: 'local-business', label: 'Local Business' }
  ],
  sports: [
    { value: 'general-sports', label: 'General Sports' },
    { value: 'high-school', label: 'High School' }
  ],
  health: [
    { value: 'general-health', label: 'General Health' },
    { value: 'local-health', label: 'Local Health' }
  ],
  entertainment: [
    { value: 'general-entertainment', label: 'General Entertainment' },
    { value: 'local-entertainment', label: 'Local Entertainment' },
    { value: 'gaming', label: 'Gaming' }
  ],
  technology: [
    { value: 'general-technology', label: 'General Technology' },
    { value: 'local-tech', label: 'Local Tech' },
    { value: 'ai', label: 'AI' }
  ]
};

const PERSONA_OPTIONS_BY_BEAT = {
  'general-local': [{ value: 'local-reporter', label: 'Local Reporter' }],
  'general-national': [{ value: 'national-correspondent', label: 'National Correspondent' }],
  'general-world': [{ value: 'foreign-correspondent', label: 'Foreign Correspondent' }],
  'general-business': [{ value: 'business-reporter', label: 'Business Reporter' }],
  'local-business': [{ value: 'local-business-reporter', label: 'Local Business Reporter' }],
  'general-sports': [{ value: 'sports-reporter', label: 'Sports Reporter' }],
  'high-school': [{ value: 'local-sports-writer', label: 'Local Sports Writer' }],
  'general-health': [{ value: 'health-science-reporter', label: 'Health & Science Reporter' }],
  'local-health': [{ value: 'local-health-reporter', label: 'Local Health Reporter' }],
  'general-entertainment': [{ value: 'entertainment-reporter', label: 'Entertainment Reporter' }],
  'local-entertainment': [{ value: 'local-culture-critic', label: 'Local Culture Critic' }],
  gaming: [{ value: 'tsuki-tamara', label: 'Tsuki Tamara (Gaming Journalist)' }],
  'general-technology': [{ value: 'tech-reporter', label: 'Tech Reporter' }],
  'local-tech': [{ value: 'local-tech-reporter', label: 'Local Tech Reporter' }],
  ai: [{ value: 'ai-future-tech-analyst', label: 'AI & Future Tech Analyst' }]
};

function getAllDefinedPersonas() {
  const beatToSection = new Map();
  for (const [section, beats] of Object.entries(BEAT_OPTIONS_BY_SECTION)) {
    for (const beat of getBeatOptionsForSection(section)) {
      beatToSection.set(beat.value, section);
    }
  }

  const personas = [];
  const seen = new Set();
  for (const [beat, entries] of Object.entries(PERSONA_OPTIONS_BY_BEAT)) {
    for (const persona of entries) {
      if (seen.has(persona.value)) continue;
      seen.add(persona.value);
      personas.push({
        id: persona.value,
        label: persona.label,
        beat,
        section: beatToSection.get(beat) || 'local'
      });
    }
  }
  for (const row of loadedPersonas) {
    const id = cleanText(row?.id || '', 255);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const section = cleanText(row?.section || beatToSection.get(row?.beat) || 'local', 60).toLowerCase() || 'local';
    const beat = cleanText(row?.beat || (getBeatOptionsForSection(section)[0]?.value || 'general-local'), 80);
    const dbLabel = cleanText(row?.displayName || '', 200);
    personas.push({
      id,
      label: dbLabel || id,
      beat,
      section
    });
  }
  return personas;
}

function getDraftWritingModelByProvider() {
  return loadedDraftWritingModelByProvider && typeof loadedDraftWritingModelByProvider === 'object'
    ? loadedDraftWritingModelByProvider
    : DEFAULT_DRAFT_WRITING_MODEL_BY_PROVIDER;
}

function buildCloudinaryOptimizedUrl(publicId) {
  const safeId = String(publicId || '')
    .split('/')
    .map((segment) => encodeURIComponent(segment))
    .join('/');
  return `https://res.cloudinary.com/${CLOUDINARY_CLOUD_NAME}/image/upload/f_auto,q_auto,c_limit,w_${CLOUDINARY_WIDTH}/${safeId}`;
}

function formatDate(dateString) {
  if (!dateString) return '';
  const d = new Date(dateString);
  if (Number.isNaN(d.getTime())) return String(dateString);
  return d.toLocaleString();
}

function escapeHtml(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function cleanText(value, max = 5000) {
  return String(value || '').trim().slice(0, max);
}

function promptLayerKey(scopeType, stageName, section) {
  const scope = cleanText(scopeType, 40).toLowerCase();
  const stage = cleanText(stageName, 120).toLowerCase();
  const sectionKey = scope === PROMPT_SCOPE_SECTION
    ? (cleanText(section, 120).toLowerCase() || 'local')
    : '';
  return `${scope}::${stage}::${sectionKey}`;
}

function getPromptLayer(scopeType, stageName, section) {
  const key = promptLayerKey(scopeType, stageName, section);
  return promptLayersByKey.get(key) || {
    stageName,
    scopeType,
    section: scopeType === PROMPT_SCOPE_SECTION ? section : null,
    promptTemplate: '',
    version: null
  };
}

function setPromptLayers(rows) {
  promptLayersByKey = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const scopeType = cleanText(row?.scopeType, 40).toLowerCase();
    const stageName = cleanText(row?.stageName, 120).toLowerCase();
    if (!scopeType || !stageName) continue;
    const section = scopeType === PROMPT_SCOPE_SECTION
      ? (cleanText(row?.section, 120).toLowerCase() || null)
      : null;
    const key = promptLayerKey(scopeType, stageName, section);
    promptLayersByKey.set(key, {
      id: Number(row?.id || 0) || null,
      stageName,
      scopeType,
      section,
      promptTemplate: cleanText(row?.promptTemplate || '', 50000),
      version: Number(row?.version || 0) || null,
      updatedAt: row?.updatedAt || null
    });
  }
}

function titleCaseSlug(value) {
  return String(value || '')
    .replace(/-/g, ' ')
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function getStaticBeatOptionsForSection(section) {
  return BEAT_OPTIONS_BY_SECTION[section] || BEAT_OPTIONS_BY_SECTION.local || [];
}

function getBeatOptionsForSection(section) {
  const key = cleanText(section, 80).toLowerCase() || 'local';
  const merged = new Map();
  for (const option of getStaticBeatOptionsForSection(key)) {
    if (!option?.value || !option?.label) continue;
    merged.set(option.value, { value: option.value, label: option.label });
  }
  for (const option of customBeatsBySection[key] || []) {
    if (!option?.value || !option?.label) continue;
    merged.set(option.value, { value: option.value, label: option.label });
  }
  return Array.from(merged.values());
}

function buildCustomBeatsBySection(personas) {
  const result = {};
  for (const row of Array.isArray(personas) ? personas : []) {
    const section = cleanText(row?.section || 'local', 80).toLowerCase() || 'local';
    const beat = cleanText(row?.beat || '', 120).toLowerCase();
    if (!beat) continue;
    const staticValues = new Set(getStaticBeatOptionsForSection(section).map((item) => item.value));
    if (staticValues.has(beat)) continue;
    if (!result[section]) result[section] = [];
    if (result[section].some((item) => item.value === beat)) continue;
    result[section].push({ value: beat, label: titleCaseSlug(beat) });
  }
  return result;
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

async function handlePersonaImageUpload(card, file) {
  const imageInput = card.querySelector('.field-avatar-url');
  const status = card.querySelector('.upload-status');
  const previewImg = card.querySelector('.persona-avatar-preview img');
  if (!file) return;

  const mime = String(file.type || '');
  if (!mime.startsWith('image/')) {
    status.textContent = 'Please select an image file.';
    return;
  }

  try {
    status.textContent = 'Uploading...';
    const result = await uploadImageToCloudinary(file);
    imageInput.value = result.optimizedUrl;
    if (previewImg) previewImg.src = result.optimizedUrl;
    status.textContent = 'Upload complete. Save to apply.';
  } catch (err) {
    status.textContent = `Upload failed: ${err.message}`;
  }
}

async function handlePersonaFallbackImageUpload(card, file) {
  const imageInput = card.querySelector('.field-image-fallback-asset-url');
  const publicIdInput = card.querySelector('.field-image-fallback-cloudinary-public-id');
  const status = card.querySelector('.fallback-upload-status');
  if (!file || !imageInput || !publicIdInput || !status) return;

  const mime = String(file.type || '');
  if (!mime.startsWith('image/')) {
    status.textContent = 'Please drop/select an image file.';
    return;
  }

  try {
    status.textContent = 'Uploading fallback image...';
    const result = await uploadImageToCloudinary(file);
    imageInput.value = result.optimizedUrl || result.secureUrl || '';
    publicIdInput.value = result.publicId || '';
    status.textContent = 'Fallback image ready. Save persona to apply.';
  } catch (err) {
    status.textContent = `Fallback upload failed: ${err.message}`;
  }
}

function injectPersonaStyles() {
  const styleId = 'persona-settings-styles';
  if (document.getElementById(styleId)) return;
  const style = document.createElement('style');
  style.id = styleId;
  style.textContent = `
    .persona-editor-grid {
      display: grid;
      grid-template-columns: 120px 1fr;
      gap: 1.5rem;
      margin-bottom: 1rem;
    }
    .persona-avatar-preview {
      width: 100px;
      height: 100px;
      border-radius: 50%;
      overflow: hidden;
      background: #f0f0f0;
      margin-bottom: 0.5rem;
      border: 1px solid #ccc;
    }
    .persona-avatar-preview img {
      width: 100%;
      height: 100%;
      object-fit: cover;
    }
    .persona-avatar-editor {
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }
    .persona-card h3 {
      margin-top: 0;
      border-bottom: 1px solid #eee;
      padding-bottom: 0.5rem;
      margin-bottom: 1rem;
    }
    .persona-header-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 0.5rem;
      align-items: center;
    }
    .persona-header-row .draft-header {
      margin: 0;
    }
    .persona-header-row .btn-rename-persona {
      white-space: nowrap;
    }
    .persona-rename-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 0.5rem;
      align-items: center;
      margin-top: 0.5rem;
    }
    .persona-rename-row[hidden] {
      display: none !important;
    }
    .persona-rename-actions {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      justify-content: flex-end;
    }
    .persona-summary {
      color: #555;
      margin-top: -0.5rem;
      margin-bottom: 1rem;
      font-size: 0.9rem;
    }
    .new-persona-beat-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 0.5rem;
      align-items: end;
    }
    .beat-flyout {
      margin-top: 0.6rem;
      border: 1px solid #d5deec;
      border-radius: 8px;
      background: #fff;
      padding: 0.65rem;
    }
    .beat-flyout-row {
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 0.45rem;
      align-items: end;
    }
    .beat-flyout .draft-meta {
      margin: 0.35rem 0 0 0;
    }
    .persona-autonomy-toggle {
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      margin-top: 0.75rem;
      font-weight: 600;
    }
    .persona-autonomy-toggle input {
      width: auto;
      margin: 0;
    }
    .workflow-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0.85rem;
      margin-top: 1rem;
    }
    .workflow-grid label {
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      font-weight: 600;
    }
    .workflow-grid .workflow-wide {
      grid-column: 1 / -1;
    }
    .workflow-stage-list {
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      margin-top: 1rem;
    }
    .workflow-stage {
      border: 1px solid #ddd;
      background: #fff;
      border-radius: 6px;
      padding: 0.75rem;
    }
    .workflow-stage-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.75rem;
      cursor: pointer;
      width: 100%;
      text-align: left;
    }
    .workflow-stage h4 {
      margin: 0 0 0.6rem 0;
      font-size: 0.96rem;
    }
    .workflow-stage-summary {
      color: #555;
      font-size: 0.82rem;
      margin-top: -0.45rem;
    }
    .workflow-stage .workflow-stage-top {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 0.65rem;
      gap: 0.75rem;
    }
    .workflow-stage-body[hidden] {
      display: none !important;
    }
    .persona-quick-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      margin-top: 0.5rem;
      margin-bottom: 0.25rem;
    }
    .btn-xs {
      padding: 0.35rem 0.6rem;
      font-size: 0.82rem;
    }
    .workflow-stage .workflow-stage-enabled {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      font-size: 0.9rem;
      font-weight: 600;
      white-space: nowrap;
    }
    .workflow-stage .workflow-stage-enabled input {
      width: auto;
      margin: 0;
    }
    .workflow-stage-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0.75rem;
    }
    .workflow-stage-grid label {
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      font-weight: 600;
    }
    .workflow-stage-grid .workflow-wide {
      grid-column: 1 / -1;
    }
    .field-layer-prompt[readonly],
    .field-final-prompt[readonly] {
      background: #f7f8fb;
    }
    .prompt-layer-editor textarea,
    .final-prompt-body textarea {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 0.83rem;
      line-height: 1.35;
    }
    .final-prompt-breakdown details {
      margin-bottom: 0.45rem;
      border: 1px solid #ddd;
      border-radius: 6px;
      background: #fff;
      padding: 0.35rem 0.5rem;
    }
    .final-prompt-breakdown pre {
      white-space: pre-wrap;
      margin: 0.45rem 0 0 0;
      font-size: 0.8rem;
      line-height: 1.35;
    }
    .time-input-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 0.4rem;
      align-items: center;
    }
    .time-input-row select {
      min-width: 4.6rem;
    }
    .stage-explainer p {
      margin: 0 0 0.35rem 0;
      font-size: 0.92rem;
    }
    .stage-explainer ul {
      margin: 0;
      padding-left: 1rem;
      color: #445;
      font-size: 0.86rem;
    }
    .stage-explainer li {
      margin: 0.15rem 0;
    }
    .section-card {
      background: #fdfdfd;
    }
    .persona-advanced {
      margin-top: 0.85rem;
      border: 1px solid #ddd;
      border-radius: 6px;
      background: #fbfbfb;
      padding: 0.65rem;
    }
    .persona-advanced-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
      width: 100%;
      text-align: left;
    }
    .persona-advanced-body[hidden] {
      display: none !important;
    }
    .persona-advanced-body {
      margin-top: 0.75rem;
    }
    .section-title {
      font-size: 1rem;
      margin: 0;
    }
    .persona-nested-list {
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      margin-top: 0.65rem;
    }
    .signals-toolbar {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 0.65rem;
      margin-bottom: 0.8rem;
    }
    .signals-toolbar label {
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      font-weight: 600;
    }
    .signals-summary {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.5rem;
      margin-bottom: 0.8rem;
    }
    .signals-summary .summary-card {
      border: 1px solid #ddd;
      background: #fff;
      padding: 0.55rem;
    }
    .signals-list {
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
    }
    .signal-title {
      margin: 0 0 0.35rem 0;
      font-size: 0.98rem;
    }
    .signal-meta {
      color: #555;
      font-size: 0.86rem;
      margin: 0.15rem 0;
    }
    .signal-flags {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      margin-top: 0.35rem;
    }
    .signal-flag {
      border: 1px solid #ddd;
      border-radius: 999px;
      padding: 0.15rem 0.5rem;
      font-size: 0.75rem;
      background: #fff;
    }
    .signal-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 0.45rem;
      margin-top: 0.65rem;
    }
    .pagination-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
      margin-top: 0.65rem;
    }
    .persona-pipeline-runs {
      margin-top: 0.85rem;
      border: 1px solid #ddd;
      border-radius: 6px;
      background: #fbfbfb;
      padding: 0.65rem;
    }
    .persona-pipeline-runs-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
      width: 100%;
      text-align: left;
    }
    .persona-pipeline-runs-body[hidden] {
      display: none !important;
    }
    .persona-pipeline-runs-body {
      margin-top: 0.75rem;
    }
    .persona-pipeline-runs-list {
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
    }
    .pipeline-run-status {
      display: inline-flex;
      align-items: center;
      border: 1px solid #ddd;
      border-radius: 999px;
      padding: 0.18rem 0.55rem;
      font-size: 0.76rem;
      background: #fff;
      text-transform: lowercase;
    }
    .pipeline-run-status.is-completed {
      background: #ecfdf3;
      border-color: #b7ebce;
      color: #1f7a44;
    }
    .pipeline-run-status.is-in-progress {
      background: #fff9e8;
      border-color: #f1d289;
      color: #8e6407;
    }
    .pipeline-run-status.is-failed {
      background: #fff0f0;
      border-color: #f2b4b4;
      color: #a32828;
    }
    .pipeline-run-status.is-pending {
      background: #f7f7f7;
      border-color: #ddd;
      color: #555;
    }
    .pipeline-run-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.6rem;
    }
    .pipeline-run-title {
      margin: 0;
      font-size: 0.98rem;
    }
    .pipeline-stages {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.45rem;
      margin-top: 0.65rem;
    }
    .pipeline-stage-chip {
      border: 1px solid #ddd;
      border-radius: 6px;
      padding: 0.45rem;
      background: #fff;
    }
    .pipeline-stage-chip h5 {
      margin: 0 0 0.35rem 0;
      font-size: 0.82rem;
    }
    .pipeline-stage-details {
      margin-top: 0.65rem;
      border-top: 1px solid #eee;
      padding-top: 0.65rem;
    }
    .pipeline-stage-details details {
      margin-bottom: 0.45rem;
    }
    .pipeline-stage-detail-item {
      border: 1px solid #eee;
      border-radius: 6px;
      padding: 0.45rem;
      margin: 0.35rem 0;
      background: #fff;
    }
    .pipeline-stage-detail-item p {
      margin: 0.2rem 0;
    }
    @media (max-width: 600px) {
      .persona-editor-grid {
        grid-template-columns: 1fr;
      }
      .persona-avatar-preview {
        margin: 0 auto 0.5rem auto;
      }
      .workflow-grid,
      .workflow-stage-grid {
        grid-template-columns: 1fr;
      }
      .signals-toolbar {
        grid-template-columns: 1fr;
      }
    }
    html[data-theme="dark"] .persona-card .workflow-stage,
    html[data-theme="dark"] .persona-card .persona-advanced,
    html[data-theme="dark"] .persona-card .persona-pipeline-runs,
    html[data-theme="dark"] .section-card,
    html[data-theme="dark"] .section-card > .section-editor,
    html[data-theme="dark"] .persona-card .workflow-stage .workflow-stage-grid,
    html[data-theme="dark"] .persona-card .workflow-stage .workflow-stage-top,
    html[data-theme="dark"] .section-prompt-layers,
    html[data-theme="dark"] .section-prompt-layers .persona-advanced-body,
    html[data-theme="dark"] .section-prompt-layers .workflow-stage,
    html[data-theme="dark"] .section-prompt-layers .workflow-stage .workflow-stage-grid,
    html[data-theme="dark"] .section-prompt-layers .workflow-stage .workflow-stage-top,
    html[data-theme="dark"] #global-prompt-layers-list .workflow-stage {
      background: #1a1f2b !important;
      border-color: #3a455b !important;
      color: #e6edf9 !important;
    }
    html[data-theme="dark"] .field-layer-prompt[readonly],
    html[data-theme="dark"] .field-final-prompt[readonly] {
      background: #20283a !important;
      color: #dbe6ff !important;
      border-color: #3a455b !important;
    }
    html[data-theme="dark"] .final-prompt-breakdown details {
      background: #1a1f2b !important;
      border-color: #3a455b !important;
      color: #e6edf9 !important;
    }
    html[data-theme="dark"] .persona-card .workflow-stage-summary,
    html[data-theme="dark"] .persona-card .signal-meta,
    html[data-theme="dark"] .persona-card .persona-summary,
    html[data-theme="dark"] .section-card .draft-meta,
    html[data-theme="dark"] .section-card .signal-meta,
    html[data-theme="dark"] .section-prompt-layers .workflow-stage-summary,
    html[data-theme="dark"] .section-prompt-layers .signal-meta {
      color: #c1cbe0 !important;
    }
    html[data-theme="dark"] .persona-card .stage-explainer ul {
      color: #c9d6ee !important;
    }
    html[data-theme="dark"] .signals-summary .summary-card {
      background: #1a1f2b !important;
      border-color: #3a455b !important;
      color: #e6edf9 !important;
    }
    html[data-theme="dark"] .pipeline-stage-chip,
    html[data-theme="dark"] .pipeline-stage-detail-item {
      background: #1a1f2b !important;
      border-color: #3a455b !important;
      color: #e6edf9 !important;
    }
    html[data-theme="dark"] .pipeline-run-status.is-pending {
      background: #283145 !important;
      border-color: #3a455b !important;
      color: #c7d2ea !important;
    }
    html[data-theme="dark"] .signals-summary .summary-card .signal-meta {
      color: #c1cbe0 !important;
    }
    html[data-theme="dark"] .signal-flag {
      background: #1a1f2b !important;
      border-color: #3a455b !important;
      color: #e6edf9 !important;
    }
    html[data-theme="dark"] .beat-flyout {
      background: #1a1f2b !important;
      border-color: #3a455b !important;
    }
  `;
  document.head.appendChild(style);
}

function ensurePersonaUi() {
  if (document.getElementById('persona-settings-list')) return;
  const appSection = document.getElementById('settings-app');
  if (!appSection) return;
  const container = document.createElement('section');
  container.className = 'admin-section';
  container.innerHTML = `
    <article class="draft-card section-card">
      <button class="draft-header draft-toggle btn-reset" type="button">
        <strong class="section-title">Global Prompts</strong>
        <span class="draft-meta">Per-stage global guidance</span>
      </button>
      <div class="section-editor is-collapsed" hidden>
        <p class="hint">Global per-stage guidance applied before section and persona layers.</p>
        <div class="workflow-stage-list" id="global-prompt-layers-list"></div>
      </div>
    </article>
    <div class="section-header">
      <h2>Persona Management</h2>
      <div class="admin-actions">
        <button id="add-persona-btn" class="btn btn-primary" type="button">Add Persona</button>
        <button id="load-personas-btn" class="btn btn-secondary" type="button">Refresh Personas</button>
      </div>
    </div>
    <p class="hint">Curate avatar/disclosure, activation mode, discovery feeds, and full per-stage workflow settings for each topic engine.</p>
    <div id="add-persona-panel" class="draft-card" hidden>
      <div class="workflow-grid">
        <label>
          Display Name
          <input id="new-persona-display-name" type="text" placeholder="Example: Neighborhood Desk" maxlength="160">
        </label>
        <label>
          Section
          <select id="new-persona-section">
            ${Object.keys(BEAT_OPTIONS_BY_SECTION).map((section) => `<option value="${escapeHtml(section)}">${escapeHtml(section.replace(/\b\w/g, (m) => m.toUpperCase()))}</option>`).join('')}
          </select>
        </label>
        <div class="new-persona-beat-row">
          <label>
            Beat
            <select id="new-persona-beat"></select>
          </label>
          <button id="new-persona-add-beat-btn" class="btn btn-secondary" type="button">Add New Beat</button>
        </div>
      </div>
      <div id="new-beat-flyout" class="beat-flyout" hidden>
        <div class="beat-flyout-row">
          <label>
            New Beat Name
            <input id="new-beat-name-input" type="text" placeholder="Example: Transit Policy" maxlength="120">
          </label>
          <button id="save-new-beat-btn" class="btn btn-primary" type="button">Save Beat</button>
          <button id="cancel-new-beat-btn" class="btn btn-secondary" type="button">Cancel</button>
        </div>
        <p class="draft-meta">Saved as a slug and available for all personas in this section.</p>
      </div>
      <div class="admin-actions">
        <button id="create-persona-btn" class="btn btn-primary" type="button">Create Persona</button>
        <button id="cancel-add-persona-btn" class="btn btn-secondary" type="button">Cancel</button>
      </div>
      <p class="draft-meta">New personas get the same default stage stack and pacing controls as existing engines.</p>
    </div>
    <div id="persona-settings-list" class="draft-list"></div>
  `;
  appSection.appendChild(container);
}

function slugifyPersonaId(value) {
  return cleanText(value, 255)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function getDefaultStageConfigs() {
  const stageConfigs = {};
  for (const stageName of TOPIC_ENGINE_STAGES) {
    stageConfigs[stageName] = {
      runnerType: HARD_CODED_STAGE_STACK[stageName]?.runnerType || 'llm',
      provider: HARD_CODED_STAGE_STACK[stageName]?.provider || '',
      modelOrEndpoint: HARD_CODED_STAGE_STACK[stageName]?.modelOrEndpoint || '',
      enabled: true,
      promptTemplate: '',
      workflowConfig: {}
    };
  }
  return stageConfigs;
}

function syncNewPersonaBeatOptions() {
  const sectionSelect = document.getElementById('new-persona-section');
  const beatSelect = document.getElementById('new-persona-beat');
  if (!sectionSelect || !beatSelect) return;
  const section = cleanText(sectionSelect.value, 80).toLowerCase() || 'local';
  const beats = getBeatOptionsForSection(section);
  const current = String(beatSelect.value || '').trim();
  const optionsHtml = beats.map((beat) =>
    `<option value="${escapeHtml(beat.value)}">${escapeHtml(beat.label)}</option>`
  ).join('');
  beatSelect.innerHTML = `${optionsHtml}<option value="${ADD_NEW_BEAT_VALUE}">+ Add New Beat...</option>`;
  if (current && (beats.some((beat) => beat.value === current) || current === ADD_NEW_BEAT_VALUE)) {
    beatSelect.value = current;
  }
}

function toggleNewBeatFlyout(show) {
  const flyout = document.getElementById('new-beat-flyout');
  const input = document.getElementById('new-beat-name-input');
  if (!flyout) return;
  if (show) {
    flyout.removeAttribute('hidden');
    if (input) {
      input.focus();
      input.select();
    }
  } else {
    flyout.setAttribute('hidden', '');
    if (input) input.value = '';
  }
}

function addCustomBeatOption(section, beatSlug) {
  const key = cleanText(section, 80).toLowerCase() || 'local';
  const beat = cleanText(beatSlug, 120).toLowerCase();
  if (!beat) return false;
  const existing = getBeatOptionsForSection(key);
  if (existing.some((item) => item.value === beat)) return false;
  if (!customBeatsBySection[key]) customBeatsBySection[key] = [];
  customBeatsBySection[key].push({ value: beat, label: titleCaseSlug(beat) });
  return true;
}

function saveNewBeatFromFlyout() {
  const sectionSelect = document.getElementById('new-persona-section');
  const beatSelect = document.getElementById('new-persona-beat');
  const input = document.getElementById('new-beat-name-input');
  if (!sectionSelect || !beatSelect || !input) return;
  const section = cleanText(sectionSelect.value, 80).toLowerCase() || 'local';
  const beatSlug = slugifyPersonaId(input.value);
  if (!beatSlug) throw new Error('Enter a beat name');
  addCustomBeatOption(section, beatSlug);
  syncNewPersonaBeatOptions();
  beatSelect.value = beatSlug;
  toggleNewBeatFlyout(false);
}

function toggleAddPersonaPanel(show) {
  const panel = document.getElementById('add-persona-panel');
  const addBtn = document.getElementById('add-persona-btn');
  const sectionSelect = document.getElementById('new-persona-section');
  if (!panel || !addBtn) return;
  if (show) {
    panel.removeAttribute('hidden');
    addBtn.setAttribute('hidden', '');
    if (sectionSelect) sectionSelect.value = 'local';
    syncNewPersonaBeatOptions();
    toggleNewBeatFlyout(false);
  } else {
    panel.setAttribute('hidden', '');
    addBtn.removeAttribute('hidden');
    toggleNewBeatFlyout(false);
  }
}

async function createPersonaFromInputs() {
  const nameInput = document.getElementById('new-persona-display-name');
  const sectionSelect = document.getElementById('new-persona-section');
  const beatSelect = document.getElementById('new-persona-beat');
  const rawName = cleanText(nameInput?.value || '', 160);
  const normalizedId = slugifyPersonaId(rawName);
  if (!normalizedId) throw new Error('Display name is required');
  if (loadedPersonas.some((row) => cleanText(row?.id || '', 255) === normalizedId)) {
    throw new Error(`Persona '${normalizedId}' already exists`);
  }

  const section = cleanText(sectionSelect?.value || 'local', 80).toLowerCase() || 'local';
  let beat = cleanText(beatSelect?.value || '', 120);
  if (!beat || beat === ADD_NEW_BEAT_VALUE) {
    beat = getBeatOptionsForSection(section)[0]?.value || 'general-local';
  }
  beat = slugifyPersonaId(beat);
  if (!beat) throw new Error('Beat is required');
  addCustomBeatOption(section, beat);

  await apiRequest('/api/admin-personas', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: normalizedId,
      displayName: rawName,
      section,
      beat,
      avatarUrl: '',
      disclosure: '',
      activationMode: 'both',
      isAutoPromoteEnabled: false,
      imageConfig: {
        imageDbEnabled: true,
        imageSourcingEnabled: true,
        imageGenerationEnabled: false,
        imageMode: 'manual',
        imageProfile: 'professional',
        quotaPostgresImageDaily: 2,
        quotaSourcedImageDaily: 2,
        quotaGeneratedImageDaily: 2,
        quotaTextOnlyDaily: 3,
        layer6TimeoutSeconds: 90,
        layer6BudgetUsd: 0.20,
        exaMaxAttempts: 3,
        generationMaxAttempts: 2
      },
      pacingConfig: getPacingConfig(null),
      feeds: [],
      stageConfigs: getDefaultStageConfigs()
    })
  });

  if (nameInput) nameInput.value = '';
  if (sectionSelect) sectionSelect.value = 'local';
  syncNewPersonaBeatOptions();
  toggleAddPersonaPanel(false);
  await loadPersonas();
  renderPersonaFilterOptions();
  setMessage(`Persona '${normalizedId}' created.`);
}

function ensureSignalsUi() {
  if (document.getElementById('signals-queue-list')) return;
  const appRoot = document.getElementById('settings-app');
  if (!appRoot) return;

  const section = document.createElement('section');
  section.className = 'admin-section';
  section.innerHTML = `
    <div class="section-header">
      <h2>Signals Queue</h2>
      <button id="load-signals-btn" class="btn btn-secondary" type="button">Refresh Signals</button>
    </div>
    <p class="hint">Calibration queue for Gatekeeper outcomes. Review watched items and manually route promote/reject decisions.</p>
    <div class="signals-toolbar">
      <label>
        Persona
        <select id="signals-filter-persona">
          <option value="">All</option>
        </select>
      </label>
      <label>
        Action
        <select id="signals-filter-action">
          <option value="">All</option>
          <option value="pending">pending</option>
          <option value="watch">watch</option>
          <option value="promote">promote</option>
          <option value="reject">reject</option>
        </select>
      </label>
      <label>
        Review Decision
        <select id="signals-filter-review">
          <option value="">All</option>
          <option value="pending_review">pending_review</option>
          <option value="promoted">promoted</option>
          <option value="rejected">rejected</option>
        </select>
      </label>
      <label>
        Sort
        <select id="signals-sort">
          <option value="created_at:desc">Newest first</option>
          <option value="created_at:asc">Oldest first</option>
          <option value="is_newsworthy:desc">Highest newsworthy</option>
          <option value="is_newsworthy:asc">Lowest newsworthy</option>
          <option value="action:asc">Action A-Z</option>
          <option value="review_decision:asc">Review A-Z</option>
        </select>
      </label>
    </div>
    <div class="admin-actions">
      <button id="apply-signals-filters-btn" class="btn btn-primary" type="button">Apply Filters</button>
    </div>
    <div id="signals-summary" class="signals-summary"></div>
    <div id="signals-queue-list" class="signals-list"></div>
    <div id="signals-pagination" class="pagination-row"></div>
  `;
  appRoot.appendChild(section);
}

function renderSignalsPersonaOptions() {
  const select = document.getElementById('signals-filter-persona');
  if (!select) return;
  const current = String(select.value || '').trim();
  const personas = getAllDefinedPersonas();
  const options = ['<option value="">All</option>']
    .concat(
      personas.map((p) => {
        const override = cleanText(loadedPersonaDisplayNames.get(p.id) || '', 200);
        const label = override || p.label;
        return `<option value="${escapeHtml(p.id)}">${escapeHtml(label)} (${escapeHtml(p.id)})</option>`;
      })
    );
  select.innerHTML = options.join('');
  if (current && Array.from(select.options).some((opt) => opt.value === current)) {
    select.value = current;
  }
}

function renderPersonaFilterOptions() {
  renderSignalsPersonaOptions();
}

function renderGlobalPromptLayerList() {
  const el = document.getElementById('global-prompt-layers-list');
  if (!el) return;
  el.innerHTML = renderGlobalPromptEditors();
}

function renderPersonas(personas) {
  const personaListEl = document.getElementById('persona-settings-list');
  if (!personaListEl) return;
  loadedPersonas = Array.isArray(personas) ? personas : [];
  customBeatsBySection = buildCustomBeatsBySection(loadedPersonas);
  const definedPersonas = getAllDefinedPersonas();
  const personaDataMap = new Map(loadedPersonas.map((p) => [p.id, p]));
  loadedPersonaDisplayNames = new Map(
    loadedPersonas.map((p) => [String(p.id || '').trim(), cleanText(p.displayName || '', 200)])
  );

  const grouped = new Map();
  for (const persona of definedPersonas) {
    const section = persona.section || 'local';
    if (!grouped.has(section)) grouped.set(section, []);
    grouped.get(section).push(persona);
  }

  const sectionOrder = Object.keys(BEAT_OPTIONS_BY_SECTION);
  const sectionTitle = (value) => String(value || '')
    .replace(/-/g, ' ')
    .replace(/\b\w/g, (match) => match.toUpperCase());

  personaListEl.innerHTML = sectionOrder
    .filter((section) => grouped.has(section))
    .map((section) => {
      const items = grouped.get(section) || [];
      return `
        <article class="draft-card section-card">
          <button class="draft-header draft-toggle btn-reset" type="button">
            <strong class="section-title">${escapeHtml(sectionTitle(section))}</strong>
            <span class="draft-meta">${items.length} persona${items.length === 1 ? '' : 's'}</span>
          </button>
          <div class="section-editor is-collapsed" hidden>
            <section class="persona-advanced section-prompt-layers">
              <button type="button" class="persona-advanced-header btn-reset btn-toggle-section-prompts" aria-expanded="false">
                <strong>Section Prompt Layers</strong>
                <span class="draft-meta">Show</span>
              </button>
              <div class="persona-advanced-body section-prompt-layers-body" hidden>
                <div class="workflow-stage-list">
                  ${renderSectionPromptEditors(section)}
                </div>
              </div>
            </section>
            <div class="persona-nested-list">
              ${items.map((p) => {
                const data = personaDataMap.get(p.id) || {};
                const defaultDisplayName = cleanText(p.label, 200) || p.id;
                const savedDisplayName = cleanText(data.displayName || '', 200);
                const displayName = savedDisplayName || defaultDisplayName;
                const activationMode = String(data.activationMode || 'both');
                const isAutoPromoteEnabled = data.isAutoPromoteEnabled === true;
                const pacingConfig = getPacingConfig(data.pacingConfig);
                const imageConfig = {
                  imageDbEnabled: data.imageDbEnabled !== false,
                  imageSourcingEnabled: data.imageSourcingEnabled !== false,
                  imageGenerationEnabled: data.imageGenerationEnabled === true,
                  imageMode: cleanText(data.imageMode || 'manual', 20).toLowerCase() === 'auto' ? 'auto' : 'manual',
                  imageProfile: ['professional', 'creative', 'cheap'].includes(cleanText(data.imageProfile || '', 30).toLowerCase())
                    ? cleanText(data.imageProfile || '', 30).toLowerCase()
                    : 'professional',
                  imageFallbackAssetUrl: cleanText(data.imageFallbackAssetUrl || '', 5000),
                  imageFallbackCloudinaryPublicId: cleanText(data.imageFallbackCloudinaryPublicId || '', 500),
                  quotaPostgresImageDaily: parseNumberWithFallback(data.quotaPostgresImageDaily, 2),
                  quotaSourcedImageDaily: parseNumberWithFallback(data.quotaSourcedImageDaily, 2),
                  quotaGeneratedImageDaily: parseNumberWithFallback(data.quotaGeneratedImageDaily, 2),
                  quotaTextOnlyDaily: parseNumberWithFallback(data.quotaTextOnlyDaily, 3),
                  layer6TimeoutSeconds: parseNumberWithFallback(data.layer6TimeoutSeconds, 90),
                  layer6BudgetUsd: parseNumberWithFallback(data.layer6BudgetUsd, 0.20),
                  exaMaxAttempts: Number(data.exaMaxAttempts ?? 3) || 3,
                  generationMaxAttempts: Number(data.generationMaxAttempts ?? 2) || 2
	                };
	                const feedsText = formatFeedsForTextArea(data.feeds);
	                const researchTrustText = formatResearchTrustForTextArea(data.researchTrustConfig);
	                return `
                  <article class="draft-card persona-card" data-id="${p.id}" data-default-name="${escapeHtml(defaultDisplayName)}" data-display-name-committed="${escapeHtml(savedDisplayName)}" data-section="${escapeHtml(p.section || 'local')}" data-beat="${escapeHtml(p.beat || 'general-local')}">
                    <div class="persona-header-row">
                      <button class="draft-header draft-toggle btn-reset" type="button">
                        <strong class="persona-name-text">${escapeHtml(displayName)}</strong>
                        <span class="draft-meta">${escapeHtml(p.id)}</span>
                      </button>
                      <button type="button" class="btn btn-secondary btn-xs btn-rename-persona">Rename</button>
                    </div>
                    <div class="persona-rename-row" hidden>
                      <input type="text" class="field-display-name-inline" value="${escapeHtml(displayName)}" maxlength="160">
                      <div class="persona-rename-actions">
                        <button type="button" class="btn btn-primary btn-xs btn-save-display-name">Save</button>
                        <span class="draft-meta">${escapeHtml(p.id)}</span>
                      </div>
                    </div>
                    <div class="article-editor is-collapsed" hidden>
                      <p class="persona-summary">Section: ${escapeHtml(sectionTitle(p.section))} | Beat: ${escapeHtml(p.beat)}</p>
                      <div class="persona-editor-grid">
                        <div class="persona-avatar-editor">
                          <div class="persona-avatar-preview">
                            <img src="${escapeHtml(data.avatarUrl || '/images/personas/default-avatar.svg')}" alt="Avatar for ${escapeHtml(displayName)}">
                          </div>
                          <input type="text" class="field-avatar-url" value="${escapeHtml(data.avatarUrl || '')}" placeholder="Avatar URL">
                          <input type="file" class="file-image" accept="image/*" hidden>
                          <button type="button" class="btn btn-secondary btn-upload-avatar">Upload New Avatar</button>
                          <p class="upload-status"></p>
                        </div>
                        <div class="persona-disclosure-editor">
                          <label>Disclosure Text</label>
                          <textarea class="field-disclosure" rows="4" placeholder="This article was generated by a topic engine...">${escapeHtml(data.disclosure || '')}</textarea>
                          <label>Activation Mode</label>
                          <select class="field-activation-mode">
                            <option value="both" ${activationMode === 'both' ? 'selected' : ''}>Both (Event + Scheduled)</option>
                            <option value="event" ${activationMode === 'event' ? 'selected' : ''}>Event-Driven (Push)</option>
                            <option value="scheduled" ${activationMode === 'scheduled' ? 'selected' : ''}>Scheduled (Pull)</option>
                          </select>
                          <label class="persona-autonomy-toggle">
                            <input type="checkbox" class="field-is-auto-promote-enabled" ${isAutoPromoteEnabled ? 'checked' : ''}>
                            Auto-promote enabled
                          </label>
                        </div>
                      </div>

                      <div class="workflow-grid">
                        <label>
                          Image DB Enabled
                          <input type="checkbox" class="field-image-db-enabled" ${imageConfig.imageDbEnabled ? 'checked' : ''}>
                        </label>
                        <label>
                          Image Sourcing Enabled
                          <input type="checkbox" class="field-image-sourcing-enabled" ${imageConfig.imageSourcingEnabled ? 'checked' : ''}>
                        </label>
                        <label>
                          Image Generation Enabled
                          <input type="checkbox" class="field-image-generation-enabled" ${imageConfig.imageGenerationEnabled ? 'checked' : ''}>
                        </label>
                        <label>
                          Image Mode
                          <select class="field-image-mode">
                            <option value="manual" ${imageConfig.imageMode === 'manual' ? 'selected' : ''}>manual</option>
                            <option value="auto" ${imageConfig.imageMode === 'auto' ? 'selected' : ''}>auto</option>
                          </select>
                        </label>
                        <label>
                          Image Profile
                          <select class="field-image-profile">
                            <option value="professional" ${imageConfig.imageProfile === 'professional' ? 'selected' : ''}>professional</option>
                            <option value="creative" ${imageConfig.imageProfile === 'creative' ? 'selected' : ''}>creative</option>
                            <option value="cheap" ${imageConfig.imageProfile === 'cheap' ? 'selected' : ''}>cheap</option>
                          </select>
                        </label>
                        <label>
                          Exa Max Attempts
                          <input type="number" min="1" max="20" step="1" class="field-exa-max-attempts" value="${escapeHtml(String(imageConfig.exaMaxAttempts))}">
                        </label>
                        <label>
                          Generation Max Attempts
                          <input type="number" min="1" max="20" step="1" class="field-generation-max-attempts" value="${escapeHtml(String(imageConfig.generationMaxAttempts))}">
                        </label>
                        <label>
                          Layer 6 Timeout (seconds)
                          <input type="number" min="15" max="600" step="1" class="field-layer6-timeout-seconds" value="${escapeHtml(String(imageConfig.layer6TimeoutSeconds))}">
                        </label>
                        <label>
                          Layer 6 Budget (USD)
                          <input type="number" min="0" max="50" step="0.01" class="field-layer6-budget-usd" value="${escapeHtml(String(imageConfig.layer6BudgetUsd))}">
                        </label>
                        <label>
                          Daily Postgres Image Quota
                          <input type="number" min="0" max="5000" step="1" class="field-quota-postgres-image-daily" value="${escapeHtml(String(imageConfig.quotaPostgresImageDaily))}">
                        </label>
                        <label>
                          Daily Sourced Image Quota
                          <input type="number" min="0" max="5000" step="1" class="field-quota-sourced-image-daily" value="${escapeHtml(String(imageConfig.quotaSourcedImageDaily))}">
                        </label>
                        <label>
                          Daily Generated Image Quota
                          <input type="number" min="0" max="5000" step="1" class="field-quota-generated-image-daily" value="${escapeHtml(String(imageConfig.quotaGeneratedImageDaily))}">
                        </label>
                        <label>
                          Daily Text-Only Quota
                          <input type="number" min="0" max="5000" step="1" class="field-quota-text-only-daily" value="${escapeHtml(String(imageConfig.quotaTextOnlyDaily))}">
                        </label>
                        <label class="workflow-wide">
                          Persona Fallback Image URL
                          <input type="text" class="field-image-fallback-asset-url" value="${escapeHtml(imageConfig.imageFallbackAssetUrl)}" placeholder="https://...">
                          <input type="hidden" class="field-image-fallback-cloudinary-public-id" value="${escapeHtml(imageConfig.imageFallbackCloudinaryPublicId)}">
                          <div class="persona-fallback-upload-row">
                            <input type="file" class="file-fallback-image" accept="image/*" hidden>
                            <button type="button" class="btn btn-secondary btn-upload-fallback-image">Upload or Drop Image</button>
                            <span class="draft-meta">Tip: drag an image file onto the URL field.</span>
                          </div>
                          <p class="fallback-upload-status"></p>
                        </label>
                        <label>
                          Posts / Active Day
                          <input type="number" min="0" max="24" step="1" class="field-pacing-posts-per-day" value="${escapeHtml(String(pacingConfig.postsPerActiveDay))}">
                        </label>
                        <label class="workflow-wide">
                          Posting Days (Mon,Tue,Wed,Thu,Fri,Sat,Sun)
                          <input type="text" class="field-pacing-posting-days" value="${escapeHtml(formatPostingDays(pacingConfig.postingDays))}">
                        </label>
                        <label>
                          Window Start (e.g. 7:30 AM)
                          <div class="time-input-row">
                            <input type="text" class="field-pacing-window-start-time" value="${escapeHtml(getTimeParts12(pacingConfig.windowStartLocal).time)}" placeholder="7:30">
                            <select class="field-pacing-window-start-meridiem">
                              <option value="AM" ${getTimeParts12(pacingConfig.windowStartLocal).meridiem === 'AM' ? 'selected' : ''}>AM</option>
                              <option value="PM" ${getTimeParts12(pacingConfig.windowStartLocal).meridiem === 'PM' ? 'selected' : ''}>PM</option>
                            </select>
                          </div>
                        </label>
                        <label>
                          Window End (e.g. 8:00 PM)
                          <div class="time-input-row">
                            <input type="text" class="field-pacing-window-end-time" value="${escapeHtml(getTimeParts12(pacingConfig.windowEndLocal).time)}" placeholder="8:00">
                            <select class="field-pacing-window-end-meridiem">
                              <option value="AM" ${getTimeParts12(pacingConfig.windowEndLocal).meridiem === 'AM' ? 'selected' : ''}>AM</option>
                              <option value="PM" ${getTimeParts12(pacingConfig.windowEndLocal).meridiem === 'PM' ? 'selected' : ''}>PM</option>
                            </select>
                          </div>
                        </label>
                        <label>
                          Single Post Time (e.g. 8:30 AM)
                          <div class="time-input-row">
                            <input type="text" class="field-pacing-single-time" value="${escapeHtml(getTimeParts12(pacingConfig.singlePostTimeLocal || '08:30:00').time)}" placeholder="8:30">
                            <select class="field-pacing-single-time-meridiem">
                              <option value="AM" ${getTimeParts12(pacingConfig.singlePostTimeLocal || '08:30:00').meridiem === 'AM' ? 'selected' : ''}>AM</option>
                              <option value="PM" ${getTimeParts12(pacingConfig.singlePostTimeLocal || '08:30:00').meridiem === 'PM' ? 'selected' : ''}>PM</option>
                            </select>
                          </div>
                        </label>
                        <label>
                          Cadence (off = even spacing)
                          <input type="checkbox" class="field-pacing-cadence-enabled" ${pacingConfig.cadenceEnabled ? 'checked' : ''}>
                        </label>
	                        <label class="workflow-wide">
	                          Discovery Feeds (one per line; optional format: URL | Source Name | Priority)
	                          <textarea class="field-feeds" rows="4" placeholder="https://example.com/rss | City Hall Agenda | 20">${escapeHtml(feedsText)}</textarea>
	                        </label>
	                        <label class="workflow-wide">
	                          Research Trust Domains (one per line: domain | trust tier | official/non-official | priority)
	                          <textarea class="field-research-trust" rows="4" placeholder="daytondailynews.com | local_news | non-official | 50">${escapeHtml(researchTrustText)}</textarea>
	                        </label>
	                      </div>

                      <section class="persona-pipeline-runs">
                        <button type="button" class="persona-pipeline-runs-header btn-reset btn-toggle-persona-pipeline-runs" aria-expanded="false">
                          <strong>Persona Pipeline Runs</strong>
                          <span class="draft-meta">Show</span>
                        </button>
                        <div class="persona-pipeline-runs-body" hidden>
                          <p class="signal-meta">Loading promoted runs...</p>
                        </div>
                      </section>

                      <section class="persona-advanced">
                        <button type="button" class="persona-advanced-header btn-reset btn-toggle-advanced" aria-expanded="false">
                          <strong>Persona Prompt Layers</strong>
                          <span class="draft-meta">Show</span>
                        </button>
                        <div class="persona-advanced-body" hidden>
                          <div class="workflow-stage-list">
                            ${renderStageEditors(data.stageConfigs)}
                          </div>
                        </div>
                      </section>
                      <section class="persona-advanced persona-final-prompts">
                        <button type="button" class="persona-advanced-header btn-reset btn-toggle-persona-final-prompts" aria-expanded="false">
                          <strong>All Final Prompt Drafts</strong>
                          <span class="draft-meta">Show</span>
                        </button>
                        <div class="persona-advanced-body persona-final-prompts-body" hidden>
                          <div class="admin-actions">
                            <button type="button" class="btn btn-secondary btn-load-persona-final-prompts">Refresh All Drafts</button>
                          </div>
                          <div class="signal-meta persona-final-prompts-message">Load to view compiled final prompts for every stage.</div>
                          <div class="workflow-stage-list persona-final-prompts-list"></div>
                        </div>
                      </section>
                      <div class="admin-actions">
                        <button type="button" class="btn btn-primary btn-save-persona">Save Persona</button>
                      </div>
                    </div>
                  </article>
                `;
              }).join('')}
            </div>
          </div>
        </article>
      `;
    }).join('');
}

function getPacingConfig(value) {
  const raw = value && typeof value === 'object' ? value : {};
  return {
    enabled: raw.enabled === true,
    postingDays: Array.isArray(raw.postingDays) && raw.postingDays.length === 7
      ? raw.postingDays.map(Boolean)
      : [true, true, true, true, true, true, true],
    postsPerActiveDay: Number.isFinite(Number.parseInt(String(raw.postsPerActiveDay ?? 1), 10))
      ? Number.parseInt(String(raw.postsPerActiveDay ?? 1), 10)
      : 1,
    windowStartLocal: String(raw.windowStartLocal || '06:00:00'),
    windowEndLocal: String(raw.windowEndLocal || '22:00:00'),
    cadenceEnabled: raw.cadenceEnabled !== false,
    singlePostTimeLocal: raw.singlePostTimeLocal ? String(raw.singlePostTimeLocal) : ''
  };
}

function parseIntegerWithFallback(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseNumberWithFallback(value, fallback) {
  const parsed = Number(String(value ?? ''));
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatPostingDays(days) {
  const labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
  const safe = Array.isArray(days) && days.length === 7 ? days : [true, true, true, true, true, true, true];
  return labels.filter((_, idx) => safe[idx]).join(',');
}

function parsePostingDays(value) {
  const raw = String(value || '').trim();
  if (!raw) return [true, true, true, true, true, true, true];
  const map = new Set(raw.split(',').map((v) => v.trim().slice(0, 3).toLowerCase()));
  return ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'].map((key) => map.has(key));
}

function getTimeParts12(value) {
  const raw = String(value || '').trim();
  if (!raw) return { time: '', meridiem: 'AM' };
  const m = raw.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (!m) return { time: raw, meridiem: 'AM' };
  let hh = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return { time: raw, meridiem: 'AM' };
  const meridiem = hh >= 12 ? 'PM' : 'AM';
  hh = hh % 12;
  if (hh === 0) hh = 12;
  return { time: `${hh}:${String(mm).padStart(2, '0')}`, meridiem };
}

function formatTimeWithMeridiem(timeValue, meridiemValue) {
  const rawTime = String(timeValue || '').trim();
  const rawMeridiem = String(meridiemValue || '').trim().toUpperCase();
  if (!rawTime) return '';
  if (rawMeridiem !== 'AM' && rawMeridiem !== 'PM') return rawTime;
  return `${rawTime} ${rawMeridiem}`;
}

function normalizeTimeInput(value, fallback) {
  const raw = String(value || '').trim();
  if (!raw) return fallback;
  const twelveHour = raw.match(/^(\d{1,2}):(\d{2})\s*([AaPp][Mm])$/);
  if (twelveHour) {
    let hh = Number(twelveHour[1]);
    const mm = Number(twelveHour[2]);
    const ap = String(twelveHour[3] || '').toUpperCase();
    if (!Number.isFinite(hh) || !Number.isFinite(mm) || hh < 1 || hh > 12 || mm < 0 || mm > 59) return null;
    if (ap === 'AM') hh = hh === 12 ? 0 : hh;
    if (ap === 'PM') hh = hh === 12 ? 12 : hh + 12;
    return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}:00`;
  }
  const twentyFourHour = raw.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!twentyFourHour) return null;
  const hh = Number(twentyFourHour[1]);
  const mm = Number(twentyFourHour[2]);
  if (!Number.isFinite(hh) || !Number.isFinite(mm) || hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
  return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}:00`;
}

function normalizeOptionalTimeInput(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  return normalizeTimeInput(raw, null);
}

function updatePacingControlState(card) {
  if (!card) return;
  const posts = parseIntegerWithFallback(card.querySelector('.field-pacing-posts-per-day')?.value, 1);
  const isPausedMode = posts <= 0;
  const isSingleMode = posts === 1;
  const windowStart = card.querySelector('.field-pacing-window-start-time');
  const windowStartMeridiem = card.querySelector('.field-pacing-window-start-meridiem');
  const windowEnd = card.querySelector('.field-pacing-window-end-time');
  const windowEndMeridiem = card.querySelector('.field-pacing-window-end-meridiem');
  const singleTime = card.querySelector('.field-pacing-single-time');
  const singleTimeMeridiem = card.querySelector('.field-pacing-single-time-meridiem');
  const cadenceToggle = card.querySelector('.field-pacing-cadence-enabled');
  if (windowStart) windowStart.disabled = isSingleMode || isPausedMode;
  if (windowStartMeridiem) windowStartMeridiem.disabled = isSingleMode || isPausedMode;
  if (windowEnd) windowEnd.disabled = isSingleMode || isPausedMode;
  if (windowEndMeridiem) windowEndMeridiem.disabled = isSingleMode || isPausedMode;
  if (singleTime) singleTime.disabled = !isSingleMode;
  if (singleTimeMeridiem) singleTimeMeridiem.disabled = !isSingleMode;
  if (cadenceToggle) cadenceToggle.disabled = isSingleMode || isPausedMode;
}

function getDefaultStageConfig() {
  return {
    runnerType: 'llm',
    provider: 'google',
    modelOrEndpoint: '',
    enabled: true,
    promptTemplate: '',
    workflowConfig: {}
  };
}

function isDraftWritingStage(stageName) {
  return String(stageName || '').trim().toLowerCase() === 'draft_writing';
}

function getStageConfig(stageConfigs, stageName) {
  const fromApi = stageConfigs && typeof stageConfigs === 'object' ? stageConfigs[stageName] : null;
  const hardcoded = HARD_CODED_STAGE_STACK[stageName] || { runnerType: 'llm', provider: 'google', modelOrEndpoint: '' };
  const config = getDefaultStageConfig();
  config.runnerType = String(hardcoded.runnerType || config.runnerType);
  config.provider = isDraftWritingStage(stageName) && fromApi && typeof fromApi === 'object'
    ? String(fromApi.provider || hardcoded.provider || config.provider)
    : String(hardcoded.provider || config.provider);
  config.modelOrEndpoint = isDraftWritingStage(stageName) && fromApi && typeof fromApi === 'object'
    ? String(fromApi.modelOrEndpoint || hardcoded.modelOrEndpoint || '')
    : String(hardcoded.modelOrEndpoint || '');
  config.enabled = !fromApi || typeof fromApi !== 'object' ? true : fromApi.enabled !== false;
  config.promptTemplate = !fromApi || typeof fromApi !== 'object' ? '' : String(fromApi.promptTemplate || '');
  config.workflowConfig = !fromApi || typeof fromApi !== 'object'
    ? {}
    : (fromApi.workflowConfig && typeof fromApi.workflowConfig === 'object' ? fromApi.workflowConfig : {});
  return config;
}

function renderDraftWritingModelControls(config) {
  const modelByProvider = getDraftWritingModelByProvider();
  const provider = cleanText(config?.provider || HARD_CODED_STAGE_STACK.draft_writing?.provider || '', 80).toLowerCase() || 'openai';
  const modelOrEndpoint = modelByProvider[provider] || modelByProvider.openai;
  // Keep the provider selector colocated with the Draft Writing persona prompt layer.
  return `
    <label>
      Writer Provider
      <select class="field-stage-provider" data-default-model="${escapeHtml(modelOrEndpoint)}">
        ${DRAFT_WRITING_PROVIDER_OPTIONS.map((option) => `
          <option value="${escapeHtml(option)}" ${provider === option ? 'selected' : ''}>${escapeHtml(DRAFT_WRITING_PROVIDER_LABELS[option] || titleCaseSlug(option))}</option>
        `).join('')}
      </select>
    </label>
    <label>
      Writer Model
      <input
        type="text"
        class="field-stage-model"
        maxlength="160"
        value="${escapeHtml(modelOrEndpoint)}"
        readonly
      >
    </label>
  `;
}

function syncDraftWritingModelSelection(stageEl) {
  const modelByProvider = getDraftWritingModelByProvider();
  const providerSelect = stageEl?.querySelector('.field-stage-provider');
  const modelInput = stageEl?.querySelector('.field-stage-model');
  if (!providerSelect || !modelInput) return;
  const provider = cleanText(providerSelect.value || '', 80).toLowerCase();
  modelInput.value = modelByProvider[provider] || modelByProvider.openai;
}

function formatFeedsForTextArea(feeds) {
  if (!Array.isArray(feeds) || feeds.length === 0) return '';
  return feeds.map((feed) => {
    const url = String(feed?.feedUrl || '').trim();
    if (!url) return '';
    const sourceName = String(feed?.sourceName || '').trim();
    const priority = Number.parseInt(String(feed?.priority || ''), 10);
    const chunks = [url];
    if (sourceName) chunks.push(sourceName);
    if (Number.isFinite(priority) && priority > 0) chunks.push(String(priority));
    return chunks.join(' | ');
  }).filter(Boolean).join('\n');
}

function formatResearchTrustForTextArea(entries) {
  if (!Array.isArray(entries) || entries.length === 0) return '';
  return entries.map((entry) => {
    const domain = cleanText(entry?.domain || '', 255);
    if (!domain) return '';
    const trustTier = cleanText(entry?.trustTier || 'trusted', 40).toLowerCase() || 'trusted';
    const official = entry?.isOfficial === true ? 'official' : 'non-official';
    const priority = Number.parseInt(String(entry?.priority || ''), 10);
    return [
      domain,
      trustTier,
      official,
      Number.isFinite(priority) && priority > 0 ? String(priority) : '100'
    ].join(' | ');
  }).filter(Boolean).join('\n');
}

function renderStageEditors(stageConfigs) {
  return TOPIC_ENGINE_STAGES.map((stageName) => {
    const config = getStageConfig(stageConfigs, stageName);
    const explainer = STAGE_EXPLANATIONS[stageName] || { summary: '', details: [] };
    const workflowConfigText = JSON.stringify(config.workflowConfig || {}, null, 2);
    const summaryText = [config.runnerType.toUpperCase(), config.provider, config.modelOrEndpoint].filter(Boolean).join(' • ');
    return `
      <section class="workflow-stage" data-stage="${stageName}">
        <button type="button" class="workflow-stage-header btn-reset btn-toggle-stage" aria-expanded="false">
          <div>
            <h4>${escapeHtml(STAGE_LABELS[stageName] || stageName)}</h4>
            <div class="workflow-stage-summary">${escapeHtml(summaryText)}</div>
          </div>
          <span class="draft-meta">Edit</span>
        </button>
        <div class="workflow-stage-body" hidden>
          <div class="workflow-stage-top">
            <label class="workflow-stage-enabled">
              <input type="checkbox" class="field-stage-enabled" ${config.enabled ? 'checked' : ''}>
              Enabled
            </label>
          </div>
          <div class="workflow-stage-grid">
            <div class="workflow-wide stage-explainer">
              <p><strong>${escapeHtml(explainer.summary || '')}</strong></p>
              <ul>
                ${(Array.isArray(explainer.details) ? explainer.details : []).map((item) => `<li>${escapeHtml(item)}</li>`).join('')}
              </ul>
            </div>
            <p class="signal-meta workflow-wide">
              ${isDraftWritingStage(stageName)
                ? 'Only Phase 5 writer selection is persona-curatable. All other stages remain hardcoded in production.'
                : 'This stage remains hardcoded in production. Persona-level model selection is disabled.'}
            </p>
            ${isDraftWritingStage(stageName) ? renderDraftWritingModelControls(config) : `
              <label>
                Runner Type
                <input type="text" value="${escapeHtml(String(config.runnerType || '').toUpperCase())}" readonly>
              </label>
              <label>
                Runtime Stack
                <input type="text" value="${escapeHtml([config.provider, config.modelOrEndpoint].filter(Boolean).join(' • '))}" readonly>
              </label>
            `}
            <label class="workflow-wide">
              Prompt Template
              <textarea class="field-stage-prompt" rows="3" placeholder="Instructions for this stage...">${escapeHtml(config.promptTemplate)}</textarea>
            </label>
            <label class="workflow-wide">
              Workflow Config (JSON)
              <textarea class="field-stage-workflow-config" rows="4" placeholder='{"temperature":0.2}'>${escapeHtml(workflowConfigText)}</textarea>
            </label>
            <section class="workflow-wide prompt-preview-panel" data-preview-stage="${stageName}">
              <button type="button" class="workflow-stage-header btn-reset btn-toggle-final-prompt" aria-expanded="false">
                <div>
                  <h4>Final Prompt Draft</h4>
                  <div class="workflow-stage-summary">Compiled global + section + persona prompt for this stage</div>
                </div>
                <span class="draft-meta">Show</span>
              </button>
              <div class="final-prompt-body" hidden>
                <div class="admin-actions">
                  <button type="button" class="btn btn-secondary btn-load-final-prompt">Refresh Draft</button>
                </div>
                <div class="signal-meta final-prompt-message">Prompt preview not loaded yet.</div>
                <div class="final-prompt-breakdown"></div>
                <label class="workflow-wide">
                  Compiled Prompt
                  <textarea class="field-final-prompt" rows="10" readonly></textarea>
                </label>
              </div>
            </section>
          </div>
        </div>
      </section>
    `;
  }).join('');
}

function renderPromptLayerEditor(scopeType, stageName, section) {
  const layer = getPromptLayer(scopeType, stageName, section);
  const label = STAGE_LABELS[stageName] || stageName;
  const scopeLabel = scopeType === PROMPT_SCOPE_GLOBAL ? 'Global' : `${titleCaseSlug(section)} Section`;
  const rows = scopeType === PROMPT_SCOPE_GLOBAL ? 8 : 4;
  const explainer = STAGE_EXPLANATIONS[stageName] || { summary: '', details: [] };
  const hardcoded = HARD_CODED_STAGE_STACK[stageName] || {};
  const summaryText = [
    String(hardcoded.runnerType || '').toUpperCase(),
    cleanText(hardcoded.provider || '', 120),
    cleanText(hardcoded.modelOrEndpoint || '', 300)
  ].filter(Boolean).join(' • ');
  return `
    <section class="workflow-stage prompt-layer-editor" data-scope-type="${escapeHtml(scopeType)}" data-stage="${escapeHtml(stageName)}" data-section="${escapeHtml(section || '')}">
      <button type="button" class="workflow-stage-header btn-reset btn-toggle-layer-editor" aria-expanded="false">
        <div>
          <h4>${escapeHtml(label)}</h4>
          <div class="workflow-stage-summary">${escapeHtml(summaryText || `${scopeLabel} prompt layer`)}</div>
        </div>
        <span class="draft-meta">Edit</span>
      </button>
      <div class="workflow-stage-body" hidden>
        <div class="workflow-stage-grid">
          <div class="workflow-wide stage-explainer">
            <p><strong>${escapeHtml(explainer.summary || '')}</strong></p>
            <ul>
              ${(Array.isArray(explainer.details) ? explainer.details : []).map((item) => `<li>${escapeHtml(item)}</li>`).join('')}
            </ul>
          </div>
          <p class="signal-meta workflow-wide">Model stack is globally hardcoded for v1. Stage-level model selection is disabled.</p>
        </div>
        <label class="workflow-wide">
          Prompt Guidance
          <textarea
            class="field-layer-prompt"
            rows="${rows}"
            readonly
            data-original="${escapeHtml(layer.promptTemplate || '')}"
          >${escapeHtml(layer.promptTemplate || '')}</textarea>
        </label>
        <p class="signal-meta">Version: <span class="field-layer-version">${layer.version == null ? 'new' : escapeHtml(String(layer.version))}</span></p>
        <div class="admin-actions">
          <button type="button" class="btn btn-secondary btn-edit-layer">Edit</button>
          <button type="button" class="btn btn-secondary btn-cancel-layer" hidden>Cancel</button>
          <button type="button" class="btn btn-primary btn-save-layer" hidden>Save</button>
        </div>
      </div>
    </section>
  `;
}

function renderGlobalPromptEditors() {
  return TOPIC_ENGINE_STAGES
    .map((stageName) => renderPromptLayerEditor(PROMPT_SCOPE_GLOBAL, stageName, null))
    .join('');
}

function renderSectionPromptEditors(section) {
  return TOPIC_ENGINE_STAGES
    .map((stageName) => renderPromptLayerEditor(PROMPT_SCOPE_SECTION, stageName, section))
    .join('');
}

function parseFeedsFromText(value) {
  const lines = String(value || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  return lines.map((line) => {
    const [feedUrlRaw, sourceNameRaw = '', priorityRaw = ''] = line.split('|').map((part) => part.trim());
    if (!feedUrlRaw) return null;
    const parsedPriority = Number.parseInt(priorityRaw, 10);
    return {
      feedUrl: feedUrlRaw,
      sourceName: sourceNameRaw,
      priority: Number.isFinite(parsedPriority) && parsedPriority > 0 ? parsedPriority : 100
    };
  }).filter(Boolean);
}

function parseResearchTrustFromText(value, section, beat) {
  const lines = String(value || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  return lines.map((line) => {
    const [domainRaw = '', trustTierRaw = 'trusted', officialRaw = '', priorityRaw = '100'] = line.split('|').map((part) => part.trim());
    const domain = cleanText(domainRaw, 255).toLowerCase().replace(/^https?:\/\//, '').replace(/\/.*$/, '').replace(/^\.+|\.+$/g, '');
    if (!domain) return null;
    const trustTierNormalized = cleanText(trustTierRaw, 40).toLowerCase();
    const trustTier = ['official', 'local_news', 'trusted', 'contextual'].includes(trustTierNormalized)
      ? trustTierNormalized
      : 'trusted';
    const officialToken = cleanText(officialRaw, 40).toLowerCase();
    const isOfficial = officialToken === 'official' || officialToken === 'true' || trustTier === 'official';
    return {
      section,
      beat,
      domain,
      trustTier,
      isOfficial,
      priority: Math.min(Math.max(parseIntegerWithFallback(priorityRaw, 100), 1), 10000),
      enabled: true,
      notes: null
    };
  }).filter(Boolean);
}

function setStageExpanded(stageEl, expanded) {
  const headerBtn = stageEl?.querySelector('.btn-toggle-stage, .btn-toggle-layer-editor, .btn-toggle-persona-final-stage');
  const body = stageEl?.querySelector('.workflow-stage-body');
  if (!headerBtn || !body) return;
  headerBtn.setAttribute('aria-expanded', expanded ? 'true' : 'false');
  if (expanded) body.removeAttribute('hidden');
  else body.setAttribute('hidden', '');
}

function updateStageSummary(stageEl) {
  if (!stageEl) return;
  const summaryEl = stageEl.querySelector('.workflow-stage-summary');
  if (!summaryEl) return;
  const stageName = String(stageEl.getAttribute('data-stage') || '').trim();
  const hardcoded = HARD_CODED_STAGE_STACK[stageName] || {};
  const parts = [
    String(hardcoded.runnerType || '').toUpperCase(),
    cleanText(hardcoded.provider || '', 120),
    cleanText(hardcoded.modelOrEndpoint || '', 300)
  ].filter(Boolean);
  summaryEl.textContent = parts.length ? parts.join(' • ') : 'No runner configured yet';
}

function setAllStagesExpanded(card, expanded) {
  const stageEls = card.querySelectorAll('.workflow-stage[data-stage]');
  for (const stageEl of stageEls) {
    setStageExpanded(stageEl, expanded);
  }
}

function setAdvancedExpanded(card, expanded) {
  const btn = card?.querySelector('.btn-toggle-advanced');
  const body = card?.querySelector('.persona-advanced-body');
  if (!btn || !body) return;
  btn.setAttribute('aria-expanded', expanded ? 'true' : 'false');
  const meta = btn.querySelector('.draft-meta');
  if (meta) meta.textContent = expanded ? 'Hide' : 'Show';
  if (expanded) body.removeAttribute('hidden');
  else body.setAttribute('hidden', '');
}

function setPersonaPipelineRunsExpanded(card, expanded) {
  const btn = card?.querySelector('.btn-toggle-persona-pipeline-runs');
  const body = card?.querySelector('.persona-pipeline-runs-body');
  if (!btn || !body) return;
  btn.setAttribute('aria-expanded', expanded ? 'true' : 'false');
  const meta = btn.querySelector('.draft-meta');
  if (meta) meta.textContent = expanded ? 'Hide' : 'Show';
  if (expanded) body.removeAttribute('hidden');
  else body.setAttribute('hidden', '');
}

function setSectionPromptsExpanded(sectionCard, expanded) {
  const btn = sectionCard?.querySelector('.btn-toggle-section-prompts');
  const body = sectionCard?.querySelector('.section-prompt-layers-body');
  if (!btn || !body) return;
  btn.setAttribute('aria-expanded', expanded ? 'true' : 'false');
  const meta = btn.querySelector('.draft-meta');
  if (meta) meta.textContent = expanded ? 'Hide' : 'Show';
  if (expanded) body.removeAttribute('hidden');
  else body.setAttribute('hidden', '');
}

function setPersonaFinalPromptsExpanded(card, expanded) {
  const btn = card?.querySelector('.btn-toggle-persona-final-prompts');
  const body = card?.querySelector('.persona-final-prompts-body');
  if (!btn || !body) return;
  btn.setAttribute('aria-expanded', expanded ? 'true' : 'false');
  const meta = btn.querySelector('.draft-meta');
  if (meta) meta.textContent = expanded ? 'Hide' : 'Show';
  if (expanded) body.removeAttribute('hidden');
  else body.setAttribute('hidden', '');
}

function setFinalPromptExpanded(stageEl, expanded) {
  const btn = stageEl?.querySelector('.btn-toggle-final-prompt');
  const body = stageEl?.querySelector('.final-prompt-body');
  if (!btn || !body) return;
  btn.setAttribute('aria-expanded', expanded ? 'true' : 'false');
  const meta = btn.querySelector('.draft-meta');
  if (meta) meta.textContent = expanded ? 'Hide' : 'Show';
  if (expanded) body.removeAttribute('hidden');
  else body.setAttribute('hidden', '');
}

async function loadFinalPromptPreview(stageEl) {
  if (!stageEl) return;
  const card = stageEl.closest('.persona-card');
  const stageName = cleanText(stageEl.dataset.stage || '', 120).toLowerCase();
  const personaId = cleanText(card?.dataset?.id || '', 255);
  const messageEl = stageEl.querySelector('.final-prompt-message');
  const output = stageEl.querySelector('.field-final-prompt');
  const breakdownEl = stageEl.querySelector('.final-prompt-breakdown');
  if (!personaId || !stageName || !output || !messageEl || !breakdownEl) return;

  messageEl.textContent = 'Loading prompt preview...';
  const params = new URLSearchParams({ personaId, stageName });
  const data = await apiRequest(`/api/admin-prompt-preview?${params.toString()}`);
  output.value = String(data?.compiledPrompt || '');
  const warnings = Array.isArray(data?.warnings) && data.warnings.length
    ? ` | warnings: ${data.warnings.join(', ')}`
    : '';
  messageEl.textContent = `sourceVersion=${cleanText(data?.promptSourceVersion || '', 120) || 'n/a'}${warnings}`;

  const layers = Array.isArray(data?.layerBreakdown) ? data.layerBreakdown : [];
  breakdownEl.innerHTML = layers.length
    ? layers.map((item) => `
      <details>
        <summary>${escapeHtml(cleanText(item?.layer || 'layer', 40))}</summary>
        <pre>${escapeHtml(cleanText(item?.text || '', 120000))}</pre>
      </details>
    `).join('')
    : '<p class="signal-meta">No layers returned.</p>';
}

async function loadPersonaFinalPromptBundle(card) {
  if (!card) return;
  const personaId = cleanText(card.dataset.id || '', 255);
  const messageEl = card.querySelector('.persona-final-prompts-message');
  const listEl = card.querySelector('.persona-final-prompts-list');
  if (!personaId || !messageEl || !listEl) return;

  messageEl.textContent = 'Loading all stage prompt drafts...';
  const rows = [];
  for (const stageName of TOPIC_ENGINE_STAGES) {
    try {
      const params = new URLSearchParams({ personaId, stageName });
      const data = await apiRequest(`/api/admin-prompt-preview?${params.toString()}`);
      rows.push({ ok: true, stageName, data });
    } catch (error) {
      rows.push({ ok: false, stageName, error });
    }
  }

  listEl.innerHTML = rows.map((row) => {
    if (!row.ok) {
      return `
        <section class="workflow-stage">
          <h4>${escapeHtml(STAGE_LABELS[row.stageName] || row.stageName)}</h4>
          <p class="signal-meta">Preview failed: ${escapeHtml(row.error?.message || 'Unknown error')}</p>
        </section>
      `;
    }
    const data = row.data || {};
    const warnings = Array.isArray(data.warnings) && data.warnings.length
      ? `<p class="signal-meta">warnings: ${escapeHtml(data.warnings.join(', '))}</p>`
      : '';
    const hardcoded = HARD_CODED_STAGE_STACK[row.stageName] || {};
    const summaryText = [
      String(hardcoded.runnerType || '').toUpperCase(),
      cleanText(hardcoded.provider || '', 120),
      cleanText(hardcoded.modelOrEndpoint || '', 300)
    ].filter(Boolean).join(' • ');
    const explainer = STAGE_EXPLANATIONS[row.stageName] || { summary: '', details: [] };
    return `
      <section class="workflow-stage">
        <button type="button" class="workflow-stage-header btn-reset btn-toggle-persona-final-stage" aria-expanded="false">
          <div>
            <h4>${escapeHtml(STAGE_LABELS[row.stageName] || row.stageName)}</h4>
            <div class="workflow-stage-summary">${escapeHtml(summaryText || `sourceVersion=${cleanText(data.promptSourceVersion || '', 120) || 'n/a'}`)}</div>
          </div>
          <span class="draft-meta">Show</span>
        </button>
        <div class="workflow-stage-body" hidden>
          <div class="workflow-stage-grid">
            <div class="workflow-wide stage-explainer">
              <p><strong>${escapeHtml(explainer.summary || '')}</strong></p>
              <ul>
                ${(Array.isArray(explainer.details) ? explainer.details : []).map((item) => `<li>${escapeHtml(item)}</li>`).join('')}
              </ul>
            </div>
            <p class="signal-meta workflow-wide">sourceVersion=${escapeHtml(cleanText(data.promptSourceVersion || '', 120) || 'n/a')}</p>
          </div>
          ${warnings}
          <textarea class="field-final-prompt" rows="9" readonly>${escapeHtml(cleanText(data.compiledPrompt || '', 200000))}</textarea>
        </div>
      </section>
    `;
  }).join('');

  messageEl.textContent = `Loaded ${rows.length} stage prompt draft${rows.length === 1 ? '' : 's'}.`;
}

function setPersonaCardExpanded(card, expanded) {
  const editor = card?.querySelector(':scope > .article-editor');
  if (!editor) return;
  if (expanded) {
    editor.removeAttribute('hidden');
    editor.classList.remove('is-collapsed');
  } else {
    editor.setAttribute('hidden', '');
    editor.classList.add('is-collapsed');
  }
}

async function savePersonaDisplayName(card) {
  const id = cleanText(card?.dataset?.id || '', 255);
  if (!id) throw new Error('Persona ID is required');
  const section = cleanText(card.dataset.section || 'local', 80).toLowerCase() || 'local';
  const beat = cleanText(card.dataset.beat || 'general-local', 120) || 'general-local';
  const defaultDisplayName = cleanText(card.dataset.defaultName || id, 160);
  const currentVisibleName = cleanText(card.querySelector('.persona-name-text')?.textContent || '', 160);
  const normalizedDisplayName = currentVisibleName && currentVisibleName !== defaultDisplayName ? currentVisibleName : null;
  const avatarUrl = cleanText(card.querySelector('.field-avatar-url')?.value || '', 5000);
  const disclosure = cleanText(card.querySelector('.field-disclosure')?.value || '', 5000);
  const activationMode = cleanText(card.querySelector('.field-activation-mode')?.value || 'both', 20) || 'both';

  await apiRequest('/api/admin-personas', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id,
      displayName: normalizedDisplayName,
      section,
      beat,
      avatarUrl,
      disclosure,
      activationMode
    })
  });
}

async function loadPersonas() {
  try {
    setMessage('Loading personas...');
    const [data, promptData] = await Promise.all([
      apiRequest('/api/admin-personas'),
      apiRequest('/api/admin-prompt-layers')
    ]);
    loadedDraftWritingModelByProvider = data?.draftWritingModels && typeof data.draftWritingModels === 'object'
      ? { ...DEFAULT_DRAFT_WRITING_MODEL_BY_PROVIDER, ...data.draftWritingModels }
      : { ...DEFAULT_DRAFT_WRITING_MODEL_BY_PROVIDER };
    setPromptLayers(promptData.layers || []);
    renderGlobalPromptLayerList();
    renderPersonas(data.personas || []);
    renderPersonaFilterOptions();
    document.querySelectorAll('.persona-card').forEach((card) => updatePacingControlState(card));
    renderPersonaPipelineRuns();
    setMessage(`Loaded ${data.personas?.length || 0} persona configurations.`);
  } catch (err) {
    setMessage(`Failed to load personas: ${err.message}`);
  }
}

async function savePromptLayerFromEditor(editorEl) {
  if (!editorEl) return;
  const scopeType = cleanText(editorEl.dataset.scopeType || '', 40).toLowerCase();
  const stageName = cleanText(editorEl.dataset.stage || '', 120).toLowerCase();
  const section = cleanText(editorEl.dataset.section || '', 120).toLowerCase();
  const textarea = editorEl.querySelector('.field-layer-prompt');
  const versionEl = editorEl.querySelector('.field-layer-version');
  const saveBtn = editorEl.querySelector('.btn-save-layer');
  const cancelBtn = editorEl.querySelector('.btn-cancel-layer');
  const editBtn = editorEl.querySelector('.btn-edit-layer');
  if (!textarea || !versionEl || !saveBtn || !cancelBtn || !editBtn) return;

  const expectedVersion = Number.parseInt(String(versionEl.textContent || '').trim(), 10);
  const payload = {
    scopeType,
    stageName,
    section: scopeType === PROMPT_SCOPE_SECTION ? section : '',
    promptTemplate: textarea.value || ''
  };
  if (Number.isFinite(expectedVersion) && expectedVersion >= 1) payload.expectedVersion = expectedVersion;

  const data = await apiRequest('/api/admin-prompt-layers', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const row = data?.layer || null;
  if (!row) throw new Error('Prompt layer save did not return a row');

  const normalized = {
    ...row,
    scopeType: cleanText(row.scopeType, 40).toLowerCase(),
    stageName: cleanText(row.stageName, 120).toLowerCase(),
    section: cleanText(row.section || '', 120).toLowerCase() || null,
    promptTemplate: String(row.promptTemplate || ''),
    version: Number(row.version || 1)
  };
  promptLayersByKey.set(
    promptLayerKey(normalized.scopeType, normalized.stageName, normalized.section),
    normalized
  );

  textarea.value = normalized.promptTemplate;
  textarea.dataset.original = normalized.promptTemplate;
  textarea.readOnly = true;
  versionEl.textContent = String(normalized.version);
  saveBtn.setAttribute('hidden', '');
  cancelBtn.setAttribute('hidden', '');
  editBtn.removeAttribute('hidden');
}

async function savePersona(card) {
  const id = card.dataset.id;
  const section = cleanText(card.dataset.section || 'local', 80).toLowerCase() || 'local';
  const beat = cleanText(card.dataset.beat || 'general-local', 120) || 'general-local';
  const currentVisibleName = cleanText(card.querySelector('.persona-name-text')?.textContent || '', 160);
  const normalizedDisplayName = currentVisibleName && currentVisibleName !== id ? currentVisibleName : null;
  const avatarUrl = card.querySelector('.field-avatar-url').value;
  const disclosure = card.querySelector('.field-disclosure').value;
  const activationMode = card.querySelector('.field-activation-mode')?.value || 'both';
  const isAutoPromoteEnabled = Boolean(card.querySelector('.field-is-auto-promote-enabled')?.checked);
  const postsPerActiveDay = parseIntegerWithFallback(card.querySelector('.field-pacing-posts-per-day')?.value, 1);
  const isPausedMode = postsPerActiveDay <= 0;
  const isSingleMode = postsPerActiveDay === 1;
  const windowStartLocal = normalizeTimeInput(
    formatTimeWithMeridiem(
      card.querySelector('.field-pacing-window-start-time')?.value || '',
      card.querySelector('.field-pacing-window-start-meridiem')?.value || 'AM'
    ),
    '06:00:00'
  );
  const windowEndLocal = normalizeTimeInput(
    formatTimeWithMeridiem(
      card.querySelector('.field-pacing-window-end-time')?.value || '',
      card.querySelector('.field-pacing-window-end-meridiem')?.value || 'PM'
    ),
    '22:00:00'
  );
  const singlePostTimeLocal = normalizeOptionalTimeInput(
    formatTimeWithMeridiem(
      card.querySelector('.field-pacing-single-time')?.value || '',
      card.querySelector('.field-pacing-single-time-meridiem')?.value || 'AM'
    )
  );
  if (!isSingleMode && !isPausedMode && (!windowStartLocal || !windowEndLocal)) {
    throw new Error('Invalid pacing time format. Use H:MM with AM/PM.');
  }
  if (isSingleMode && !singlePostTimeLocal) {
    throw new Error('Single post time is required when posts/day is exactly 1.');
  }
  if (isSingleMode && card.querySelector('.field-pacing-single-time')?.value && !singlePostTimeLocal) {
    throw new Error('Invalid single post time format. Use H:MM with AM/PM.');
  }
  const pacingConfig = {
    enabled: true,
    postingDays: parsePostingDays(card.querySelector('.field-pacing-posting-days')?.value || ''),
    postsPerActiveDay,
    windowStartLocal: (isSingleMode || isPausedMode) ? '06:00:00' : windowStartLocal,
    windowEndLocal: (isSingleMode || isPausedMode) ? '22:00:00' : windowEndLocal,
    cadenceEnabled: (isSingleMode || isPausedMode) ? false : Boolean(card.querySelector('.field-pacing-cadence-enabled')?.checked),
    singlePostTimeLocal: isSingleMode ? singlePostTimeLocal : null,
    singlePostDaypart: null,
    minSpacingMinutes: 90,
    maxBacklog: 200,
    maxRetries: 3
  };
  const imageConfig = {
    imageDbEnabled: Boolean(card.querySelector('.field-image-db-enabled')?.checked),
    imageSourcingEnabled: Boolean(card.querySelector('.field-image-sourcing-enabled')?.checked),
    imageGenerationEnabled: Boolean(card.querySelector('.field-image-generation-enabled')?.checked),
    imageMode: cleanText(card.querySelector('.field-image-mode')?.value || 'manual', 20).toLowerCase() === 'auto' ? 'auto' : 'manual',
    imageProfile: (() => {
      const raw = cleanText(card.querySelector('.field-image-profile')?.value || 'professional', 30).toLowerCase();
      return raw === 'creative' || raw === 'cheap' ? raw : 'professional';
    })(),
    imageFallbackAssetUrl: cleanText(card.querySelector('.field-image-fallback-asset-url')?.value || '', 5000) || null,
    imageFallbackCloudinaryPublicId: cleanText(card.querySelector('.field-image-fallback-cloudinary-public-id')?.value || '', 500) || null,
    quotaPostgresImageDaily: Math.min(Math.max(parseIntegerWithFallback(card.querySelector('.field-quota-postgres-image-daily')?.value, 2), 0), 5000),
    quotaSourcedImageDaily: Math.min(Math.max(parseIntegerWithFallback(card.querySelector('.field-quota-sourced-image-daily')?.value, 2), 0), 5000),
    quotaGeneratedImageDaily: Math.min(Math.max(parseIntegerWithFallback(card.querySelector('.field-quota-generated-image-daily')?.value, 2), 0), 5000),
    quotaTextOnlyDaily: Math.min(Math.max(parseIntegerWithFallback(card.querySelector('.field-quota-text-only-daily')?.value, 3), 0), 5000),
    layer6TimeoutSeconds: Math.min(Math.max(parseIntegerWithFallback(card.querySelector('.field-layer6-timeout-seconds')?.value, 90), 15), 600),
    layer6BudgetUsd: Math.min(Math.max(parseNumberWithFallback(card.querySelector('.field-layer6-budget-usd')?.value, 0.20), 0), 50),
    exaMaxAttempts: Math.min(Math.max(Number.parseInt(String(card.querySelector('.field-exa-max-attempts')?.value || '3'), 10) || 3, 1), 20),
    generationMaxAttempts: Math.min(Math.max(Number.parseInt(String(card.querySelector('.field-generation-max-attempts')?.value || '2'), 10) || 2, 1), 20)
  };
  const feeds = parseFeedsFromText(card.querySelector('.field-feeds')?.value || '');
  const researchTrustConfig = parseResearchTrustFromText(
    card.querySelector('.field-research-trust')?.value || '',
    section,
    beat
  );
  const stageConfigs = {};
  const stageEls = card.querySelectorAll('.workflow-stage[data-stage]');
  for (const stageEl of stageEls) {
    const stageName = stageEl.dataset.stage;
    if (!stageName) continue;
    const workflowConfigRaw = stageEl.querySelector('.field-stage-workflow-config')?.value || '';
    let workflowConfig = {};
    if (workflowConfigRaw.trim()) {
      try {
        workflowConfig = JSON.parse(workflowConfigRaw);
      } catch (err) {
        throw new Error(`Invalid JSON in "${STAGE_LABELS[stageName] || stageName}" workflow config`);
      }
      if (!workflowConfig || typeof workflowConfig !== 'object' || Array.isArray(workflowConfig)) {
        throw new Error(`Workflow config for "${STAGE_LABELS[stageName] || stageName}" must be a JSON object`);
      }
    }
    stageConfigs[stageName] = {
      runnerType: HARD_CODED_STAGE_STACK[stageName]?.runnerType || 'llm',
      provider: isDraftWritingStage(stageName)
        ? (cleanText(stageEl.querySelector('.field-stage-provider')?.value || '', 80).toLowerCase() || HARD_CODED_STAGE_STACK[stageName]?.provider || '')
        : (HARD_CODED_STAGE_STACK[stageName]?.provider || ''),
      modelOrEndpoint: isDraftWritingStage(stageName)
        ? (getDraftWritingModelByProvider()[
            cleanText(stageEl.querySelector('.field-stage-provider')?.value || '', 80).toLowerCase()
          ] || getDraftWritingModelByProvider().openai)
        : (HARD_CODED_STAGE_STACK[stageName]?.modelOrEndpoint || ''),
      enabled: Boolean(stageEl.querySelector('.field-stage-enabled')?.checked),
      promptTemplate: stageEl.querySelector('.field-stage-prompt')?.value || '',
      workflowConfig
    };
  }

  await apiRequest('/api/admin-personas', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id,
      displayName: normalizedDisplayName,
      section,
      beat,
      avatarUrl,
      disclosure,
      activationMode,
      isAutoPromoteEnabled,
      imageConfig,
      pacingConfig,
      feeds,
      researchTrustConfig,
      stageConfigs
    })
  });
}

function parseSortControlValue(value) {
  const raw = String(value || 'created_at:desc');
  const [sortBy = 'created_at', sortDir = 'desc'] = raw.split(':');
  return {
    sortBy: String(sortBy).trim() || 'created_at',
    sortDir: String(sortDir).trim().toLowerCase() === 'asc' ? 'asc' : 'desc'
  };
}

function buildSignalsQueryParams() {
  const params = new URLSearchParams();
  if (currentSignalFilters.personaId) params.set('persona_id', currentSignalFilters.personaId);
  if (currentSignalFilters.action) params.set('action', currentSignalFilters.action);
  if (currentSignalFilters.reviewDecision) params.set('review_decision', currentSignalFilters.reviewDecision);
  params.set('page', String(signalPage));
  params.set('pageSize', String(signalPageSize));
  params.set('sortBy', signalSortBy);
  params.set('sortDir', signalSortDir);
  return params;
}

function renderSignalsSummary(summary24h) {
  const container = document.getElementById('signals-summary');
  if (!container) return;
  const totals = summary24h?.totals || { total: 0, promoted: 0, rejected: 0, watch: 0 };
  const byPersona = Array.isArray(summary24h?.byPersona) ? summary24h.byPersona : [];
  const topPersona = byPersona[0];
  container.innerHTML = `
    <article class="summary-card">
      <strong>Total (24h)</strong>
      <div class="signal-meta">${Number(totals.total || 0).toLocaleString()}</div>
    </article>
    <article class="summary-card">
      <strong>Promoted / Rejected / Watch</strong>
      <div class="signal-meta">${Number(totals.promoted || 0).toLocaleString()} / ${Number(totals.rejected || 0).toLocaleString()} / ${Number(totals.watch || 0).toLocaleString()}</div>
    </article>
    <article class="summary-card">
      <strong>Most Active Persona (24h)</strong>
      <div class="signal-meta">${topPersona ? `${escapeHtml(topPersona.personaId)} (${Number(topPersona.total || 0).toLocaleString()})` : 'none'}</div>
    </article>
  `;
}

function renderSignalsList(data) {
  const list = document.getElementById('signals-queue-list');
  const pagination = document.getElementById('signals-pagination');
  if (!list || !pagination) return;

  const signals = Array.isArray(data?.signals) ? data.signals : [];
  const page = Number(data?.pagination?.page || 1);
  const totalPages = Number(data?.pagination?.totalPages || 1);
  const total = Number(data?.pagination?.total || 0);

  if (!signals.length) {
    list.innerHTML = '<p class="signal-meta">No signals match this filter.</p>';
  } else {
    list.innerHTML = signals.map((signal) => `
      <article class="draft-card" data-signal-id="${signal.id}">
        <h4 class="signal-title">${escapeHtml(signal.title || '(untitled signal)')}</h4>
        <p class="signal-meta">
          persona=${escapeHtml(signal.personaId || 'n/a')} |
          action=${escapeHtml(signal.action || 'n/a')} |
          review=${escapeHtml(signal.reviewDecision || 'n/a')} |
          newsworthy=${signal.isNewsworthy == null ? 'n/a' : Number(signal.isNewsworthy).toFixed(3)}
        </p>
        <p class="signal-meta">
          source=${escapeHtml(signal.sourceType || 'n/a')} |
          relation=${escapeHtml(signal.relationToArchive || 'n/a')} |
          event=${escapeHtml(signal.eventKey || signal.dedupeKey || 'n/a')}
        </p>
        <p class="signal-meta">${escapeHtml(signal.reasoning || '')}</p>
        <div class="signal-flags">
          ${(Array.isArray(signal.policyFlags) ? signal.policyFlags : []).map((flag) => `<span class="signal-flag">${escapeHtml(flag)}</span>`).join('')}
        </div>
        <div class="signal-actions">
          <button type="button" class="btn btn-secondary btn-signal-action" data-action="watch">Set Watch</button>
          <button type="button" class="btn btn-primary btn-signal-action" data-action="promote">Promote</button>
          <button type="button" class="btn btn-danger btn-signal-action" data-action="reject">Reject</button>
        </div>
      </article>
    `).join('');
  }

  pagination.innerHTML = `
    <div class="signal-meta">Page ${page} / ${totalPages} (${total.toLocaleString()} total)</div>
    <div class="admin-actions">
      <button type="button" class="btn btn-secondary btn-signals-prev" ${page <= 1 ? 'disabled' : ''}>Previous</button>
      <button type="button" class="btn btn-secondary btn-signals-next" ${page >= totalPages ? 'disabled' : ''}>Next</button>
    </div>
  `;
}

async function loadSignals() {
  const params = buildSignalsQueryParams();
  const data = await apiRequest(`/api/admin/signals?${params.toString()}`);
  renderSignalsSummary(data.summary24h || {});
  renderSignalsList(data);
  return data;
}

async function updateSignalAction(signalId, action) {
  const notes = window.prompt('Optional review note (can be blank):', '') || '';
  return apiRequest(`/api/admin/signals/${encodeURIComponent(String(signalId))}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action, reviewNotes: notes })
  });
}

function getStageStatusClass(status) {
  const normalized = cleanText(status, 40).toLowerCase();
  if (normalized === 'completed') return 'is-completed';
  if (normalized === 'degraded') return 'is-in-progress';
  if (normalized === 'in_progress') return 'is-in-progress';
  if (normalized === 'timed_out') return 'is-failed';
  if (normalized === 'failed') return 'is-failed';
  return 'is-pending';
}

function formatStageStatusLabel(status) {
  const normalized = cleanText(status, 40).toLowerCase();
  if (normalized === 'in_progress') return 'in progress';
  if (normalized === 'timed_out') return 'timed out';
  if (normalized === 'phase_2_degraded') return 'phase 2 degraded';
  if (normalized === 'phase_2_failed') return 'phase 2 failed';
  return normalized || 'pending';
}

function formatResearchPassType(value) {
  const normalized = cleanText(value, 80).toLowerCase();
  if (normalized === 'strict_official') return 'strict official';
  if (normalized === 'broad_context') return 'broad context';
  if (normalized === 'historical_background') return 'historical background';
  return normalized ? normalized.replace(/_/g, ' ') : 'n/a';
}

function formatResearchReason(value) {
  const normalized = cleanText(value, 240).toLowerCase();
  if (normalized === 'used_broad_context_pass') return 'Used broad context rescue pass';
  if (normalized === 'used_historical_background_pass') return 'Used historical background rescue pass';
  if (normalized === 'insufficient_research_evidence') return 'No pass found enough usable evidence';
  if (normalized === 'research_search_plan_generation_failed') return 'Model output did not produce a valid search plan';
  if (normalized === 'research_search_plan_provider_unavailable') return 'Search plan provider unavailable';
  if (normalized === 'research_retrieval_failed') return 'Research retrieval failed';
  return normalized ? normalized.replace(/_/g, ' ') : 'n/a';
}

function renderResearchSummaryDetail(item) {
  const metadata = item && item.metadata && typeof item.metadata === 'object' ? item.metadata : {};
  const passSummaries = Array.isArray(metadata.passSummaries) ? metadata.passSummaries : [];
  return [
    metadata.status ? `<p class="signal-meta">status=${escapeHtml(formatStageStatusLabel(metadata.status))}</p>` : '',
    metadata.successfulPassType ? `<p class="signal-meta">successfulPass=${escapeHtml(formatResearchPassType(metadata.successfulPassType))}</p>` : '',
    metadata.degradationReason ? `<p class="signal-meta">degraded=${escapeHtml(formatResearchReason(metadata.degradationReason))}</p>` : '',
    metadata.failureReason ? `<p class="signal-meta">failure=${escapeHtml(formatResearchReason(metadata.failureReason))}</p>` : '',
    passSummaries.map((pass) => `
      <article class="pipeline-stage-detail-item">
        <p class="signal-meta"><strong>Pass ${escapeHtml(String(pass.passIndex || '?'))}: ${escapeHtml(formatResearchPassType(pass.passType || ''))}</strong></p>
        ${pass.intent ? `<p class="signal-meta">${escapeHtml(pass.intent)}</p>` : ''}
        <p class="signal-meta">usable=${Number(pass.usableResultCount || 0)} | fetched=${Number(pass.fetchedResultCount || 0)} | domains=${Number(pass.distinctDomainCount || 0)} | sufficiency=${pass.sufficiencyMet === true ? 'yes' : 'no'}</p>
        ${Array.isArray(pass.appliedDomains) && pass.appliedDomains.length ? `<p class="signal-meta">domains=${escapeHtml(pass.appliedDomains.join(', '))}</p>` : '<p class="signal-meta">domains=whole web</p>'}
        ${Number.isFinite(Number(pass.appliedMaxAgeDays)) ? `<p class="signal-meta">maxAgeDays=${Number(pass.appliedMaxAgeDays)}</p>` : '<p class="signal-meta">maxAgeDays=none</p>'}
      </article>
    `).join('')
  ].join('');
}

function buildPipelineRunsQueryParams() {
  const params = new URLSearchParams();
  params.set('limit', '100');
  return params;
}

function renderPipelineStageDetailItems(items) {
  if (!Array.isArray(items) || !items.length) {
    return '<p class="signal-meta">No stage artifacts yet.</p>';
  }
  return items.map((item) => `
    <article class="pipeline-stage-detail-item">
      <p class="signal-meta"><strong>${escapeHtml(item.title || item.artifactType || 'artifact')}</strong></p>
      ${cleanText(item.artifactType, 120).toLowerCase() === 'search_plan_summary' ? renderResearchSummaryDetail(item) : ''}
      ${item.sourceUrl ? `<p class="signal-meta">source=<a href="${escapeHtml(item.sourceUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.sourceUrl)}</a></p>` : ''}
      ${item.query ? `<p class="signal-meta">query=${escapeHtml(item.query)}</p>` : ''}
      ${item.provider ? `<p class="signal-meta">provider=${escapeHtml(item.provider)}</p>` : ''}
      ${item.model ? `<p class="signal-meta">model=${escapeHtml(item.model)}</p>` : ''}
      ${Number.isFinite(item.rank) ? `<p class="signal-meta">rank=${Number(item.rank)}</p>` : ''}
      ${Number.isFinite(item.score) ? `<p class="signal-meta">score=${Number(item.score).toFixed(3)}</p>` : ''}
      ${Number.isFinite(item.confidence) ? `<p class="signal-meta">confidence=${Number(item.confidence).toFixed(3)}</p>` : ''}
      ${Number.isFinite(item.sectionCount) ? `<p class="signal-meta">sections=${Number(item.sectionCount)}</p>` : ''}
      ${Number.isFinite(item.evidenceCount) ? `<p class="signal-meta">evidence=${Number(item.evidenceCount)}</p>` : ''}
      ${item.evidenceQuote ? `<p class="signal-meta">quote=${escapeHtml(item.evidenceQuote)}</p>` : ''}
      ${item.whyItMatters ? `<p class="signal-meta">why=${escapeHtml(item.whyItMatters)}</p>` : ''}
      ${item.content ? `<p class="signal-meta">${escapeHtml(item.content)}</p>` : ''}
      ${item.createdAt ? `<p class="signal-meta">created=${escapeHtml(formatDate(item.createdAt))}</p>` : ''}
    </article>
  `).join('');
}

function renderPersonaPipelineRunCard(run) {
  const statusClass = getStageStatusClass(
    run.runStatus === 'phase_6_complete'
      || run.runStatus === 'phase_2_complete'
      || run.runStatus === 'phase_5_complete'
      || run.runStatus === 'phase_4_complete'
      || run.runStatus === 'phase_3_complete'
      ? 'completed'
      : run.runStatus === 'phase_2_degraded'
        ? 'degraded'
      : run.runStatus === 'phase_6_failed'
        || run.runStatus === 'phase_2_failed'
        || run.runStatus === 'phase_6_timed_out'
        || run.runStatus === 'blocked'
        ? 'failed'
      : run.runStatus === 'queued'
        ? 'in_progress'
        : run.runStatus
  );
  const stageChips = (Array.isArray(run.stageProgress) ? run.stageProgress : []).map((stage) => `
    <article class="pipeline-stage-chip">
      <h5>${escapeHtml(STAGE_LABELS[stage.stage] || stage.stage)}</h5>
      <span class="pipeline-run-status ${getStageStatusClass(stage.status)}">${escapeHtml(formatStageStatusLabel(stage.status))}</span>
      <p class="signal-meta">artifacts=${Number(stage.artifactCount || 0).toLocaleString()}</p>
      ${stage.latestAt ? `<p class="signal-meta">latest=${escapeHtml(formatDate(stage.latestAt))}</p>` : ''}
    </article>
  `).join('');

  const stageDetails = (Array.isArray(run.stageProgress) ? run.stageProgress : []).map((stage) => `
    <details>
      <summary>${escapeHtml(STAGE_LABELS[stage.stage] || stage.stage)} (${escapeHtml(formatStageStatusLabel(stage.status))})</summary>
      <div class="pipeline-stage-detail-body">
        <p class="signal-meta">Artifacts: ${Number(stage.artifactCount || 0).toLocaleString()}</p>
        ${stage.latestAt ? `<p class="signal-meta">Latest update: ${escapeHtml(formatDate(stage.latestAt))}</p>` : ''}
        ${renderPipelineStageDetailItems(stage.details || [])}
      </div>
    </details>
  `).join('');

  return `
    <article class="draft-card" data-pipeline-signal-id="${run.signalId}">
      <div class="pipeline-run-header">
        <h4 class="pipeline-run-title">${escapeHtml(run.title || `(signal ${run.signalId})`)}</h4>
        <span class="pipeline-run-status ${statusClass}">${escapeHtml(cleanText(run.runStatus, 40).replace(/_/g, ' '))}</span>
      </div>
      <p class="signal-meta">signal=#${Number(run.signalId)} | current=${escapeHtml(STAGE_LABELS[run.currentStage] || run.currentStage || 'n/a')}</p>
      <p class="signal-meta">action=${escapeHtml(run.action || 'n/a')} | review=${escapeHtml(run.reviewDecision || 'n/a')} | next=${escapeHtml(run.nextStep || 'n/a')}</p>
      <p class="signal-meta">event=${escapeHtml(run.eventKey || 'n/a')} | lastActivity=${escapeHtml(run.lastActivityAt ? formatDate(run.lastActivityAt) : 'n/a')}</p>
      ${run.researchDiscovery ? `<p class="signal-meta">phase2=${escapeHtml(formatStageStatusLabel(run.researchDiscovery.status || 'pending'))} | pass=${escapeHtml(formatResearchPassType(run.researchDiscovery.successfulPassType || ''))} | reason=${escapeHtml(formatResearchReason(run.researchDiscovery.degradationReason || run.researchDiscovery.failureReason || ''))}</p>` : ''}
      ${run.queue ? `<p class="signal-meta">queue=${escapeHtml(run.queue.status || 'n/a')} | reason=${escapeHtml(run.queue.reasonCode || 'n/a')} | scheduled=${escapeHtml(run.queue.scheduledForUtc ? formatDate(run.queue.scheduledForUtc) : 'n/a')} | released=${escapeHtml(run.queue.releasedAt ? formatDate(run.queue.releasedAt) : 'n/a')}</p>` : '<p class="signal-meta">queue=n/a (manual route or pacing bypass)</p>'}
      <div class="pipeline-stages">${stageChips}</div>
      <details class="pipeline-stage-details">
        <summary>Expand Full Stage Details</summary>
        <div class="pipeline-stage-detail-body">
          ${run.decisionDetails?.reasoning ? `<p class="signal-meta"><strong>Gatekeeper reasoning:</strong> ${escapeHtml(run.decisionDetails.reasoning)}</p>` : ''}
          ${run.decisionDetails?.reviewNotes ? `<p class="signal-meta"><strong>Review notes:</strong> ${escapeHtml(run.decisionDetails.reviewNotes)}</p>` : ''}
          ${(run.decisionDetails?.policyFlags || []).length ? `<p class="signal-meta"><strong>Policy flags:</strong> ${(run.decisionDetails.policyFlags || []).map((f) => escapeHtml(f)).join(', ')}</p>` : ''}
          ${stageDetails}
        </div>
      </details>
    </article>
  `;
}

function renderPersonaPipelineRuns() {
  const runs = Array.isArray(latestPipelineRuns) ? latestPipelineRuns : [];
  const runsByPersona = new Map();
  for (const run of runs) {
    const personaId = cleanText(run?.personaId || '', 255);
    if (!personaId) continue;
    if (!runsByPersona.has(personaId)) runsByPersona.set(personaId, []);
    runsByPersona.get(personaId).push(run);
  }

  document.querySelectorAll('.persona-card').forEach((card) => {
    const personaId = cleanText(card?.dataset?.id || '', 255);
    const body = card.querySelector('.persona-pipeline-runs-body');
    if (!body) return;
    const personaRuns = runsByPersona.get(personaId) || [];
    if (!personaRuns.length) {
      body.innerHTML = '<p class="signal-meta">No promoted runs yet for this persona.</p>';
      return;
    }
    body.innerHTML = `<div class="persona-pipeline-runs-list">${personaRuns.map((run) => renderPersonaPipelineRunCard(run)).join('')}</div>`;
  });
}

async function loadPipelineRuns() {
  const params = buildPipelineRunsQueryParams();
  const data = await apiRequest(`/api/admin/persona-pipeline-runs?${params.toString()}`);
  latestPipelineRuns = Array.isArray(data?.runs) ? data.runs : [];
  renderPersonaPipelineRuns();
  return data;
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

    sessionStorage.setItem('de_admin_unlocked_settings', '1');
    setLockState(true);
    setMessage('Settings unlocked.');
    if (getToken()) {
      await syncAdminTimezoneFromBrowser();
      await Promise.all([loadPersonas(), loadSignals(), loadPipelineRuns()]);
    }
  } catch (err) {
    setMessage(`Unlock failed: ${err.message}`);
  }
}

async function apiRequest(url, options = {}) {
  if (!unlocked) throw new Error('Settings page is locked');
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
    const error = new Error(data.details ? `${message}: ${data.details}` : message);
    error.status = res.status;
    error.payload = data;
    throw error;
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

function onAppSectionClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  const button = target.closest('button');
  if (!button) return;

  if (button.classList.contains('btn-toggle-layer-editor')) {
    const editor = button.closest('.prompt-layer-editor');
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setStageExpanded(editor, !isExpanded);
    return;
  }

  if (button.classList.contains('btn-edit-layer')) {
    const editor = button.closest('.prompt-layer-editor');
    const textarea = editor?.querySelector('.field-layer-prompt');
    const saveBtn = editor?.querySelector('.btn-save-layer');
    const cancelBtn = editor?.querySelector('.btn-cancel-layer');
    if (!editor || !textarea || !saveBtn || !cancelBtn) return;
    textarea.readOnly = false;
    textarea.focus();
    saveBtn.removeAttribute('hidden');
    cancelBtn.removeAttribute('hidden');
    button.setAttribute('hidden', '');
    return;
  }

  if (button.classList.contains('btn-cancel-layer')) {
    const editor = button.closest('.prompt-layer-editor');
    const textarea = editor?.querySelector('.field-layer-prompt');
    const saveBtn = editor?.querySelector('.btn-save-layer');
    const editBtn = editor?.querySelector('.btn-edit-layer');
    if (!editor || !textarea || !saveBtn || !editBtn) return;
    textarea.value = textarea.dataset.original || '';
    textarea.readOnly = true;
    button.setAttribute('hidden', '');
    saveBtn.setAttribute('hidden', '');
    editBtn.removeAttribute('hidden');
    return;
  }

  if (button.classList.contains('btn-save-layer')) {
    const editor = button.closest('.prompt-layer-editor');
    button.disabled = true;
    savePromptLayerFromEditor(editor)
      .then(() => {
        setMessage('Prompt layer saved.');
      })
      .catch((err) => {
        if (Number(err?.status) === 409 && err?.payload?.current) {
          const current = err.payload.current;
          const textarea = editor?.querySelector('.field-layer-prompt');
          const versionEl = editor?.querySelector('.field-layer-version');
          const editBtn = editor?.querySelector('.btn-edit-layer');
          const cancelBtn = editor?.querySelector('.btn-cancel-layer');
          if (textarea) {
            const text = String(current.promptTemplate || '');
            textarea.value = text;
            textarea.dataset.original = text;
            textarea.readOnly = true;
          }
          if (versionEl) versionEl.textContent = String(current.version || 'new');
          if (editBtn) editBtn.removeAttribute('hidden');
          if (cancelBtn) cancelBtn.setAttribute('hidden', '');
          button.setAttribute('hidden', '');
          setMessage('Prompt layer changed by another editor. Refreshed to latest.');
        } else {
          setMessage(`Prompt layer save failed: ${err.message}`);
        }
      })
      .finally(() => {
        button.disabled = false;
      });
    return;
  }

  if (!button.classList.contains('draft-toggle')) return;
  const card = button.closest('.draft-card');
  if (!card) return;
  if (card.classList.contains('persona-card')) {
    endPersonaRename(card, false);
  }
  const editors = card.querySelectorAll(':scope > .section-editor, :scope > .article-editor');
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
}

function onPersonaListClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const button = target.closest('button');
  if (!(button instanceof HTMLElement)) return;

  const card = button.closest('.persona-card');
  const sectionCard = button.closest('.section-card');

  if (button.classList.contains('btn-toggle-section-prompts')) {
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setSectionPromptsExpanded(sectionCard, !isExpanded);
    return;
  }

  if (button.classList.contains('btn-toggle-final-prompt')) {
    const stageEl = button.closest('.workflow-stage');
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setFinalPromptExpanded(stageEl, !isExpanded);
    if (!isExpanded) {
      loadFinalPromptPreview(stageEl).catch((err) => {
        const messageEl = stageEl?.querySelector('.final-prompt-message');
        if (messageEl) messageEl.textContent = `Preview failed: ${err.message}`;
      });
    }
    return;
  }

  if (button.classList.contains('btn-toggle-persona-final-prompts')) {
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setPersonaFinalPromptsExpanded(card, !isExpanded);
    if (!isExpanded) {
      loadPersonaFinalPromptBundle(card).catch((err) => {
        const msg = card.querySelector('.persona-final-prompts-message');
        if (msg) msg.textContent = `Preview failed: ${err.message}`;
      });
    }
    return;
  }

  if (button.classList.contains('btn-load-persona-final-prompts')) {
    button.disabled = true;
    loadPersonaFinalPromptBundle(card)
      .catch((err) => {
        const msg = card.querySelector('.persona-final-prompts-message');
        if (msg) msg.textContent = `Preview failed: ${err.message}`;
      })
      .finally(() => { button.disabled = false; });
    return;
  }

  if (button.classList.contains('btn-toggle-persona-final-stage')) {
    const stageEl = button.closest('.workflow-stage');
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setStageExpanded(stageEl, !isExpanded);
    const meta = button.querySelector('.draft-meta');
    if (meta) meta.textContent = isExpanded ? 'Show' : 'Hide';
    return;
  }

  if (button.classList.contains('btn-load-final-prompt')) {
    const stageEl = button.closest('.workflow-stage');
    button.disabled = true;
    loadFinalPromptPreview(stageEl)
      .catch((err) => {
        const messageEl = stageEl?.querySelector('.final-prompt-message');
        if (messageEl) messageEl.textContent = `Preview failed: ${err.message}`;
      })
      .finally(() => { button.disabled = false; });
    return;
  }

  if (!card) return;

  if (button.classList.contains('btn-rename-persona')) {
    const renameRow = card.querySelector('.persona-rename-row');
    const input = card.querySelector('.field-display-name-inline');
    const currentName = card.querySelector('.persona-name-text')?.textContent || card.dataset.defaultName || card.dataset.id || '';
    if (renameRow) renameRow.removeAttribute('hidden');
    if (input) {
      input.value = cleanText(currentName, 160);
      input.focus();
      input.select();
    }
    return;
  }

  if (button.classList.contains('btn-save-display-name')) {
    button.disabled = true;
    endPersonaRename(card, true);
    savePersonaDisplayName(card)
      .then(() => {
        const savedName = cleanText(card.querySelector('.persona-name-text')?.textContent || '', 160);
        loadedPersonaDisplayNames.set(card.dataset.id || '', savedName);
        renderPersonaFilterOptions();
        setMessage(`Topic engine name saved for '${card.dataset.id}'.`);
      })
      .catch((err) => setMessage(`Name save failed: ${err.message}`))
      .finally(() => { button.disabled = false; });
    return;
  }

  if (button.classList.contains('btn-toggle-stage')) {
    const stageEl = button.closest('.workflow-stage');
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setStageExpanded(stageEl, !isExpanded);
    return;
  }

  if (button.classList.contains('btn-toggle-advanced')) {
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setAdvancedExpanded(card, !isExpanded);
    return;
  }

  if (button.classList.contains('btn-toggle-persona-pipeline-runs')) {
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setPersonaPipelineRunsExpanded(card, !isExpanded);
    return;
  }

  if (button.classList.contains('btn-upload-avatar')) {
    card.querySelector('.file-image')?.click();
    return;
  }

  if (button.classList.contains('btn-upload-fallback-image')) {
    card.querySelector('.file-fallback-image')?.click();
    return;
  }

  if (button.classList.contains('btn-save-persona')) {
    button.disabled = true;
    savePersona(card)
      .then(() => {
        const savedName = cleanText(card.querySelector('.persona-name-text')?.textContent || '', 160);
        loadedPersonaDisplayNames.set(card.dataset.id || '', savedName);
        renderPersonaFilterOptions();
        setMessage(`Persona '${card.dataset.id}' saved.`);
        setAdvancedExpanded(card, false);
        setPersonaCardExpanded(card, false);
        window.scrollTo({ top: 0, behavior: 'smooth' });
      })
      .catch(err => setMessage(`Save failed: ${err.message}`))
      .finally(() => { button.disabled = false; });
  }
}

function applyPersonaDisplayName(card, nextName) {
  if (!card) return;
  const defaultDisplayName = cleanText(card.dataset.defaultName || card.dataset.id || '', 160);
  const normalized = cleanText(nextName, 160) || defaultDisplayName;
  const committed = normalized === defaultDisplayName ? '' : normalized;
  const nameText = card.querySelector('.persona-name-text');
  const input = card.querySelector('.field-display-name-inline');
  if (nameText) nameText.textContent = normalized;
  if (input) input.value = normalized;
  card.dataset.displayNameCommitted = committed;
}

function endPersonaRename(card, commit) {
  if (!card) return;
  const renameRow = card.querySelector('.persona-rename-row');
  const input = card.querySelector('.field-display-name-inline');
  const committedName = cleanText(
    card.dataset.displayNameCommitted || card.querySelector('.persona-name-text')?.textContent || '',
    160
  ) || cleanText(card.dataset.defaultName || card.dataset.id || '', 160);

  if (commit) {
    applyPersonaDisplayName(card, input?.value || committedName);
  } else if (input) {
    input.value = committedName;
  }

  if (renameRow) renameRow.setAttribute('hidden', '');
}

function onPersonaListChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target instanceof HTMLInputElement && target.type === 'file') {
    const card = target.closest('.persona-card');
    const file = target.files?.[0];
    if (card && file) {
      if (target.classList.contains('file-fallback-image')) {
        handlePersonaFallbackImageUpload(card, file);
      } else {
        handlePersonaImageUpload(card, file);
      }
    }
    return;
  }

  const card = target.closest('.persona-card');
  if (card && (
    target.classList.contains('field-pacing-posts-per-day') ||
    target.classList.contains('field-pacing-cadence-enabled')
  )) {
    updatePacingControlState(card);
    return;
  }

  const stageEl = target.closest('.workflow-stage');
  if (!stageEl) return;
  if (target.classList.contains('field-stage-provider') && isDraftWritingStage(stageEl.dataset.stage)) {
    syncDraftWritingModelSelection(stageEl);
    return;
  }
}

function onPersonaListDragOver(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target?.closest('.field-image-fallback-asset-url')) return;
  event.preventDefault();
}

function onPersonaListDrop(event) {
  const target = event.target instanceof Element ? event.target : null;
  const input = target?.closest('.field-image-fallback-asset-url');
  if (!input) return;
  event.preventDefault();
  const card = input.closest('.persona-card');
  const file = event.dataTransfer?.files?.[0];
  if (card && file) {
    handlePersonaFallbackImageUpload(card, file);
  }
}

function onPersonaListFocusOut(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  if (!target.classList.contains('field-display-name-inline')) return;
  const card = target.closest('.persona-card');
  if (!card) return;
  const related = event.relatedTarget instanceof HTMLElement ? event.relatedTarget : null;
  if (related && related.closest('.persona-rename-row')) return;
  endPersonaRename(card, false);
}

function onPersonaListKeyDown(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  if (!target.classList.contains('field-display-name-inline')) return;
  const card = target.closest('.persona-card');
  if (!card) return;

  if (event.key === 'Enter') {
    event.preventDefault();
    const saveButton = card.querySelector('.btn-save-display-name');
    if (saveButton instanceof HTMLButtonElement && !saveButton.disabled) {
      saveButton.click();
    } else {
      endPersonaRename(card, true);
      renderPersonaFilterOptions();
    }
    return;
  }
  if (event.key === 'Escape') {
    event.preventDefault();
    endPersonaRename(card, false);
  }
}

function applySignalsFilterControls() {
  const personaInput = document.getElementById('signals-filter-persona');
  const actionSelect = document.getElementById('signals-filter-action');
  const reviewSelect = document.getElementById('signals-filter-review');
  const sortSelect = document.getElementById('signals-sort');
  currentSignalFilters = {
    personaId: String(personaInput?.value || '').trim(),
    action: String(actionSelect?.value || '').trim(),
    reviewDecision: String(reviewSelect?.value || '').trim()
  };
  const sort = parseSortControlValue(sortSelect?.value || 'created_at:desc');
  signalSortBy = sort.sortBy;
  signalSortDir = sort.sortDir;
  signalPage = 1;
}

function onSignalsPanelClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const button = target.closest('button');
  if (!(button instanceof HTMLElement)) return;

  if (button.id === 'apply-signals-filters-btn' || button.id === 'load-signals-btn') {
    applySignalsFilterControls();
    loadSignals()
      .then(() => setMessage('Signals queue loaded.'))
      .catch((err) => setMessage(`Signals load failed: ${err.message}`));
    return;
  }

  if (button.classList.contains('btn-signals-prev')) {
    if (signalPage > 1) signalPage -= 1;
    loadSignals().catch((err) => setMessage(`Signals load failed: ${err.message}`));
    return;
  }

  if (button.classList.contains('btn-signals-next')) {
    signalPage += 1;
    loadSignals().catch((err) => setMessage(`Signals load failed: ${err.message}`));
    return;
  }

  if (button.classList.contains('btn-signal-action')) {
    const card = button.closest('[data-signal-id]');
    const signalId = Number(card?.getAttribute('data-signal-id') || 0);
    const action = String(button.getAttribute('data-action') || '').trim().toLowerCase();
    if (!signalId || !action) return;
    button.disabled = true;
    let actionResult = null;
    updateSignalAction(signalId, action)
      .then((result) => {
        actionResult = result || null;
      })
      .then(() => Promise.all([loadSignals(), loadPipelineRuns()]))
      .then(() => {
        const finalAction = String(actionResult?.signal?.action || action).toLowerCase();
        const nextStep = String(actionResult?.signal?.nextStep || '').toLowerCase();
        const trigger = actionResult?.manualTrigger || null;
        if (finalAction === 'promote' && nextStep === 'research_discovery') {
          if (trigger?.sent) {
            setMessage(`Signal #${signalId} set to 'promote' and routed to research.`);
            return;
          }
          const reason = trigger?.reason || (Number.isFinite(trigger?.status) ? `HTTP ${trigger.status}` : 'event_send_failed');
          setMessage(`Signal #${signalId} set to 'promote', but routing failed (${reason}).`);
          return;
        }
        setMessage(`Signal #${signalId} set to '${finalAction}'.`);
      })
      .catch((err) => setMessage(`Signal update failed: ${err.message}`))
      .finally(() => { button.disabled = false; });
    return;
  }
}

function init() {
  if (unlockAdminBtn) unlockAdminBtn.addEventListener('click', unlock);
  if (saveTokenBtn) saveTokenBtn.addEventListener('click', saveToken);
  if (appSection) appSection.addEventListener('click', onAppSectionClick);

  // Inject UI and Styles
  injectPersonaStyles();
  ensurePersonaUi();
  ensureSignalsUi();

  // Bind Persona Events
  const loadPersonasBtn = document.getElementById('load-personas-btn');
  const addPersonaBtn = document.getElementById('add-persona-btn');
  const cancelAddPersonaBtn = document.getElementById('cancel-add-persona-btn');
  const createPersonaBtn = document.getElementById('create-persona-btn');
  const newPersonaSection = document.getElementById('new-persona-section');
  const newPersonaBeatSelect = document.getElementById('new-persona-beat');
  const newPersonaNameInput = document.getElementById('new-persona-display-name');
  const newPersonaAddBeatBtn = document.getElementById('new-persona-add-beat-btn');
  const saveNewBeatBtn = document.getElementById('save-new-beat-btn');
  const cancelNewBeatBtn = document.getElementById('cancel-new-beat-btn');
  const newBeatNameInput = document.getElementById('new-beat-name-input');
  const personaListEl = document.getElementById('persona-settings-list');
  if (loadPersonasBtn) loadPersonasBtn.addEventListener('click', loadPersonas);
  if (addPersonaBtn) addPersonaBtn.addEventListener('click', () => toggleAddPersonaPanel(true));
  if (cancelAddPersonaBtn) cancelAddPersonaBtn.addEventListener('click', () => toggleAddPersonaPanel(false));
  if (newPersonaSection) {
    newPersonaSection.addEventListener('change', () => {
      syncNewPersonaBeatOptions();
      toggleNewBeatFlyout(false);
    });
  }
  if (newPersonaBeatSelect) {
    newPersonaBeatSelect.addEventListener('change', () => {
      if (newPersonaBeatSelect.value === ADD_NEW_BEAT_VALUE) toggleNewBeatFlyout(true);
    });
  }
  if (newPersonaAddBeatBtn) newPersonaAddBeatBtn.addEventListener('click', () => toggleNewBeatFlyout(true));
  if (cancelNewBeatBtn) cancelNewBeatBtn.addEventListener('click', () => toggleNewBeatFlyout(false));
  if (saveNewBeatBtn) {
    saveNewBeatBtn.addEventListener('click', () => {
      try {
        saveNewBeatFromFlyout();
      } catch (err) {
        setMessage(`Beat save failed: ${err.message}`);
      }
    });
  }
  if (newBeatNameInput) {
    newBeatNameInput.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        try {
          saveNewBeatFromFlyout();
        } catch (err) {
          setMessage(`Beat save failed: ${err.message}`);
        }
      }
      if (event.key === 'Escape') {
        event.preventDefault();
        toggleNewBeatFlyout(false);
      }
    });
  }
  if (createPersonaBtn) {
    createPersonaBtn.addEventListener('click', () => {
      createPersonaBtn.disabled = true;
      createPersonaFromInputs()
        .catch((err) => setMessage(`Create failed: ${err.message}`))
        .finally(() => { createPersonaBtn.disabled = false; });
    });
  }
  if (personaListEl) {
    personaListEl.addEventListener('click', onPersonaListClick);
    personaListEl.addEventListener('change', onPersonaListChange);
    personaListEl.addEventListener('dragover', onPersonaListDragOver);
    personaListEl.addEventListener('drop', onPersonaListDrop);
    personaListEl.addEventListener('focusout', onPersonaListFocusOut);
    personaListEl.addEventListener('keydown', onPersonaListKeyDown);
  }
  const loadSignalsBtn = document.getElementById('load-signals-btn');
  const applySignalsFiltersBtn = document.getElementById('apply-signals-filters-btn');
  const signalsQueueListEl = document.getElementById('signals-queue-list');
  const signalsPaginationEl = document.getElementById('signals-pagination');
  renderPersonaFilterOptions();
  if (loadSignalsBtn) loadSignalsBtn.addEventListener('click', onSignalsPanelClick);
  if (applySignalsFiltersBtn) applySignalsFiltersBtn.addEventListener('click', onSignalsPanelClick);
  if (signalsQueueListEl) signalsQueueListEl.addEventListener('click', onSignalsPanelClick);
  if (signalsPaginationEl) signalsPaginationEl.addEventListener('click', onSignalsPanelClick);

  loadToken();
  setLockState(sessionStorage.getItem('de_admin_unlocked_settings') === '1');
  if (unlocked && getToken()) {
    syncAdminTimezoneFromBrowser()
      .then(() => Promise.all([loadPersonas(), loadSignals(), loadPipelineRuns()]))
      .catch((err) => setMessage(`Load failed: ${err.message}`));
  }
}

init();
