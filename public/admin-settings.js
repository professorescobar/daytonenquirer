const lockSection = document.getElementById('settings-lock');
const appSection = document.getElementById('settings-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');
const tokenInput = document.getElementById('admin-token');
const manualBudgetInput = document.getElementById('manual-budget-input');
const runLimitInput = document.getElementById('run-limit');
const saveTokenBtn = document.getElementById('save-token-btn');
const saveManualBudgetBtn = document.getElementById('save-manual-budget-btn');
const loadRunsBtn = document.getElementById('load-runs-btn');
const messageEl = document.getElementById('settings-message');
const generationRunListEl = document.getElementById('generation-run-list');
const runFilterInput = document.getElementById('run-filter');

let unlocked = false;
let generationRuns = [];
let signalPage = 1;
let signalPageSize = 25;
let signalSortBy = 'created_at';
let signalSortDir = 'desc';
let currentSignalFilters = {
  personaId: '',
  action: '',
  reviewDecision: ''
};
let topicEngineSettings = [];

function setMessage(text) {
  if (!messageEl) return;
  messageEl.hidden = !text;
  messageEl.textContent = text || '';
}

function getToken() {
  return (tokenInput.value || '').trim();
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
  research_discovery: 'Research Discovery',
  evidence_extraction: 'Evidence Extraction',
  story_planning: 'Story Planning',
  draft_writing: 'Draft Writing',
  final_review: 'Final Review'
};
const RUNNER_TYPE_OPTIONS = [
  { value: 'llm', label: 'LLM' },
  { value: 'api_workflow', label: 'API Workflow' },
  { value: 'tool', label: 'Tool' },
  { value: 'script', label: 'Script' }
];
const RECOMMENDED_STAGE_STACK = {
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
    provider: 'anthropic',
    modelOrEndpoint: 'claude-3-5-sonnet'
  },
  final_review: {
    runnerType: 'llm',
    provider: 'openai',
    modelOrEndpoint: 'gpt-4o'
  }
};

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
    for (const beat of beats) {
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
  return personas;
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

function formatTopSkipReasons(reasons) {
  if (!Array.isArray(reasons) || reasons.length === 0) return 'none';
  return reasons
    .slice(0, 5)
    .map((item) => `${escapeHtml(item.reason || 'unknown')} (${Number(item.count || 0).toLocaleString()})`)
    .join(', ');
}

function isUnderfilledRun(run) {
  const target = Number(run?.targetCount || 0);
  const created = Number(run?.createdCount || 0);
  if (target > 0 && created < target) return true;
  return String(run?.runReason || '').toLowerCase() === 'underfilled';
}

function shouldIncludeRunByFilter(run, filterValue) {
  const status = String(run?.runStatus || '').toLowerCase();
  const failedOrSkipped = status === 'error' || status === 'invalid_request' || status === 'skipped';
  const underfilled = isUnderfilledRun(run);
  if (filterValue === 'failed_or_skipped') return failedOrSkipped;
  if (filterValue === 'underfilled') return underfilled;
  if (filterValue === 'failed_or_underfilled') return failedOrSkipped || underfilled;
  return true;
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
    .persona-summary {
      color: #555;
      margin-top: -0.5rem;
      margin-bottom: 1rem;
      font-size: 0.9rem;
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
    .section-card {
      background: #fdfdfd;
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
    .persona-autonomy-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 0.5rem;
      margin-bottom: 0.8rem;
    }
    .persona-autonomy-item {
      border: 1px solid #ddd;
      background: #fff;
      padding: 0.45rem 0.55rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
      font-size: 0.9rem;
    }
    .persona-autonomy-item button {
      white-space: nowrap;
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
    <div class="section-header">
      <h2>Persona Management</h2>
      <button id="load-personas-btn" class="btn btn-secondary" type="button">Refresh Personas</button>
    </div>
    <p class="hint">Curate avatar/disclosure, activation mode, discovery feeds, and full per-stage workflow settings for each topic engine.</p>
    <div id="persona-settings-list" class="draft-list"></div>
  `;
  appSection.appendChild(container);
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
        <input id="signals-filter-persona" type="text" placeholder="local-reporter" />
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
    <h3>Autonomy Controls</h3>
    <div id="persona-autonomy-grid" class="persona-autonomy-grid"></div>
    <div id="signals-queue-list" class="signals-list"></div>
    <div id="signals-pagination" class="pagination-row"></div>
  `;
  appRoot.appendChild(section);
}

function renderPersonas(personas) {
  const personaListEl = document.getElementById('persona-settings-list');
  if (!personaListEl) return;
  const definedPersonas = getAllDefinedPersonas();
  const personaDataMap = new Map(personas.map(p => [p.id, p]));

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
            <div class="persona-nested-list">
              ${items.map((p) => {
                const data = personaDataMap.get(p.id) || {};
                const activationMode = String(data.activationMode || 'both');
                const isAutoPromoteEnabled = data.isAutoPromoteEnabled === true;
                const feedsText = formatFeedsForTextArea(data.feeds);
                return `
                  <article class="draft-card persona-card" data-id="${p.id}">
                    <button class="draft-header draft-toggle btn-reset" type="button">
                      <strong>${escapeHtml(p.label)}</strong>
                      <span class="draft-meta">${escapeHtml(p.id)}</span>
                    </button>
                    <div class="article-editor is-collapsed" hidden>
                      <p class="persona-summary">Section: ${escapeHtml(sectionTitle(p.section))} | Beat: ${escapeHtml(p.beat)}</p>
                      <div class="persona-editor-grid">
                        <div class="persona-avatar-editor">
                          <div class="persona-avatar-preview">
                            <img src="${escapeHtml(data.avatarUrl || '/images/personas/default-avatar.svg')}" alt="Avatar for ${escapeHtml(p.label)}">
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
                        <label class="workflow-wide">
                          Discovery Feeds (one per line; optional format: URL | Source Name | Priority)
                          <textarea class="field-feeds" rows="4" placeholder="https://example.com/rss | City Hall Agenda | 20">${escapeHtml(feedsText)}</textarea>
                        </label>
                      </div>

                      <div class="persona-quick-actions">
                        <button type="button" class="btn btn-secondary btn-xs btn-apply-recommended-stack">Apply Recommended Model Stack</button>
                        <button type="button" class="btn btn-secondary btn-xs btn-expand-stages">Expand All Stages</button>
                        <button type="button" class="btn btn-secondary btn-xs btn-collapse-stages">Collapse All Stages</button>
                      </div>

                      <div class="workflow-stage-list">
                        ${renderStageEditors(data.stageConfigs)}
                      </div>
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

function getDefaultStageConfig() {
  return {
    runnerType: 'llm',
    provider: '',
    modelOrEndpoint: '',
    enabled: true,
    promptTemplate: '',
    workflowConfig: {}
  };
}

function getStageConfig(stageConfigs, stageName) {
  const fromApi = stageConfigs && typeof stageConfigs === 'object' ? stageConfigs[stageName] : null;
  if (!fromApi || typeof fromApi !== 'object') return getDefaultStageConfig();
  const config = getDefaultStageConfig();
  config.runnerType = String(fromApi.runnerType || config.runnerType);
  config.provider = String(fromApi.provider || '');
  config.modelOrEndpoint = String(fromApi.modelOrEndpoint || '');
  config.enabled = fromApi.enabled !== false;
  config.promptTemplate = String(fromApi.promptTemplate || '');
  config.workflowConfig = fromApi.workflowConfig && typeof fromApi.workflowConfig === 'object'
    ? fromApi.workflowConfig
    : {};
  return config;
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

function renderStageEditors(stageConfigs) {
  return TOPIC_ENGINE_STAGES.map((stageName) => {
    const config = getStageConfig(stageConfigs, stageName);
    const workflowConfigText = JSON.stringify(config.workflowConfig || {}, null, 2);
    const summaryParts = [];
    if (config.runnerType) summaryParts.push(config.runnerType.toUpperCase());
    if (config.provider) summaryParts.push(config.provider);
    if (config.modelOrEndpoint) summaryParts.push(config.modelOrEndpoint);
    const summaryText = summaryParts.length ? summaryParts.join(' • ') : 'No runner configured yet';
    const runnerOptions = RUNNER_TYPE_OPTIONS.map((opt) => `
      <option value="${opt.value}" ${config.runnerType === opt.value ? 'selected' : ''}>${opt.label}</option>
    `).join('');
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
            <label>
              Runner Type
              <select class="field-stage-runner-type">
                ${runnerOptions}
              </select>
            </label>
            <label>
              Provider
              <input type="text" class="field-stage-provider" value="${escapeHtml(config.provider)}" placeholder="OpenAI / Anthropic / Tavily / custom">
            </label>
            <label class="workflow-wide">
              Model or Endpoint
              <input type="text" class="field-stage-model" value="${escapeHtml(config.modelOrEndpoint)}" placeholder="gpt-4o / claude-3-5-sonnet / https://...">
            </label>
            <label class="workflow-wide">
              Prompt Template
              <textarea class="field-stage-prompt" rows="3" placeholder="Instructions for this stage...">${escapeHtml(config.promptTemplate)}</textarea>
            </label>
            <label class="workflow-wide">
              Workflow Config (JSON)
              <textarea class="field-stage-workflow-config" rows="4" placeholder='{"temperature":0.2}'>${escapeHtml(workflowConfigText)}</textarea>
            </label>
          </div>
        </div>
      </section>
    `;
  }).join('');
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

function setStageExpanded(stageEl, expanded) {
  const headerBtn = stageEl?.querySelector('.btn-toggle-stage');
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
  const runnerType = String(stageEl.querySelector('.field-stage-runner-type')?.value || '').trim();
  const provider = String(stageEl.querySelector('.field-stage-provider')?.value || '').trim();
  const modelOrEndpoint = String(stageEl.querySelector('.field-stage-model')?.value || '').trim();
  const parts = [];
  if (runnerType) parts.push(runnerType.toUpperCase());
  if (provider) parts.push(provider);
  if (modelOrEndpoint) parts.push(modelOrEndpoint);
  summaryEl.textContent = parts.length ? parts.join(' • ') : 'No runner configured yet';
}

function setAllStagesExpanded(card, expanded) {
  const stageEls = card.querySelectorAll('.workflow-stage[data-stage]');
  for (const stageEl of stageEls) {
    setStageExpanded(stageEl, expanded);
  }
}

function applyRecommendedStack(card) {
  for (const [stageName, preset] of Object.entries(RECOMMENDED_STAGE_STACK)) {
    const stageEl = card.querySelector(`.workflow-stage[data-stage="${stageName}"]`);
    if (!stageEl) continue;
    const runnerEl = stageEl.querySelector('.field-stage-runner-type');
    const providerEl = stageEl.querySelector('.field-stage-provider');
    const modelEl = stageEl.querySelector('.field-stage-model');
    const enabledEl = stageEl.querySelector('.field-stage-enabled');
    if (runnerEl) runnerEl.value = preset.runnerType;
    if (providerEl) providerEl.value = preset.provider;
    if (modelEl) modelEl.value = preset.modelOrEndpoint;
    if (enabledEl) enabledEl.checked = true;
    updateStageSummary(stageEl);
  }
}

async function loadPersonas() {
  try {
    setMessage('Loading personas...');
    const data = await apiRequest('/api/admin-personas');
    renderPersonas(data.personas || []);
    setMessage(`Loaded ${data.personas?.length || 0} persona configurations.`);
  } catch (err) {
    setMessage(`Failed to load personas: ${err.message}`);
  }
}

async function savePersona(card) {
  const id = card.dataset.id;
  const avatarUrl = card.querySelector('.field-avatar-url').value;
  const disclosure = card.querySelector('.field-disclosure').value;
  const activationMode = card.querySelector('.field-activation-mode')?.value || 'both';
  const isAutoPromoteEnabled = Boolean(card.querySelector('.field-is-auto-promote-enabled')?.checked);
  const feeds = parseFeedsFromText(card.querySelector('.field-feeds')?.value || '');
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
      runnerType: stageEl.querySelector('.field-stage-runner-type')?.value || 'llm',
      provider: stageEl.querySelector('.field-stage-provider')?.value || '',
      modelOrEndpoint: stageEl.querySelector('.field-stage-model')?.value || '',
      enabled: Boolean(stageEl.querySelector('.field-stage-enabled')?.checked),
      promptTemplate: stageEl.querySelector('.field-stage-prompt')?.value || '',
      workflowConfig
    };
  }

  await apiRequest('/api/admin-personas', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id, avatarUrl, disclosure, activationMode, isAutoPromoteEnabled, feeds, stageConfigs })
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

async function loadTopicEngines() {
  const data = await apiRequest('/api/admin-topic-engines');
  topicEngineSettings = Array.isArray(data.topicEngines) ? data.topicEngines : [];
  renderTopicEngineAutonomyGrid();
}

function renderTopicEngineAutonomyGrid() {
  const grid = document.getElementById('persona-autonomy-grid');
  if (!grid) return;
  if (!topicEngineSettings.length) {
    grid.innerHTML = '<p class="signal-meta">No topic engine settings yet.</p>';
    return;
  }
  grid.innerHTML = topicEngineSettings.map((item) => `
    <div class="persona-autonomy-item">
      <div>
        <strong>${escapeHtml(item.personaId || '')}</strong>
        <div class="signal-meta">Auto-promote: ${item.isAutoPromoteEnabled ? 'ON' : 'OFF'}</div>
      </div>
      <button
        type="button"
        class="btn btn-secondary btn-toggle-autonomy"
        data-persona-id="${escapeHtml(item.personaId || '')}"
        data-next-state="${item.isAutoPromoteEnabled ? 'off' : 'on'}"
      >
        Turn ${item.isAutoPromoteEnabled ? 'Off' : 'On'}
      </button>
    </div>
  `).join('');
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
  await apiRequest(`/api/admin/signals/${encodeURIComponent(String(signalId))}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action, reviewNotes: notes })
  });
}

async function setPersonaAutoPromote(personaId, enabled) {
  await apiRequest('/api/admin-topic-engines', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ personaId, isAutoPromoteEnabled: Boolean(enabled) })
  });
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
      await Promise.all([loadManualBudget(), loadGenerationRuns(), loadPersonas(), loadTopicEngines(), loadSignals()]);
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
    throw new Error(data.details ? `${message}: ${data.details}` : message);
  }
  return data;
}

function renderGenerationRuns(runs) {
  if (!Array.isArray(runs) || runs.length === 0) {
    generationRunListEl.innerHTML = '<p>No generation runs found.</p>';
    return;
  }

  const filterValue = String(runFilterInput?.value || 'all');
  const filtered = runs.filter((run) => shouldIncludeRunByFilter(run, filterValue));
  if (!filtered.length) {
    generationRunListEl.innerHTML = '<p>No generation runs match this filter.</p>';
    return;
  }

  generationRunListEl.innerHTML = filtered.map((run) => `
    <article class="draft-card">
      <button class="draft-header draft-toggle btn-reset" type="button">
        <strong>#${run.id} - ${escapeHtml(run.runStatus || 'unknown')}</strong>
        <span class="draft-meta">
          ${escapeHtml(formatDate(run.runAt))} |
          ET: ${escapeHtml(run.etDate || 'n/a')} ${escapeHtml(run.etTime || '')} |
          created: ${Number(run.createdCount || 0).toLocaleString()}/${Number(run.targetCount || 0).toLocaleString()}
        </span>
      </button>
      <p class="draft-meta article-editor is-collapsed" hidden>
        reason: ${escapeHtml(run.runReason || 'none')}
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>
        mode: ${escapeHtml(run.runMode || 'n/a')} |
        provider: ${escapeHtml(run.writerProvider || 'n/a')} |
        model: ${escapeHtml(run.writerModelForRun || 'n/a')} |
        dryRun: ${run.dryRun ? 'yes' : 'no'}
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>
        requested: ${Number(run.requestedCount || 0).toLocaleString()} |
        target: ${Number(run.targetCount || 0).toLocaleString()} |
        created: ${Number(run.createdCount || 0).toLocaleString()} |
        skipped: ${Number(run.skippedCount || 0).toLocaleString()}
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>
        tokens today: ${Number(run.tokensUsedToday || 0).toLocaleString()} /
        ${Number(run.dailyTokenBudget || 0).toLocaleString()} budget |
        consumed this run: ${Number(run.runTokensConsumed || 0).toLocaleString()}
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>
        sections: active=${escapeHtml(run.activeSections || 'n/a')} |
        include=${escapeHtml(run.includeSections || 'none')} |
        exclude=${escapeHtml(run.excludeSections || 'none')}
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>
        top skip reasons: ${formatTopSkipReasons(run.topSkipReasons)}
      </p>
    </article>
  `).join('');
}

async function loadManualBudget() {
  const data = await apiRequest('/api/admin-budget?scope=manual');
  const manualBudget = Number(
    data.dailyTokenBudgetManual ||
    data.dailyTokenBudgetAuto ||
    350000
  );
  if (manualBudgetInput) {
    manualBudgetInput.value = String(manualBudget);
  }
}

async function saveManualBudget() {
  const budget = Number(manualBudgetInput?.value || 0);
  if (!budget || budget < 1) {
    throw new Error('Enter a valid manual budget');
  }
  await apiRequest('/api/admin-budget', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ dailyTokenBudgetManual: budget })
  });
}

async function loadGenerationRuns() {
  const limit = encodeURIComponent(runLimitInput?.value || '50');
  const data = await apiRequest(`/api/admin-generation-runs?limit=${limit}`);
  generationRuns = Array.isArray(data.runs) ? data.runs : [];
  renderGenerationRuns(generationRuns);
  return Number(data.count || 0);
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
  if (!button || !button.classList.contains('draft-toggle')) return;
  const card = button.closest('.draft-card');
  if (!card) return;
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
  if (!card) return;

  if (button.classList.contains('btn-toggle-stage')) {
    const stageEl = button.closest('.workflow-stage');
    const isExpanded = button.getAttribute('aria-expanded') === 'true';
    setStageExpanded(stageEl, !isExpanded);
    return;
  }

  if (button.classList.contains('btn-expand-stages')) {
    setAllStagesExpanded(card, true);
    return;
  }

  if (button.classList.contains('btn-collapse-stages')) {
    setAllStagesExpanded(card, false);
    return;
  }

  if (button.classList.contains('btn-apply-recommended-stack')) {
    applyRecommendedStack(card);
    setMessage(`Recommended model stack applied to '${card.dataset.id}'. Save to persist.`);
    return;
  }

  if (button.classList.contains('btn-upload-avatar')) {
    card.querySelector('.file-image')?.click();
    return;
  }

  if (button.classList.contains('btn-save-persona')) {
    button.disabled = true;
    savePersona(card)
      .then(() => setMessage(`Persona '${card.dataset.id}' saved.`))
      .catch(err => setMessage(`Save failed: ${err.message}`))
      .finally(() => { button.disabled = false; });
  }
}

function onPersonaListChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target instanceof HTMLInputElement && target.type === 'file') {
    const card = target.closest('.persona-card');
    const file = target.files?.[0];
    if (card && file) {
      handlePersonaImageUpload(card, file);
    }
    return;
  }

  const stageEl = target.closest('.workflow-stage');
  if (!stageEl) return;
  if (
    target.classList.contains('field-stage-runner-type') ||
    target.classList.contains('field-stage-provider') ||
    target.classList.contains('field-stage-model')
  ) {
    updateStageSummary(stageEl);
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
    updateSignalAction(signalId, action)
      .then(() => Promise.all([loadSignals(), loadTopicEngines()]))
      .then(() => setMessage(`Signal #${signalId} set to '${action}'.`))
      .catch((err) => setMessage(`Signal update failed: ${err.message}`))
      .finally(() => { button.disabled = false; });
    return;
  }

  if (button.classList.contains('btn-toggle-autonomy')) {
    const personaId = String(button.getAttribute('data-persona-id') || '').trim();
    const nextState = String(button.getAttribute('data-next-state') || '').trim().toLowerCase() === 'on';
    if (!personaId) return;
    button.disabled = true;
    setPersonaAutoPromote(personaId, nextState)
      .then(() => Promise.all([loadTopicEngines(), loadPersonas()]))
      .then(() => setMessage(`Auto-promote ${nextState ? 'enabled' : 'disabled'} for '${personaId}'.`))
      .catch((err) => setMessage(`Autonomy toggle failed: ${err.message}`))
      .finally(() => { button.disabled = false; });
  }
}

function init() {
  if (unlockAdminBtn) unlockAdminBtn.addEventListener('click', unlock);
  if (saveTokenBtn) saveTokenBtn.addEventListener('click', saveToken);
  if (saveManualBudgetBtn) {
    saveManualBudgetBtn.addEventListener('click', () => {
      saveManualBudget()
        .then(loadManualBudget)
        .then(() => setMessage('Manual budget updated.'))
        .catch((err) => setMessage(`Manual budget update failed: ${err.message}`));
    });
  }
  if (loadRunsBtn) {
    loadRunsBtn.addEventListener('click', () => {
      loadGenerationRuns()
        .then((count) => setMessage(`Loaded ${count} generation run(s).`))
        .catch((err) => setMessage(`Load failed: ${err.message}`));
    });
  }
  if (runFilterInput) runFilterInput.addEventListener('change', () => renderGenerationRuns(generationRuns));
  if (appSection) appSection.addEventListener('click', onAppSectionClick);

  // Inject UI and Styles
  injectPersonaStyles();
  ensurePersonaUi();
  ensureSignalsUi();

  // Bind Persona Events
  const loadPersonasBtn = document.getElementById('load-personas-btn');
  const personaListEl = document.getElementById('persona-settings-list');
  if (loadPersonasBtn) loadPersonasBtn.addEventListener('click', loadPersonas);
  if (personaListEl) {
    personaListEl.addEventListener('click', onPersonaListClick);
    personaListEl.addEventListener('change', onPersonaListChange);
  }
  const loadSignalsBtn = document.getElementById('load-signals-btn');
  const applySignalsFiltersBtn = document.getElementById('apply-signals-filters-btn');
  const signalsQueueListEl = document.getElementById('signals-queue-list');
  const signalsPaginationEl = document.getElementById('signals-pagination');
  const personaAutonomyGridEl = document.getElementById('persona-autonomy-grid');
  if (loadSignalsBtn) loadSignalsBtn.addEventListener('click', onSignalsPanelClick);
  if (applySignalsFiltersBtn) applySignalsFiltersBtn.addEventListener('click', onSignalsPanelClick);
  if (signalsQueueListEl) signalsQueueListEl.addEventListener('click', onSignalsPanelClick);
  if (signalsPaginationEl) signalsPaginationEl.addEventListener('click', onSignalsPanelClick);
  if (personaAutonomyGridEl) personaAutonomyGridEl.addEventListener('click', onSignalsPanelClick);

  loadToken();
  setLockState(sessionStorage.getItem('de_admin_unlocked_settings') === '1');
  if (unlocked && getToken()) {
    Promise.all([loadManualBudget(), loadGenerationRuns(), loadPersonas(), loadTopicEngines(), loadSignals()])
      .catch((err) => setMessage(`Load failed: ${err.message}`));
  }
}

init();
