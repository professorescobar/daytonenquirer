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
const loadPersonasBtn = document.getElementById('load-personas-btn');
const personaListEl = document.getElementById('persona-settings-list');
const generationRunListEl = document.getElementById('generation-run-list');
const runFilterInput = document.getElementById('run-filter');

let unlocked = false;
let generationRuns = [];

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
  const allPersonas = new Map();
  for (const beat of Object.keys(PERSONA_OPTIONS_BY_BEAT)) {
    for (const persona of PERSONA_OPTIONS_BY_BEAT[beat]) {
      if (!allPersonas.has(persona.value)) {
        allPersonas.set(persona.value, persona.label);
      }
    }
  }
  return Array.from(allPersonas.entries()).map(([id, label]) => ({ id, label }));
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

function renderPersonas(personas) {
  if (!personaListEl) return;
  const definedPersonas = getAllDefinedPersonas();
  const personaDataMap = new Map(personas.map(p => [p.id, p]));

  personaListEl.innerHTML = definedPersonas.map(p => {
    const data = personaDataMap.get(p.id) || {};
    return `
      <article class="draft-card persona-card" data-id="${p.id}">
        <div class="draft-form">
          <h3>${escapeHtml(p.label)} <small>(${p.id})</small></h3>
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
            </div>
          </div>
          <div class="admin-actions">
            <button type="button" class="btn btn-primary btn-save-persona">Save Persona</button>
          </div>
        </div>
      </article>
    `;
  }).join('');
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

  await apiRequest('/api/admin-personas', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id, avatarUrl, disclosure })
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
      await Promise.all([loadManualBudget(), loadGenerationRuns(), loadPersonas()]);
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

  const card = target.closest('.persona-card');
  if (!card) return;

  if (target.classList.contains('btn-upload-avatar')) {
    card.querySelector('.file-image')?.click();
    return;
  }

  if (target.classList.contains('btn-save-persona')) {
    target.disabled = true;
    savePersona(card)
      .then(() => setMessage(`Persona '${card.dataset.id}' saved.`))
      .catch(err => setMessage(`Save failed: ${err.message}`))
      .finally(() => { target.disabled = false; });
  }
}

function onPersonaListChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLInputElement) || target.type !== 'file') return;

  const card = target.closest('.persona-card');
  const file = target.files?.[0];
  if (card && file) {
    handlePersonaImageUpload(card, file);
  }
}

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
if (loadPersonasBtn) loadPersonasBtn.addEventListener('click', loadPersonas);
if (personaListEl) personaListEl.addEventListener('click', onPersonaListClick);
if (personaListEl) personaListEl.addEventListener('change', onPersonaListChange);

loadToken();
setLockState(sessionStorage.getItem('de_admin_unlocked_settings') === '1');
if (unlocked && getToken()) {
  Promise.all([loadManualBudget(), loadGenerationRuns(), loadPersonas()])
    .catch((err) => setMessage(`Load failed: ${err.message}`));
}
