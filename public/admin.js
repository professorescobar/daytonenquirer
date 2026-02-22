const tokenInput = document.getElementById('admin-token');
const lockSection = document.getElementById('admin-lock');
const appSection = document.getElementById('admin-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');
const statusFilterInput = document.getElementById('status-filter');
const limitInput = document.getElementById('list-limit');
const genCountInput = document.getElementById('gen-count');
const genIncludeInput = document.getElementById('gen-include');
const genExcludeInput = document.getElementById('gen-exclude');
const manualTitleInput = document.getElementById('manual-title');
const manualSectionInput = document.getElementById('manual-section');
const loadDraftsBtn = document.getElementById('load-drafts-btn');
const generateBtn = document.getElementById('generate-btn');
const createDraftBtn = document.getElementById('create-draft-btn');
const saveTokenBtn = document.getElementById('save-token-btn');
const messageEl = document.getElementById('admin-message');
const draftListEl = document.getElementById('draft-list');
const usageTokensEl = document.getElementById('usage-tokens');
const usageBudgetEl = document.getElementById('usage-budget');
const usagePercentEl = document.getElementById('usage-percent');
const usageDraftsEl = document.getElementById('usage-drafts');
const usageBudgetInput = document.getElementById('usage-budget-input');
const saveBudgetBtn = document.getElementById('save-budget-btn');

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

let adminUiUnlocked = false;

function getToken() {
  return (tokenInput.value || '').trim();
}

function setMessage(text) {
  messageEl.hidden = !text;
  messageEl.textContent = text || '';
}

async function apiRequest(url, options = {}) {
  if (!adminUiUnlocked) {
    throw new Error('Admin UI is locked');
  }

  const token = getToken();
  if (!token) {
    throw new Error('Missing admin token');
  }

  const headers = {
    'x-admin-token': token,
    ...(options.headers || {})
  };

  const res = await fetch(url, { ...options, headers });
  const data = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(data.error || data.details || `Request failed (${res.status})`);
  }

  return data;
}

function setLockState(unlocked) {
  adminUiUnlocked = unlocked;
  if (lockSection) lockSection.hidden = unlocked;
  if (appSection) appSection.hidden = !unlocked;
}

async function unlockAdminUi() {
  try {
    setMessage('');
    const password = (adminUiPasswordInput.value || '').trim();
    if (!password) {
      setMessage('Enter the admin UI password.');
      return;
    }

    const res = await fetch('/api/admin-ui-auth', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.error || 'Unlock failed');
    }

    sessionStorage.setItem('de_admin_unlocked', '1');
    setLockState(true);
    setMessage('Admin unlocked.');
  } catch (err) {
    setMessage(`Unlock failed: ${err.message}`);
  }
}

function formatDate(dateString) {
  if (!dateString) return '';
  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) return dateString;
  return date.toLocaleString();
}

function escapeHtml(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function sectionSelectHtml(selected) {
  return SECTION_OPTIONS.map((value) => {
    const isSelected = value === selected ? 'selected' : '';
    return `<option value="${value}" ${isSelected}>${value}</option>`;
  }).join('');
}

function renderDrafts(drafts) {
  if (!Array.isArray(drafts) || drafts.length === 0) {
    draftListEl.innerHTML = '<p>No drafts found for this filter.</p>';
    return;
  }

  draftListEl.innerHTML = drafts.map((draft) => `
    <article class="draft-card" data-id="${draft.id}">
      <div class="draft-header">
        <strong>#${draft.id} - ${escapeHtml(draft.title || '')}</strong>
        <span class="draft-meta">status: ${escapeHtml(draft.status || '')}</span>
      </div>
      <p class="draft-meta">
        section: ${escapeHtml(draft.section || '')} |
        slug: ${escapeHtml(draft.slug || '')} |
        via: ${escapeHtml(draft.createdVia || 'unknown')} |
        created: ${escapeHtml(formatDate(draft.createdAt))}
      </p>
      <p class="draft-meta">source: <a href="${escapeHtml(draft.sourceUrl || '#')}" target="_blank" rel="noopener noreferrer">${escapeHtml(draft.sourceUrl || 'N/A')}</a></p>

      <div class="draft-form">
        <label class="full">
          Title
          <input class="field-title" type="text" value="${escapeHtml(draft.title || '')}" />
        </label>
        <label class="full">
          Description
          <textarea class="field-description">${escapeHtml(draft.description || '')}</textarea>
        </label>
        <label class="full">
          Content
          <textarea class="field-content">${escapeHtml(draft.content || '')}</textarea>
        </label>
        <label class="full">
          Image URL
          <input class="field-image" type="text" value="${escapeHtml(draft.image || '')}" />
        </label>
        <label class="full">
          Image Description / Caption
          <textarea class="field-image-caption">${escapeHtml(draft.imageCaption || '')}</textarea>
        </label>
        <label class="full">
          Image Source / Credit
          <input class="field-image-credit" type="text" value="${escapeHtml(draft.imageCredit || '')}" />
        </label>
        <label>
          Section
          <select class="field-section">
            ${sectionSelectHtml(draft.section)}
          </select>
        </label>
        <label>
          Status
          <select class="field-status">
            <option value="pending_review" ${draft.status === 'pending_review' ? 'selected' : ''}>pending_review</option>
            <option value="published" ${draft.status === 'published' ? 'selected' : ''}>published</option>
          </select>
        </label>
      </div>

      <div class="draft-actions">
        <button class="btn btn-save">Save Draft</button>
        <button class="btn btn-primary btn-publish">Publish Draft</button>
      </div>
    </article>
  `).join('');
}

async function loadDrafts() {
  try {
    setMessage('Loading drafts...');
    const status = encodeURIComponent(statusFilterInput.value || 'pending_review');
    const limit = encodeURIComponent(limitInput.value || '25');
    const data = await apiRequest(`/api/admin-drafts?status=${status}&limit=${limit}`);
    renderDrafts(data.drafts || []);
    await loadUsageDashboard();
    setMessage(`Loaded ${data.count || 0} draft(s).`);
  } catch (err) {
    setMessage(`Load failed: ${err.message}`);
  }
}

async function generateDrafts() {
  try {
    setMessage('Generating drafts...');
    const count = encodeURIComponent(genCountInput.value || '3');
    const includeSections = encodeURIComponent((genIncludeInput.value || '').trim());
    const excludeSections = encodeURIComponent((genExcludeInput.value || '').trim());
    const url = `/api/admin-generate-drafts?count=${count}&includeSections=${includeSections}&excludeSections=${excludeSections}`;
    const data = await apiRequest(url, { method: 'POST' });
    setMessage(`Generated ${data.createdCount || 0} draft(s), skipped ${data.skippedCount || 0}.`);
    await loadUsageDashboard();
    await loadDrafts();
  } catch (err) {
    setMessage(`Generate failed: ${err.message}`);
  }
}

async function createManualDraft() {
  try {
    const title = (manualTitleInput.value || '').trim();
    const section = (manualSectionInput.value || 'local').trim();
    const data = await apiRequest('/api/admin-create-draft', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title, section })
    });
    setMessage(`Created blank draft #${data.draft?.id || ''}.`);
    manualTitleInput.value = '';
    await loadDrafts();
  } catch (err) {
    setMessage(`Create draft failed: ${err.message}`);
  }
}

async function loadUsageDashboard() {
  const data = await apiRequest('/api/admin-usage');
  usageTokensEl.textContent = Number(data.tokensUsedToday || 0).toLocaleString();
  usageBudgetEl.textContent = Number(data.dailyTokenBudget || 0).toLocaleString();
  usagePercentEl.textContent = `${data.budgetPercent || 0}%`;
  usageDraftsEl.textContent = Number(data.draftsToday || 0).toLocaleString();
  usageBudgetInput.value = Number(data.dailyTokenBudget || 0);
}

async function saveBudget() {
  try {
    const dailyTokenBudget = Number(usageBudgetInput.value || 0);
    if (!dailyTokenBudget || dailyTokenBudget < 1) {
      throw new Error('Enter a valid token budget');
    }
    const data = await apiRequest('/api/admin-budget', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dailyTokenBudget })
    });
    setMessage(`Daily token budget updated to ${Number(data.dailyTokenBudget).toLocaleString()}.`);
    await loadUsageDashboard();
  } catch (err) {
    setMessage(`Budget update failed: ${err.message}`);
  }
}

async function saveDraft(card) {
  const id = Number(card.dataset.id);
  const body = {
    id,
    title: card.querySelector('.field-title').value,
    description: card.querySelector('.field-description').value,
    content: card.querySelector('.field-content').value,
    image: card.querySelector('.field-image').value,
    imageCaption: card.querySelector('.field-image-caption').value,
    imageCredit: card.querySelector('.field-image-credit').value,
    section: card.querySelector('.field-section').value,
    status: card.querySelector('.field-status').value
  };

  await apiRequest('/api/admin-update-draft', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
}

async function publishDraft(card) {
  const id = Number(card.dataset.id);
  await apiRequest('/api/admin-publish-draft', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id })
  });
}

function onDraftListClick(event) {
  const button = event.target.closest('button');
  if (!button) return;

  const card = event.target.closest('.draft-card');
  if (!card) return;

  if (button.classList.contains('btn-save')) {
    saveDraft(card)
      .then(() => setMessage(`Draft #${card.dataset.id} saved.`))
      .catch((err) => setMessage(`Save failed: ${err.message}`));
  }

  if (button.classList.contains('btn-publish')) {
    publishDraft(card)
      .then(async () => {
        setMessage(`Draft #${card.dataset.id} published.`);
        await loadDrafts();
      })
      .catch((err) => setMessage(`Publish failed: ${err.message}`));
  }
}

function loadStoredToken() {
  const saved = localStorage.getItem('de_admin_token') || '';
  if (saved) tokenInput.value = saved;
}

function saveToken() {
  localStorage.setItem('de_admin_token', getToken());
  setMessage('Token saved in this browser.');
}

saveTokenBtn.addEventListener('click', saveToken);
loadDraftsBtn.addEventListener('click', loadDrafts);
generateBtn.addEventListener('click', generateDrafts);
createDraftBtn.addEventListener('click', createManualDraft);
draftListEl.addEventListener('click', onDraftListClick);
unlockAdminBtn.addEventListener('click', unlockAdminUi);
saveBudgetBtn.addEventListener('click', saveBudget);

loadStoredToken();
setLockState(sessionStorage.getItem('de_admin_unlocked') === '1');
if (adminUiUnlocked) {
  loadUsageDashboard().catch((err) => setMessage(`Usage load failed: ${err.message}`));
}
