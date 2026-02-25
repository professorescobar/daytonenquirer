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

function setMessage(text) {
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
      await Promise.all([loadManualBudget(), loadGenerationRuns()]);
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

loadToken();
setLockState(sessionStorage.getItem('de_admin_unlocked_settings') === '1');
if (unlocked && getToken()) {
  Promise.all([loadManualBudget(), loadGenerationRuns()])
    .catch((err) => setMessage(`Load failed: ${err.message}`));
}
