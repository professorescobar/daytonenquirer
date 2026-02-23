const lockSection = document.getElementById('settings-lock');
const appSection = document.getElementById('settings-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');

const tokenInput = document.getElementById('admin-token');
const duplicateLimitInput = document.getElementById('duplicate-limit');
const rejectedLimitInput = document.getElementById('rejected-limit');
const saveTokenBtn = document.getElementById('save-token-btn');
const loadAllBtn = document.getElementById('load-all-btn');
const messageEl = document.getElementById('settings-message');
const duplicateListEl = document.getElementById('duplicate-list');
const rejectionListEl = document.getElementById('rejection-list');
const usageTokensAutoEl = document.getElementById('usage-tokens-auto');
const usageTokensManualEl = document.getElementById('usage-tokens-manual');
const usageBudgetAutoEl = document.getElementById('usage-budget-auto');
const usageBudgetManualEl = document.getElementById('usage-budget-manual');
const usagePercentAutoEl = document.getElementById('usage-percent-auto');
const usagePercentManualEl = document.getElementById('usage-percent-manual');
const usageDraftsAutoEl = document.getElementById('usage-drafts-auto');
const usageDraftsManualEl = document.getElementById('usage-drafts-manual');
const usageBudgetInputAuto = document.getElementById('usage-budget-input-auto');
const usageBudgetInputManual = document.getElementById('usage-budget-input-manual');
const saveBudgetBtn = document.getElementById('save-budget-btn');
const usageRejectedTotalEl = document.getElementById('usage-rejected-total');
const usageRejectedDuplicateEl = document.getElementById('usage-rejected-duplicate');
const usageRejectedStaleEl = document.getElementById('usage-rejected-stale');
const usageRejectedThinEl = document.getElementById('usage-rejected-thin');
const usageRejectedStyleEl = document.getElementById('usage-rejected-style');
const usageRejectedUserErrorEl = document.getElementById('usage-rejected-user-error');
const usageBadTokensEl = document.getElementById('usage-bad-tokens');

let unlocked = false;

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
    if (getToken()) await loadAll();
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

function renderDuplicateReports(reports) {
  if (!Array.isArray(reports) || reports.length === 0) {
    duplicateListEl.innerHTML = '<p>No duplicate reports found.</p>';
    return;
  }

  duplicateListEl.innerHTML = reports.map((report) => `
    <article class="draft-card" data-report-id="${report.id}">
      <button class="draft-header draft-toggle btn-reset" type="button">
        <strong>#${report.id} - ${escapeHtml(report.draftTitle || '')}</strong>
        <span class="draft-meta">
          section: ${escapeHtml(report.section || 'n/a')} |
          type: ${escapeHtml(report.duplicateType || 'internal')} |
          reported: ${escapeHtml(formatDate(report.reportedAt))}
        </span>
      </button>
      <p class="draft-meta article-editor is-collapsed" hidden>
        source title: ${escapeHtml(report.sourceTitle || 'N/A')}
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>source url:
        <a href="${escapeHtml(report.sourceUrl || '#')}" target="_blank" rel="noopener noreferrer">${escapeHtml(report.sourceUrl || 'N/A')}</a>
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>notes: ${escapeHtml(report.notes || 'none')}</p>
      <div class="draft-actions article-editor is-collapsed" hidden>
        <button class="btn btn-danger btn-remove-duplicate-report">Remove From Duplicate List</button>
      </div>
    </article>
  `).join('');
}

function renderRejections(rejections) {
  if (!Array.isArray(rejections) || rejections.length === 0) {
    rejectionListEl.innerHTML = '<p>No rejected drafts found.</p>';
    return;
  }

  rejectionListEl.innerHTML = rejections.map((item) => `
    <article class="draft-card" data-rejection-id="${item.id}">
      <button class="draft-header draft-toggle btn-reset" type="button">
        <strong>#${item.id} - ${escapeHtml(item.draftTitle || '')}</strong>
        <span class="draft-meta">
          reason: ${escapeHtml(item.rejectReason || 'n/a')} |
          section: ${escapeHtml(item.section || 'n/a')} |
          rejected: ${escapeHtml(formatDate(item.rejectedAt))}
        </span>
      </button>
      <p class="draft-meta article-editor is-collapsed" hidden>
        tokens: ${Number(item.totalTokens || 0).toLocaleString()}
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>source:
        <a href="${escapeHtml(item.sourceUrl || '#')}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.sourceUrl || 'N/A')}</a>
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>notes: ${escapeHtml(item.notes || 'none')}</p>
      <div class="draft-actions article-editor is-collapsed" hidden>
        <button class="btn btn-danger btn-delete-rejection">Permanently Delete Record</button>
      </div>
    </article>
  `).join('');
}

async function loadDuplicateReports() {
  const limit = encodeURIComponent(duplicateLimitInput?.value || '50');
  const data = await apiRequest(`/api/admin-duplicate-reports?limit=${limit}`);
  renderDuplicateReports(data.reports || []);
  return Number(data.count || 0);
}

async function loadRejections() {
  const limit = encodeURIComponent(rejectedLimitInput?.value || '50');
  const data = await apiRequest(`/api/admin-rejections?limit=${limit}`);
  renderRejections(data.rejections || []);
  return Number(data.count || 0);
}

async function loadAll() {
  try {
    setMessage('Loading settings lists...');
    const [dupCount, rejCount] = await Promise.all([
      loadDuplicateReports(),
      loadRejections(),
      loadUsageDashboard()
    ]);
    setMessage(`Loaded ${dupCount} duplicate report(s) and ${rejCount} rejection record(s).`);
  } catch (err) {
    setMessage(`Load failed: ${err.message}`);
  }
}

async function loadUsageDashboard() {
  const [usageAuto, usageManual, quality] = await Promise.all([
    apiRequest('/api/admin-usage?scope=auto'),
    apiRequest('/api/admin-usage?scope=manual'),
    apiRequest('/api/admin-quality-metrics')
  ]);

  if (!usageTokensAutoEl) return;
  usageTokensAutoEl.textContent = Number(usageAuto.tokensUsedToday || 0).toLocaleString();
  usageTokensManualEl.textContent = Number(usageManual.tokensUsedToday || 0).toLocaleString();
  usageBudgetAutoEl.textContent = Number(usageAuto.dailyTokenBudget || 0).toLocaleString();
  usageBudgetManualEl.textContent = Number(usageManual.dailyTokenBudget || 0).toLocaleString();
  usagePercentAutoEl.textContent = `${usageAuto.budgetUsedPercent || 0}%`;
  usagePercentManualEl.textContent = `${usageManual.budgetUsedPercent || 0}%`;
  usageDraftsAutoEl.textContent = Number(usageAuto.draftsToday || 0).toLocaleString();
  usageDraftsManualEl.textContent = Number(usageManual.draftsToday || 0).toLocaleString();
  usageBudgetInputAuto.value = Number(usageAuto.dailyTokenBudget || 0);
  usageBudgetInputManual.value = Number(usageManual.dailyTokenBudget || 0);

  const byReason = quality.byReason || {};
  usageRejectedTotalEl.textContent = Number(quality.totalRejected || 0).toLocaleString();
  usageRejectedDuplicateEl.textContent = Number(byReason.duplicate || 0).toLocaleString();
  usageRejectedStaleEl.textContent = Number(byReason.stale_or_not_time_relevant || 0).toLocaleString();
  usageRejectedThinEl.textContent = Number(byReason.low_newsworthiness_or_thin || 0).toLocaleString();
  usageRejectedStyleEl.textContent = Number(byReason.style_mismatch || 0).toLocaleString();
  usageRejectedUserErrorEl.textContent = Number(byReason.user_error || 0).toLocaleString();
  usageBadTokensEl.textContent = Number(quality.badTokensTotal || 0).toLocaleString();
}

async function saveBudget() {
  try {
    const dailyTokenBudgetAuto = Number(usageBudgetInputAuto.value || 0);
    const dailyTokenBudgetManual = Number(usageBudgetInputManual.value || 0);
    if (!dailyTokenBudgetAuto || dailyTokenBudgetAuto < 1) {
      throw new Error('Enter a valid auto token budget');
    }
    if (!dailyTokenBudgetManual || dailyTokenBudgetManual < 1) {
      throw new Error('Enter a valid manual token budget');
    }
    const data = await apiRequest('/api/admin-budget', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dailyTokenBudgetAuto, dailyTokenBudgetManual })
    });
    setMessage(
      `Budgets updated. Auto: ${Number(data.dailyTokenBudgetAuto).toLocaleString()} | Manual: ${Number(data.dailyTokenBudgetManual).toLocaleString()}.`
    );
    await loadUsageDashboard();
  } catch (err) {
    setMessage(`Budget update failed: ${err.message}`);
  }
}

async function removeDuplicateReport(id) {
  await apiRequest('/api/admin-remove-duplicate-report', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id })
  });
}

async function deleteRejection(id) {
  await apiRequest('/api/admin-delete-rejection', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id })
  });
}

function onListClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  const card = target.closest('.draft-card');
  if (!card) return;
  const button = target.closest('button');
  if (!button) return;

  if (button.classList.contains('draft-toggle')) {
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
    return;
  }

  if (button.classList.contains('btn-remove-duplicate-report')) {
    const reportId = Number(card.dataset.reportId || 0);
    if (!reportId) return;
    const ok = window.confirm(`Remove duplicate report #${reportId}?`);
    if (!ok) return;
    removeDuplicateReport(reportId)
      .then(loadAll)
      .then(() => setMessage(`Duplicate report #${reportId} removed.`))
      .catch((err) => setMessage(`Remove duplicate report failed: ${err.message}`));
    return;
  }

  if (button.classList.contains('btn-delete-rejection')) {
    const rejectionId = Number(card.dataset.rejectionId || 0);
    if (!rejectionId) return;
    const ok = window.confirm(`Permanently delete rejection record #${rejectionId}?`);
    if (!ok) return;
    deleteRejection(rejectionId)
      .then(loadAll)
      .then(() => setMessage(`Rejection record #${rejectionId} deleted.`))
      .catch((err) => setMessage(`Delete rejection failed: ${err.message}`));
  }
}

function saveToken() {
  localStorage.setItem('de_admin_token', getToken());
  setMessage('Token saved.');
}

function loadToken() {
  const token = localStorage.getItem('de_admin_token') || '';
  if (token) tokenInput.value = token;
}

if (unlockAdminBtn) unlockAdminBtn.addEventListener('click', unlock);
if (saveTokenBtn) saveTokenBtn.addEventListener('click', saveToken);
if (loadAllBtn) loadAllBtn.addEventListener('click', loadAll);
if (saveBudgetBtn) saveBudgetBtn.addEventListener('click', saveBudget);
if (appSection) appSection.addEventListener('click', onListClick);

loadToken();
setLockState(sessionStorage.getItem('de_admin_unlocked_settings') === '1');
if (unlocked && getToken()) {
  loadAll().catch((err) => setMessage(`Load failed: ${err.message}`));
}
