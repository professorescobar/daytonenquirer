const lockSection = document.getElementById('settings-lock');
const appSection = document.getElementById('settings-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');

const tokenInput = document.getElementById('admin-token');
const duplicateLimitInput = document.getElementById('duplicate-limit');
const rejectedLimitInput = document.getElementById('rejected-limit');
const runLimitInput = document.getElementById('run-limit');
const saveTokenBtn = document.getElementById('save-token-btn');
const loadAllBtn = document.getElementById('load-all-btn');
const messageEl = document.getElementById('settings-message');
const duplicateListEl = document.getElementById('duplicate-list');
const rejectionListEl = document.getElementById('rejection-list');
const generationRunListEl = document.getElementById('generation-run-list');
const runFilterInput = document.getElementById('run-filter');
const usageAutoDailyUsedEl = document.getElementById('usage-auto-daily-used');
const usageAutoDailyBudgetEl = document.getElementById('usage-auto-daily-budget');
const usageAutoDailyRemainingEl = document.getElementById('usage-auto-daily-remaining');
const usageAutoDailyBarLabelEl = document.getElementById('usage-auto-daily-bar-label');
const usageAutoWeeklyBarLabelEl = document.getElementById('usage-auto-weekly-bar-label');
const usageAutoMonthlyBarLabelEl = document.getElementById('usage-auto-monthly-bar-label');
const usageAutoProgressDailyEl = document.getElementById('usage-auto-progress-daily');
const usageAutoProgressWeeklyEl = document.getElementById('usage-auto-progress-weekly');
const usageAutoProgressMonthlyEl = document.getElementById('usage-auto-progress-monthly');
const usageAutoWeeklyUsedEl = document.getElementById('usage-auto-weekly-used');
const usageAutoMonthlyUsedEl = document.getElementById('usage-auto-monthly-used');
const usageManualDailyUsedEl = document.getElementById('usage-manual-daily-used');
const usageManualDailyBudgetEl = document.getElementById('usage-manual-daily-budget');
const usageManualDailyRemainingEl = document.getElementById('usage-manual-daily-remaining');
const usageManualDailyBarLabelEl = document.getElementById('usage-manual-daily-bar-label');
const usageManualWeeklyBarLabelEl = document.getElementById('usage-manual-weekly-bar-label');
const usageManualMonthlyBarLabelEl = document.getElementById('usage-manual-monthly-bar-label');
const usageManualProgressDailyEl = document.getElementById('usage-manual-progress-daily');
const usageManualProgressWeeklyEl = document.getElementById('usage-manual-progress-weekly');
const usageManualProgressMonthlyEl = document.getElementById('usage-manual-progress-monthly');
const usageManualWeeklyUsedEl = document.getElementById('usage-manual-weekly-used');
const usageManualMonthlyUsedEl = document.getElementById('usage-manual-monthly-used');
const usageBudgetInputAuto = document.getElementById('usage-budget-input-auto');
const usageBudgetInputManual = document.getElementById('usage-budget-input-manual');
const saveBudgetBtn = document.getElementById('save-budget-btn');
const usageRejectedDailyEl = document.getElementById('usage-rejected-daily');
const usageRejectedMonthlyEl = document.getElementById('usage-rejected-monthly');
const usageRejectedAnnualEl = document.getElementById('usage-rejected-annual');
const usageRejectedTotalEl = document.getElementById('usage-rejected-total');
const usageBadTokensDailyEl = document.getElementById('usage-bad-tokens-daily');
const usageBadTokensMonthlyEl = document.getElementById('usage-bad-tokens-monthly');
const usageBadTokensAnnualEl = document.getElementById('usage-bad-tokens-annual');
const usageBadTokensTotalEl = document.getElementById('usage-bad-tokens-total');
const usageRejectedDuplicateDailyEl = document.getElementById('usage-rejected-duplicate-daily');
const usageRejectedStaleDailyEl = document.getElementById('usage-rejected-stale-daily');
const usageRejectedThinDailyEl = document.getElementById('usage-rejected-thin-daily');
const usageRejectedStyleDailyEl = document.getElementById('usage-rejected-style-daily');
const usageRejectedUserErrorDailyEl = document.getElementById('usage-rejected-user-error-daily');
const qualityBreakdownToggleBtn = document.getElementById('quality-breakdown-toggle');
const qualityBreakdownEl = document.getElementById('quality-breakdown');

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

function formatUsageLabel(percentUsed, draftCount, periodLabel) {
  const pct = Number(percentUsed || 0).toFixed(1);
  const drafts = Number(draftCount || 0).toLocaleString();
  return `${pct}% (${drafts} drafts ${periodLabel})`;
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
    <article class="draft-card" data-generation-run-id="${run.id}">
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
        schedule: ${escapeHtml(run.scheduleMode || 'n/a')} |
        track: ${escapeHtml(run.track || 'n/a')} |
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

async function loadGenerationRuns() {
  const limit = encodeURIComponent(runLimitInput?.value || '50');
  const data = await apiRequest(`/api/admin-generation-runs?limit=${limit}`);
  generationRuns = Array.isArray(data.runs) ? data.runs : [];
  renderGenerationRuns(generationRuns);
  return Number(data.count || 0);
}

async function loadAll() {
  try {
    setMessage('Loading settings lists...');
    const [dupCount, rejCount, runCount] = await Promise.all([
      loadDuplicateReports(),
      loadRejections(),
      loadGenerationRuns(),
      loadUsageDashboard()
    ]);
    setMessage(
      `Loaded ${dupCount} duplicate report(s), ${rejCount} rejection record(s), and ${runCount} generation run(s).`
    );
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

  if (!usageAutoDailyUsedEl) return;
  usageAutoDailyUsedEl.textContent = Number(usageAuto.dailyTokensUsed || 0).toLocaleString();
  usageAutoDailyBudgetEl.textContent = Number(usageAuto.dailyTokenBudget || 0).toLocaleString();
  usageAutoDailyRemainingEl.textContent = Number(usageAuto.tokensRemainingToday || 0).toLocaleString();
  usageAutoDailyBarLabelEl.textContent = formatUsageLabel(usageAuto.budgetUsedPercent, usageAuto.dailyDrafts, 'today');
  usageAutoWeeklyBarLabelEl.textContent = formatUsageLabel(usageAuto.weeklyBudgetUsedPercent, usageAuto.weeklyDrafts, 'this week');
  usageAutoMonthlyBarLabelEl.textContent = formatUsageLabel(usageAuto.monthlyBudgetUsedPercent, usageAuto.monthlyDrafts, 'this month');
  usageAutoWeeklyUsedEl.textContent = Number(usageAuto.weeklyTokensUsed || 0).toLocaleString();
  usageAutoMonthlyUsedEl.textContent = Number(usageAuto.monthlyTokensUsed || 0).toLocaleString();
  paintUsageBar(usageAutoProgressDailyEl, Number(usageAuto.budgetUsedPercent || 0));
  paintUsageBar(usageAutoProgressWeeklyEl, Number(usageAuto.weeklyBudgetUsedPercent || 0));
  paintUsageBar(usageAutoProgressMonthlyEl, Number(usageAuto.monthlyBudgetUsedPercent || 0));

  usageManualDailyUsedEl.textContent = Number(usageManual.dailyTokensUsed || 0).toLocaleString();
  usageManualDailyBudgetEl.textContent = Number(usageManual.dailyTokenBudget || 0).toLocaleString();
  usageManualDailyRemainingEl.textContent = Number(usageManual.tokensRemainingToday || 0).toLocaleString();
  usageManualDailyBarLabelEl.textContent = formatUsageLabel(usageManual.budgetUsedPercent, usageManual.dailyDrafts, 'today');
  usageManualWeeklyBarLabelEl.textContent = formatUsageLabel(usageManual.weeklyBudgetUsedPercent, usageManual.weeklyDrafts, 'this week');
  usageManualMonthlyBarLabelEl.textContent = formatUsageLabel(usageManual.monthlyBudgetUsedPercent, usageManual.monthlyDrafts, 'this month');
  usageManualWeeklyUsedEl.textContent = Number(usageManual.weeklyTokensUsed || 0).toLocaleString();
  usageManualMonthlyUsedEl.textContent = Number(usageManual.monthlyTokensUsed || 0).toLocaleString();
  paintUsageBar(usageManualProgressDailyEl, Number(usageManual.budgetUsedPercent || 0));
  paintUsageBar(usageManualProgressWeeklyEl, Number(usageManual.weeklyBudgetUsedPercent || 0));
  paintUsageBar(usageManualProgressMonthlyEl, Number(usageManual.monthlyBudgetUsedPercent || 0));

  usageBudgetInputAuto.value = Number(usageAuto.dailyTokenBudget || 0);
  usageBudgetInputManual.value = Number(usageManual.dailyTokenBudget || 0);

  const daily = quality.daily || {};
  const monthly = quality.monthly || {};
  const annual = quality.annual || {};
  const total = quality.total || {};
  const dailyByReason = daily.byReason || {};
  usageRejectedDailyEl.textContent = Number(daily.totalRejected || 0).toLocaleString();
  usageRejectedMonthlyEl.textContent = Number(monthly.totalRejected || 0).toLocaleString();
  usageRejectedAnnualEl.textContent = Number(annual.totalRejected || 0).toLocaleString();
  usageRejectedTotalEl.textContent = Number(total.totalRejected || 0).toLocaleString();
  usageBadTokensDailyEl.textContent = Number(daily.badTokensTotal || 0).toLocaleString();
  usageBadTokensMonthlyEl.textContent = Number(monthly.badTokensTotal || 0).toLocaleString();
  usageBadTokensAnnualEl.textContent = Number(annual.badTokensTotal || 0).toLocaleString();
  usageBadTokensTotalEl.textContent = Number(total.badTokensTotal || 0).toLocaleString();
  usageRejectedDuplicateDailyEl.textContent = Number(dailyByReason.duplicate || 0).toLocaleString();
  usageRejectedStaleDailyEl.textContent = Number(dailyByReason.stale_or_not_time_relevant || 0).toLocaleString();
  usageRejectedThinDailyEl.textContent = Number(dailyByReason.low_newsworthiness_or_thin || 0).toLocaleString();
  usageRejectedStyleDailyEl.textContent = Number(dailyByReason.style_mismatch || 0).toLocaleString();
  usageRejectedUserErrorDailyEl.textContent = Number(dailyByReason.user_error || 0).toLocaleString();
}

function paintUsageBar(barEl, usedPercent) {
  if (!barEl) return;
  const clamped = Math.max(0, Math.min(100, Number(usedPercent || 0)));
  barEl.style.width = `${clamped}%`;
  barEl.classList.remove('usage-progress-good', 'usage-progress-warn', 'usage-progress-danger');
  if (clamped >= 85) {
    barEl.classList.add('usage-progress-danger');
  } else if (clamped >= 60) {
    barEl.classList.add('usage-progress-warn');
  } else {
    barEl.classList.add('usage-progress-good');
  }
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
if (qualityBreakdownToggleBtn && qualityBreakdownEl) {
  qualityBreakdownToggleBtn.addEventListener('click', () => {
    const nextHidden = !qualityBreakdownEl.hasAttribute('hidden');
    if (nextHidden) {
      qualityBreakdownEl.setAttribute('hidden', '');
      qualityBreakdownToggleBtn.textContent = 'Show Breakdown';
    } else {
      qualityBreakdownEl.removeAttribute('hidden');
      qualityBreakdownToggleBtn.textContent = 'Hide Breakdown';
    }
  });
}
if (appSection) appSection.addEventListener('click', onListClick);
if (runFilterInput) {
  runFilterInput.addEventListener('change', () => renderGenerationRuns(generationRuns));
}

loadToken();
setLockState(sessionStorage.getItem('de_admin_unlocked_settings') === '1');
if (unlocked && getToken()) {
  loadAll().catch((err) => setMessage(`Load failed: ${err.message}`));
}
