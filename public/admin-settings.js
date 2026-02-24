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
const globalDailyLabelEl = document.getElementById('global-daily-label');
const globalWeeklyLabelEl = document.getElementById('global-weekly-label');
const globalMonthlyLabelEl = document.getElementById('global-monthly-label');
const globalThroughputLabelEl = document.getElementById('global-throughput-label');
const globalAcceptanceLabelEl = document.getElementById('global-acceptance-label');
const globalQualityLossLabelEl = document.getElementById('global-quality-loss-label');
const globalProgressDailyEl = document.getElementById('global-progress-daily');
const globalProgressWeeklyEl = document.getElementById('global-progress-weekly');
const globalProgressMonthlyEl = document.getElementById('global-progress-monthly');
const globalProgressThroughputEl = document.getElementById('global-progress-throughput');
const globalProgressAcceptanceEl = document.getElementById('global-progress-acceptance');
const globalProgressQualityLossEl = document.getElementById('global-progress-quality-loss');
const globalTokensTodayEl = document.getElementById('global-tokens-today');
const globalDraftsTodayEl = document.getElementById('global-drafts-today');
const globalQualityLossTodayValueEl = document.getElementById('global-quality-loss-today-value');
const modelMetricsListEl = document.getElementById('model-metrics-list');
const qualityRejectedTodayEl = document.getElementById('quality-rejected-today');
const qualityRejectedMonthEl = document.getElementById('quality-rejected-month');
const qualityBadTokensTodayEl = document.getElementById('quality-bad-tokens-today');
const qualityBadTokensMonthEl = document.getElementById('quality-bad-tokens-month');
const qualityDuplicateTodayEl = document.getElementById('quality-duplicate-today');
const qualityStaleTodayEl = document.getElementById('quality-stale-today');
const qualityThinTodayEl = document.getElementById('quality-thin-today');
const qualityStyleTodayEl = document.getElementById('quality-style-today');
const budgetTotalDailyEl = document.getElementById('budget-total-daily');
const budgetTotalUsedEl = document.getElementById('budget-total-used');
const budgetTotalRemainingEl = document.getElementById('budget-total-remaining');
const budgetDraftTargetEl = document.getElementById('budget-draft-target');

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

function formatModelLabel(model) {
  const raw = String(model || '').trim();
  const lower = raw.toLowerCase();
  if (!raw) return 'Unknown';
  if (lower.includes('gpt')) return `ChatGPT (${raw})`;
  if (lower.includes('claude')) return `Claude (${raw})`;
  if (lower.includes('gemini')) return `Gemini (${raw})`;
  if (lower.includes('grok')) return `Grok (${raw})`;
  return raw;
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
          model: ${escapeHtml(report.model || 'unknown')} |
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
          model: ${escapeHtml(item.model || 'unknown')} |
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

function renderModelMetrics(items, usageByModel) {
  if (!modelMetricsListEl) return;
  if (!Array.isArray(items) || !items.length) {
    modelMetricsListEl.innerHTML = '<p class="draft-meta">No model metrics yet.</p>';
    return;
  }

  modelMetricsListEl.innerHTML = items.map((item) => {
    const model = String(item.model || 'unknown');
    const modelLabel = formatModelLabel(model);
    const modelUsage = usageByModel[model] || {};
    const byReason = item.byReason || {};
    const budgetValue = Number(modelUsage.dailyTokenBudget || 0);
    return `
      <article class="draft-card" data-model-name="${escapeHtml(model)}">
        <button class="draft-header draft-toggle btn-reset" type="button">
          <strong class="model-title">${escapeHtml(modelLabel)}</strong>
          <span class="draft-meta">
            daily used: ${Number(modelUsage.dailyTokensUsed || 0).toLocaleString()} / ${budgetValue.toLocaleString()} |
            loss: ${Number(modelUsage.qualityLossRatePercent || 0).toFixed(1)}%
          </span>
        </button>
        <div class="section-editor article-editor is-collapsed" hidden>
          <p class="usage-row"><span>Daily Usage</span><strong>${Number(modelUsage.budgetUsedPercent || 0).toFixed(1)}%</strong></p>
          <div class="usage-progress"><div class="usage-progress-bar usage-progress-model-daily"></div></div>
          <p class="usage-row"><span>Weekly Usage</span><strong>${Number(modelUsage.weeklyBudgetUsedPercent || 0).toFixed(1)}%</strong></p>
          <div class="usage-progress"><div class="usage-progress-bar usage-progress-model-weekly"></div></div>
          <p class="usage-row"><span>Monthly Usage</span><strong>${Number(modelUsage.monthlyBudgetUsedPercent || 0).toFixed(1)}%</strong></p>
          <div class="usage-progress"><div class="usage-progress-bar usage-progress-model-monthly"></div></div>
          <p class="usage-row"><span>Acceptance Rate (Today)</span><strong>${Number(modelUsage.acceptanceRatePercent || 0).toFixed(1)}%</strong></p>
          <div class="usage-progress"><div class="usage-progress-bar usage-progress-model-acceptance"></div></div>
          <p class="usage-row"><span>Quality Loss Rate (Today)</span><strong>${Number(modelUsage.qualityLossRatePercent || 0).toFixed(1)}%</strong></p>
          <div class="usage-progress"><div class="usage-progress-bar usage-progress-loss usage-progress-model-loss"></div></div>
          <p class="usage-row"><span>Drafts Given (Total)</span><strong>${Number(item.draftsGiven || 0).toLocaleString()}</strong></p>
          <p class="usage-row"><span>Turned Down (Total)</span><strong>${Number(item.turnedDown || 0).toLocaleString()}</strong></p>
          <p class="draft-meta">
            duplicate: ${Number(byReason.duplicate || 0).toLocaleString()} |
            stale: ${Number(byReason.stale_or_not_time_relevant || 0).toLocaleString()} |
            thin: ${Number(byReason.low_newsworthiness_or_thin || 0).toLocaleString()} |
            style: ${Number(byReason.style_mismatch || 0).toLocaleString()} |
            user_error: ${Number(byReason.user_error || 0).toLocaleString()}
          </p>
          <div class="admin-grid single">
            <label>
              Daily Budget
              <input class="model-budget-input" type="number" min="1" step="1000" value="${budgetValue}" />
            </label>
          </div>
          <div class="admin-actions">
            <button class="btn btn-primary btn-save-model-budget" type="button">Save ${escapeHtml(model)} Budget</button>
          </div>
        </div>
      </article>
    `;
  }).join('');

  modelMetricsListEl.querySelectorAll('.draft-card').forEach((card) => {
    const dailyBar = card.querySelector('.usage-progress-model-daily');
    const weeklyBar = card.querySelector('.usage-progress-model-weekly');
    const monthlyBar = card.querySelector('.usage-progress-model-monthly');
    const acceptanceBar = card.querySelector('.usage-progress-model-acceptance');
    const lossBar = card.querySelector('.usage-progress-model-loss');
    const modelName = card.getAttribute('data-model-name') || '';
    const data = usageByModel[modelName] || {};
    paintUsageBar(dailyBar, Number(data.budgetUsedPercent || 0));
    paintUsageBar(weeklyBar, Number(data.weeklyBudgetUsedPercent || 0));
    paintUsageBar(monthlyBar, Number(data.monthlyBudgetUsedPercent || 0));
    paintOutcomeBar(acceptanceBar, Number(data.acceptanceRatePercent || 0));
    paintLossBar(lossBar, Number(data.qualityLossRatePercent || 0));
  });
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
  const [usageGlobal, quality] = await Promise.all([
    apiRequest('/api/admin-usage?scope=all'),
    apiRequest('/api/admin-quality-metrics')
  ]);

  if (!globalDailyLabelEl) return;
  globalDailyLabelEl.textContent = `${Number(usageGlobal.budgetUsedPercent || 0).toFixed(1)}%`;
  globalWeeklyLabelEl.textContent = `${Number(usageGlobal.weeklyBudgetUsedPercent || 0).toFixed(1)}%`;
  globalMonthlyLabelEl.textContent = `${Number(usageGlobal.monthlyBudgetUsedPercent || 0).toFixed(1)}%`;
  globalThroughputLabelEl.textContent = `${Number(usageGlobal.throughputPercent || 0).toFixed(1)}%`;
  globalAcceptanceLabelEl.textContent = `${Number(usageGlobal.acceptanceRatePercent || 0).toFixed(1)}%`;
  globalQualityLossLabelEl.textContent = `${Number(usageGlobal.qualityLossRatePercent || 0).toFixed(1)}%`;
  globalTokensTodayEl.textContent = Number(usageGlobal.dailyTokensUsed || 0).toLocaleString();
  globalDraftsTodayEl.textContent = Number(usageGlobal.dailyDrafts || 0).toLocaleString();
  if (globalQualityLossTodayValueEl) {
    globalQualityLossTodayValueEl.textContent = `${Number(usageGlobal.qualityLossRatePercent || 0).toFixed(1)}%`;
  }
  paintUsageBar(globalProgressDailyEl, Number(usageGlobal.budgetUsedPercent || 0));
  paintUsageBar(globalProgressWeeklyEl, Number(usageGlobal.weeklyBudgetUsedPercent || 0));
  paintUsageBar(globalProgressMonthlyEl, Number(usageGlobal.monthlyBudgetUsedPercent || 0));
  paintOutcomeBar(globalProgressThroughputEl, Number(usageGlobal.throughputPercent || 0));
  paintOutcomeBar(globalProgressAcceptanceEl, Number(usageGlobal.acceptanceRatePercent || 0));
  paintLossBar(globalProgressQualityLossEl, Number(usageGlobal.qualityLossRatePercent || 0));

  const dailyQuality = quality.daily || {};
  const monthlyQuality = quality.monthly || {};
  const dailyByReason = dailyQuality.byReason || {};
  if (qualityRejectedTodayEl) qualityRejectedTodayEl.textContent = Number(dailyQuality.totalRejected || 0).toLocaleString();
  if (qualityRejectedMonthEl) qualityRejectedMonthEl.textContent = Number(monthlyQuality.totalRejected || 0).toLocaleString();
  if (qualityBadTokensTodayEl) qualityBadTokensTodayEl.textContent = Number(dailyQuality.badTokensTotal || 0).toLocaleString();
  if (qualityBadTokensMonthEl) qualityBadTokensMonthEl.textContent = Number(monthlyQuality.badTokensTotal || 0).toLocaleString();
  if (qualityDuplicateTodayEl) qualityDuplicateTodayEl.textContent = Number(dailyByReason.duplicate || 0).toLocaleString();
  if (qualityStaleTodayEl) qualityStaleTodayEl.textContent = Number(dailyByReason.stale_or_not_time_relevant || 0).toLocaleString();
  if (qualityThinTodayEl) qualityThinTodayEl.textContent = Number(dailyByReason.low_newsworthiness_or_thin || 0).toLocaleString();
  if (qualityStyleTodayEl) qualityStyleTodayEl.textContent = Number(dailyByReason.style_mismatch || 0).toLocaleString();
  if (budgetTotalDailyEl) budgetTotalDailyEl.textContent = Number(usageGlobal.dailyTokenBudget || 0).toLocaleString();
  if (budgetTotalUsedEl) budgetTotalUsedEl.textContent = Number(usageGlobal.dailyTokensUsed || 0).toLocaleString();
  if (budgetTotalRemainingEl) budgetTotalRemainingEl.textContent = Number(usageGlobal.tokensRemainingToday || 0).toLocaleString();
  if (budgetDraftTargetEl) budgetDraftTargetEl.textContent = Number(usageGlobal.dailyDraftTarget || 81).toLocaleString();

  const models = Array.from(new Set([
    ...(quality.configuredModels || []).map((v) => String(v || '').trim()),
    ...((quality.modelBreakdown?.total || []).map((item) => String(item.model || '').trim()))
  ].filter(Boolean)));
  const usageByModelEntries = await Promise.all(models.map(async (model) => {
    const usage = await apiRequest(`/api/admin-usage?scope=all&model=${encodeURIComponent(model)}`);
    return [model, usage];
  }));
  const usageByModel = Object.fromEntries(usageByModelEntries);
  renderModelMetrics(quality.modelBreakdown?.total || [], usageByModel);
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

function paintOutcomeBar(barEl, percentValue) {
  if (!barEl) return;
  const clamped = Math.max(0, Math.min(100, Number(percentValue || 0)));
  barEl.style.width = `${clamped}%`;
  barEl.classList.remove('usage-progress-good', 'usage-progress-warn', 'usage-progress-danger');
  if (clamped >= 75) {
    barEl.classList.add('usage-progress-good');
  } else if (clamped >= 45) {
    barEl.classList.add('usage-progress-warn');
  } else {
    barEl.classList.add('usage-progress-danger');
  }
}

function paintLossBar(barEl, lossPercent) {
  if (!barEl) return;
  const clamped = Math.max(0, Math.min(100, Number(lossPercent || 0)));
  barEl.style.width = `${clamped}%`;
  barEl.classList.remove('usage-progress-good', 'usage-progress-warn', 'usage-progress-danger');
  barEl.classList.add('usage-progress-danger');
}

async function saveModelBudget(modelName, budgetValue) {
  const numeric = Number(budgetValue || 0);
  if (!numeric || numeric < 1) {
    throw new Error('Enter a valid model daily budget');
  }
  await apiRequest('/api/admin-budget', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      modelBudgets: {
        [modelName]: numeric
      }
    })
  });
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

  if (button.classList.contains('btn-save-model-budget')) {
    const modelName = String(card.dataset.modelName || '').trim();
    const input = card.querySelector('.model-budget-input');
    if (!modelName || !input) return;
    saveModelBudget(modelName, input.value)
      .then(loadUsageDashboard)
      .then(() => setMessage(`${modelName} budget updated.`))
      .catch((err) => setMessage(`Model budget update failed: ${err.message}`));
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
if (appSection) appSection.addEventListener('click', onListClick);
if (runFilterInput) {
  runFilterInput.addEventListener('change', () => renderGenerationRuns(generationRuns));
}

loadToken();
setLockState(sessionStorage.getItem('de_admin_unlocked_settings') === '1');
if (unlocked && getToken()) {
  loadAll().catch((err) => setMessage(`Load failed: ${err.message}`));
}
