const lockSection = document.getElementById('dictionary-lock');
const appSection = document.getElementById('dictionary-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');
const tokenInput = document.getElementById('admin-token');
const saveTokenBtn = document.getElementById('save-token-btn');
const loadDictionaryBtn = document.getElementById('load-dictionary-btn');
const runFreshnessScanBtn = document.getElementById('run-freshness-scan-btn');
const reviewLimitInput = document.getElementById('review-limit');
const rootLimitInput = document.getElementById('root-limit');
const dispatchRefreshesInput = document.getElementById('dispatch-refreshes');
const refreshLimitInput = document.getElementById('refresh-limit');
const refreshCooldownHoursInput = document.getElementById('refresh-cooldown-hours');
const messageEl = document.getElementById('dictionary-message');
const summaryEl = document.getElementById('dictionary-summary');
const maintenanceStatusEl = document.getElementById('dictionary-maintenance-status');
const reviewItemsEl = document.getElementById('dictionary-review-items');
const attentionRootsEl = document.getElementById('dictionary-attention-roots');
const recentRunsEl = document.getElementById('dictionary-recent-runs');
const recentDispatchRunsEl = document.getElementById('dictionary-recent-dispatch-runs');

let unlocked = false;

function cleanText(value, max = 2000) {
  return String(value || '').trim().slice(0, max);
}

function parsePositiveInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function setMessage(text) {
  messageEl.hidden = !text;
  messageEl.textContent = text || '';
}

function setLockState(value) {
  unlocked = value;
  lockSection.hidden = value;
  appSection.hidden = !value;
}

function getToken() {
  return (tokenInput.value || '').trim();
}

function formatDate(dateString) {
  if (!dateString) return '';
  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) return String(dateString);
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

async function apiRequest(url, options = {}) {
  if (!unlocked) {
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
    throw new Error(data.error || `Request failed (${res.status})`);
  }
  return data;
}

function renderSummary(summary = {}) {
  const cards = [
    {
      label: 'Open Items',
      value: summary.openItemCount || 0
    },
    {
      label: 'Critical Open',
      value: summary.criticalOpenCount || 0
    },
    {
      label: 'Freshness Overdue',
      value: summary.freshnessOverdueOpenCount || 0
    },
    {
      label: 'Expired High Impact',
      value: summary.expiredHighImpactOpenCount || 0
    },
    {
      label: 'High Severity Open',
      value: summary.highOpenCount || 0
    }
  ];

  summaryEl.innerHTML = cards.map((card) => `
    <article class="usage-card">
      <strong>${escapeHtml(card.label)}</strong>
      <div class="dictionary-metric-value">${escapeHtml(card.value)}</div>
    </article>
  `).join('');
}

function renderReviewItems(items = []) {
  if (!items.length) {
    reviewItemsEl.innerHTML = '<p class="hint">No matching review items.</p>';
    return;
  }

  reviewItemsEl.innerHTML = items.map((item) => `
    <article class="draft-card dictionary-card">
      <div class="dictionary-card-header">
        <strong>${escapeHtml(item.itemType || 'unknown')}</strong>
        <span class="dictionary-pill severity-${escapeHtml(item.severity || 'low')}">${escapeHtml(item.severity || 'low')}</span>
      </div>
      <p class="dictionary-card-meta">
        Root: ${escapeHtml(item.rootSourceName || item.rootSourceUrl || 'unlinked')}
        <br />
        Record: ${escapeHtml(item.affectedRecordType || 'unknown')} ${escapeHtml(item.affectedRecordId || '')}
      </p>
      <p>${escapeHtml(item.lastError || 'No error message recorded.')}</p>
      <p class="hint">${escapeHtml(item.suggestedAction || 'No suggested action recorded.')}</p>
      <p class="dictionary-card-meta">
        Retry Count: ${escapeHtml(item.retryCount)}
        <br />
        Last Failed: ${escapeHtml(formatDate(item.lastFailedAt))}
      </p>
    </article>
  `).join('');
}

function renderAttentionRoots(roots = []) {
  if (!roots.length) {
    attentionRootsEl.innerHTML = '<p class="hint">No roots currently require attention.</p>';
    return;
  }

  attentionRootsEl.innerHTML = roots.map((root) => `
    <article class="draft-card dictionary-card">
      <div class="dictionary-card-header">
        <strong>${escapeHtml(root.sourceName || root.rootUrl || 'Unknown root')}</strong>
        <span class="dictionary-pill">${escapeHtml(root.attentionReason || 'attention_required')}</span>
      </div>
      <p class="dictionary-card-meta">
        ${escapeHtml(root.rootUrl || '')}
        <br />
        Trust: ${escapeHtml(root.trustTier || 'unknown')}
      </p>
      <p class="hint">
        Blocked: ${root.isBlocked ? 'yes' : 'no'} |
        Overdue by SLA: ${root.overdueByFreshnessSla ? 'yes' : 'no'} |
        Dispatch Eligible: ${root.shouldDispatchRefresh ? 'yes' : 'no'}
      </p>
      <p class="dictionary-card-meta">
        Last Successful Crawl: ${escapeHtml(formatDate(root.lastSuccessfulCrawlAt))}
        <br />
        Blocking Failures: ${escapeHtml(root.openBlockingFailureCount || 0)}
      </p>
    </article>
  `).join('');
}

function renderMaintenanceStatus(recentDispatchRuns = [], recentRuns = []) {
  const latestDispatch = recentDispatchRuns[0] || null;
  const latestFreshness = recentRuns[0] || null;

  const cards = [
    {
      title: 'Dispatch Scheduler',
      body: latestDispatch
        ? `Latest ${latestDispatch.triggerType || 'unknown'} dispatch ${formatDate(latestDispatch.createdAt)} with status ${latestDispatch.status || 'unknown'}.`
        : 'No maintenance dispatch runs recorded yet.'
    },
    {
      title: 'Dispatch Selection',
      body: latestDispatch
        ? `Mode ${latestDispatch.outputPayload?.selectionMode || latestDispatch.inputPayload?.selectionMode || 'unknown'} | eligible ${latestDispatch.outputPayload?.eligibleRootSourceCount || 0} | selected ${latestDispatch.outputPayload?.selectedRootSourceCount || 0} | skipped ${latestDispatch.outputPayload?.skippedRootSourceCount || 0}.`
        : 'No recent dispatch selection data.'
    },
    {
      title: 'Freshness Refresh',
      body: latestFreshness
        ? `Latest freshness run emitted ${latestFreshness.outputPayload?.refreshDispatchEmittedCount || 0} refreshes, suppressed ${latestFreshness.outputPayload?.refreshDispatchSuppressedCount || 0}, and deferred ${latestFreshness.outputPayload?.refreshDispatchDeferredByLimitCount || 0} by limit.`
        : 'No recent freshness refresh data.'
    }
  ];

  maintenanceStatusEl.innerHTML = cards.map((card) => `
    <article class="draft-card dictionary-card">
      <div class="dictionary-card-header">
        <strong>${escapeHtml(card.title)}</strong>
        <span class="dictionary-pill">weekly</span>
      </div>
      <p>${escapeHtml(card.body)}</p>
    </article>
  `).join('');
}

function formatReasonMap(map = {}) {
  const entries = Object.entries(map || {}).filter(([, value]) => Number(value) > 0);
  if (!entries.length) return 'none';
  return entries
    .map(([key, value]) => `${key}:${value}`)
    .join(', ');
}

function renderRecentRuns(runs = []) {
  if (!runs.length) {
    recentRunsEl.innerHTML = '<p class="hint">No Phase F runs recorded yet.</p>';
    return;
  }

  recentRunsEl.innerHTML = runs.map((run) => {
    const output = run.outputPayload || {};
    const counts = output.counts || {};
    return `
      <article class="draft-card dictionary-card">
        <div class="dictionary-card-header">
          <strong>${escapeHtml(run.status || 'unknown')}</strong>
          <span class="dictionary-pill">${escapeHtml(run.triggerType || 'unknown')}</span>
        </div>
        <p class="dictionary-card-meta">
          Created: ${escapeHtml(formatDate(run.createdAt))}
          <br />
          Ended: ${escapeHtml(formatDate(run.endedAt))}
        </p>
        <p class="hint">
          Overdue Roots: ${escapeHtml(counts.overdueRootSourceCount || 0)} |
          Overdue Assertions: ${escapeHtml(counts.overdueAssertionCount || 0)} |
          Expired High Impact: ${escapeHtml(counts.expiredHighImpactAssertionCount || 0)}
        </p>
        <p class="dictionary-card-meta">
          Refresh Dispatch Requested: ${output.refreshDispatchRequested ? 'yes' : 'no'}
          <br />
          Refresh Dispatch Emitted: ${escapeHtml(output.refreshDispatchEmittedCount || 0)}
          <br />
          Suppressed: ${escapeHtml(output.refreshDispatchSuppressedCount || 0)} |
          Deferred By Limit: ${escapeHtml(output.refreshDispatchDeferredByLimitCount || 0)}
        </p>
        <p class="hint">
          Suppression Reasons: ${escapeHtml(formatReasonMap(output.refreshDispatchSuppressedByReason || {}))}
        </p>
      </article>
    `;
  }).join('');
}

function renderRecentDispatchRuns(runs = []) {
  if (!runs.length) {
    recentDispatchRunsEl.innerHTML = '<p class="hint">No maintenance dispatch runs recorded yet.</p>';
    return;
  }

  recentDispatchRunsEl.innerHTML = runs.map((run) => {
    const output = run.outputPayload || {};
    return `
      <article class="draft-card dictionary-card">
        <div class="dictionary-card-header">
          <strong>${escapeHtml(run.status || 'unknown')}</strong>
          <span class="dictionary-pill">${escapeHtml(run.triggerType || 'unknown')}</span>
        </div>
        <p class="dictionary-card-meta">
          Created: ${escapeHtml(formatDate(run.createdAt))}
          <br />
          Ended: ${escapeHtml(formatDate(run.endedAt))}
        </p>
        <p class="hint">
          Mode: ${escapeHtml(output.selectionMode || run.inputPayload?.selectionMode || 'unknown')} |
          Cooldown: ${escapeHtml(output.maintenanceCooldownHours || run.inputPayload?.maintenanceCooldownHours || 'n/a')}
        </p>
        <p class="dictionary-card-meta">
          Evaluated: ${escapeHtml(output.evaluatedRootSourceCount || 0)} |
          Eligible: ${escapeHtml(output.eligibleRootSourceCount || 0)} |
          Selected: ${escapeHtml(output.selectedRootSourceCount || 0)}
          <br />
          Skipped: ${escapeHtml(output.skippedRootSourceCount || 0)}
        </p>
        <p class="hint">
          Skip Reasons: ${escapeHtml(formatReasonMap(output.skippedByReason || {}))}
        </p>
      </article>
    `;
  }).join('');
}

async function loadDictionaryFreshnessView() {
  const limit = parsePositiveInt(reviewLimitInput.value, 25, 1, 100);
  const rootLimit = parsePositiveInt(rootLimitInput.value, 10, 1, 50);
  setMessage('Loading dictionary freshness state...');
  const data = await apiRequest(`/api/admin-dictionary-freshness?limit=${limit}&rootLimit=${rootLimit}&runLimit=10&openOnly=true`);
  renderSummary(data.summary || {});
  renderMaintenanceStatus(data.recentDispatchRuns || [], data.recentRuns || []);
  renderReviewItems(data.reviewItems || []);
  renderAttentionRoots(data.attentionRoots || []);
  renderRecentRuns(data.recentRuns || []);
  renderRecentDispatchRuns(data.recentDispatchRuns || []);
  setMessage('Dictionary freshness view updated.');
}

async function runFreshnessScan() {
  const payload = {
    dispatchRefreshes: dispatchRefreshesInput.value !== 'false',
    refreshLimit: parsePositiveInt(refreshLimitInput.value, 10, 1, 50),
    refreshCooldownHours: parsePositiveInt(refreshCooldownHoursInput.value, 24, 1, 168)
  };

  setMessage('Dispatching manual freshness scan...');
  await apiRequest('/api/admin-dictionary-freshness', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  setMessage('Manual freshness scan dispatched.');
  await loadDictionaryFreshnessView();
}

async function unlockAdminUi() {
  try {
    setMessage('');
    const password = cleanText(adminUiPasswordInput.value, 500);
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
    setMessage('Dictionary admin unlocked.');
    if (getToken()) {
      await loadDictionaryFreshnessView();
    }
  } catch (error) {
    setMessage(`Unlock failed: ${error.message}`);
  }
}

unlockAdminBtn?.addEventListener('click', () => {
  unlockAdminUi().catch((error) => setMessage(`Unlock failed: ${error.message}`));
});

saveTokenBtn?.addEventListener('click', () => {
  const token = getToken();
  if (!token) {
    setMessage('Enter an admin token first.');
    return;
  }
  localStorage.setItem('de_admin_token', token);
  setMessage('Admin token saved locally.');
});

loadDictionaryBtn?.addEventListener('click', () => {
  loadDictionaryFreshnessView().catch((error) => setMessage(`Load failed: ${error.message}`));
});

runFreshnessScanBtn?.addEventListener('click', () => {
  runFreshnessScan().catch((error) => setMessage(`Freshness scan failed: ${error.message}`));
});

window.addEventListener('DOMContentLoaded', () => {
  const savedToken = localStorage.getItem('de_admin_token') || '';
  if (savedToken && tokenInput) tokenInput.value = savedToken;

  if (sessionStorage.getItem('de_admin_unlocked') === '1') {
    setLockState(true);
    if (getToken()) {
      loadDictionaryFreshnessView().catch((error) => setMessage(`Load failed: ${error.message}`));
    }
  } else {
    setLockState(false);
  }
});
