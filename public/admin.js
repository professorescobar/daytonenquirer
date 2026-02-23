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
const usageRejectedTotalEl = document.getElementById('usage-rejected-total');
const usageRejectedDuplicateEl = document.getElementById('usage-rejected-duplicate');
const usageRejectedStaleEl = document.getElementById('usage-rejected-stale');
const usageRejectedThinEl = document.getElementById('usage-rejected-thin');
const usageRejectedStyleEl = document.getElementById('usage-rejected-style');
const usageRejectedUserErrorEl = document.getElementById('usage-rejected-user-error');
const usageBadTokensEl = document.getElementById('usage-bad-tokens');
const duplicateLimitInput = document.getElementById('duplicate-limit');
const loadDuplicatesBtn = document.getElementById('load-duplicates-btn');
const duplicateListEl = document.getElementById('duplicate-list');
const rejectedLimitInput = document.getElementById('rejected-limit');
const loadRejectionsBtn = document.getElementById('load-rejections-btn');
const rejectionListEl = document.getElementById('rejection-list');
const rejectModalEl = document.getElementById('reject-modal');
const rejectReasonInput = document.getElementById('reject-reason');
const rejectNotesInput = document.getElementById('reject-notes');
const rejectConfirmBtn = document.getElementById('reject-confirm-btn');
const rejectCancelBtn = document.getElementById('reject-cancel-btn');

const CLOUDINARY_CLOUD_NAME = 'dtlkzlp87';
const CLOUDINARY_UPLOAD_PRESET = 'dayton-enquirer';
const CLOUDINARY_WIDTH = 1600;

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
let rejectTargetDraftId = 0;

function getToken() {
  return (tokenInput.value || '').trim();
}

function setMessage(text) {
  messageEl.hidden = !text;
  messageEl.textContent = text || '';
}

function scrollToTopStatus() {
  window.scrollTo({ top: 0, behavior: 'smooth' });
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
    loadUsageDashboard().catch((err) => setMessage(`Usage load failed: ${err.message}`));
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
      <button class="draft-header draft-toggle btn-reset" type="button">
        <strong>#${draft.id} - ${escapeHtml(draft.title || '')}</strong>
        <span class="draft-meta">
          section: ${escapeHtml(draft.section || '')} |
          pubdate: ${escapeHtml(formatDate(draft.pubDate || draft.sourcePublishedAt || draft.createdAt))} |
          status: ${escapeHtml(draft.status || '')}
        </span>
      </button>
      <p class="draft-meta article-editor is-collapsed" hidden>
        section: ${escapeHtml(draft.section || '')} |
        slug: ${escapeHtml(draft.slug || '')} |
        via: ${escapeHtml(draft.createdVia || 'unknown')} |
        created: ${escapeHtml(formatDate(draft.createdAt))}
      </p>
      <p class="draft-meta article-editor is-collapsed" hidden>source: <a href="${escapeHtml(draft.sourceUrl || '#')}" target="_blank" rel="noopener noreferrer">${escapeHtml(draft.sourceUrl || 'N/A')}</a></p>

      <div class="draft-form article-editor is-collapsed" hidden>
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
        <div class="full image-uploader">
          <button type="button" class="upload-dropzone btn-reset">
            Drop image here or click to upload
          </button>
          <input class="file-image" type="file" accept="image/*" hidden />
          <p class="upload-hint">Uploads to Cloudinary and auto-fills optimized URL.</p>
          <p class="upload-status" hidden></p>
          <div class="upload-preview" ${draft.image ? '' : 'hidden'}>
            <img src="${escapeHtml(draft.image || '')}" alt="Uploaded preview" loading="lazy" />
          </div>
        </div>
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
        <label class="full">
          Publish Date (ET, optional)
          <input class="field-publish-at-et" type="datetime-local" value="" />
          <small class="hint">Leave blank to publish at the current time.</small>
        </label>
      </div>

      <div class="draft-actions article-editor is-collapsed" hidden>
        <button class="btn btn-save">Save Draft</button>
        <button class="btn btn-primary btn-publish">Publish Draft</button>
        <button class="btn btn-warning btn-reject">Reject Draft</button>
        <button class="btn btn-warning btn-report-duplicate">Report Duplicate</button>
      </div>
    </article>
  `).join('');
}

function renderDuplicateReports(reports) {
  if (!duplicateListEl) return;
  if (!Array.isArray(reports) || reports.length === 0) {
    duplicateListEl.innerHTML = '<p>No duplicate reports found.</p>';
    return;
  }

  duplicateListEl.innerHTML = reports.map((report) => `
    <article class="draft-card" data-report-id="${report.id}">
      <div class="draft-header">
        <strong>#${report.id} - ${escapeHtml(report.draftTitle || '')}</strong>
        <span class="draft-meta">section: ${escapeHtml(report.section || 'n/a')} | type: ${escapeHtml(report.duplicateType || 'internal')}</span>
      </div>
      <p class="draft-meta">
        source title: ${escapeHtml(report.sourceTitle || 'N/A')} |
        reported: ${escapeHtml(formatDate(report.reportedAt))}
      </p>
      <p class="draft-meta">source url:
        <a href="${escapeHtml(report.sourceUrl || '#')}" target="_blank" rel="noopener noreferrer">${escapeHtml(report.sourceUrl || 'N/A')}</a>
      </p>
      <div class="draft-actions">
        <button class="btn btn-danger btn-remove-duplicate-report">Remove From Duplicate List</button>
      </div>
    </article>
  `).join('');
}

function renderRejections(rejections) {
  if (!rejectionListEl) return;
  if (!Array.isArray(rejections) || rejections.length === 0) {
    rejectionListEl.innerHTML = '<p>No rejected drafts found.</p>';
    return;
  }

  rejectionListEl.innerHTML = rejections.map((item) => `
    <article class="draft-card" data-rejection-id="${item.id}">
      <div class="draft-header">
        <strong>#${item.id} - ${escapeHtml(item.draftTitle || '')}</strong>
        <span class="draft-meta">reason: ${escapeHtml(item.rejectReason || 'n/a')}</span>
      </div>
      <p class="draft-meta">
        section: ${escapeHtml(item.section || 'n/a')} |
        tokens: ${Number(item.totalTokens || 0).toLocaleString()} |
        rejected: ${escapeHtml(formatDate(item.rejectedAt))}
      </p>
      <p class="draft-meta">source:
        <a href="${escapeHtml(item.sourceUrl || '#')}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.sourceUrl || 'N/A')}</a>
      </p>
      <p class="draft-meta">notes: ${escapeHtml(item.notes || 'none')}</p>
      <div class="draft-actions">
        <button class="btn btn-danger btn-delete-rejection">Permanently Delete Record</button>
      </div>
    </article>
  `).join('');
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

function getUploadElements(card) {
  return {
    imageInput: card.querySelector('.field-image'),
    fileInput: card.querySelector('.file-image'),
    status: card.querySelector('.upload-status'),
    previewWrap: card.querySelector('.upload-preview'),
    previewImg: card.querySelector('.upload-preview img')
  };
}

function setUploadStatus(el, text) {
  if (!el) return;
  el.hidden = !text;
  el.textContent = text || '';
}

async function handleCardImageUpload(card, file) {
  const { imageInput, status, previewWrap, previewImg } = getUploadElements(card);
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
    const url = `/api/admin-generate-drafts?count=${count}&includeSections=${includeSections}&excludeSections=${excludeSections}&runMode=manual`;
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
  const [usage, quality] = await Promise.all([
    apiRequest('/api/admin-usage'),
    apiRequest('/api/admin-quality-metrics')
  ]);

  usageTokensEl.textContent = Number(usage.tokensUsedToday || 0).toLocaleString();
  usageBudgetEl.textContent = Number(usage.dailyTokenBudget || 0).toLocaleString();
  usagePercentEl.textContent = `${usage.budgetPercent || 0}%`;
  usageDraftsEl.textContent = Number(usage.draftsToday || 0).toLocaleString();
  usageBudgetInput.value = Number(usage.dailyTokenBudget || 0);

  const byReason = quality.byReason || {};
  usageRejectedTotalEl.textContent = Number(quality.totalRejected || 0).toLocaleString();
  usageRejectedDuplicateEl.textContent = Number(byReason.duplicate || 0).toLocaleString();
  usageRejectedStaleEl.textContent = Number(byReason.stale_or_not_time_relevant || 0).toLocaleString();
  usageRejectedThinEl.textContent = Number(byReason.low_newsworthiness_or_thin || 0).toLocaleString();
  usageRejectedStyleEl.textContent = Number(byReason.style_mismatch || 0).toLocaleString();
  if (usageRejectedUserErrorEl) {
    usageRejectedUserErrorEl.textContent = Number(byReason.user_error || 0).toLocaleString();
  }
  usageBadTokensEl.textContent = Number(quality.badTokensTotal || 0).toLocaleString();
}

async function loadDuplicateReports() {
  try {
    setMessage('Loading duplicate reports...');
    const limit = encodeURIComponent(duplicateLimitInput?.value || '50');
    const data = await apiRequest(`/api/admin-duplicate-reports?limit=${limit}`);
    renderDuplicateReports(data.reports || []);
    setMessage(`Loaded ${data.count || 0} duplicate report(s).`);
  } catch (err) {
    setMessage(`Load duplicate reports failed: ${err.message}`);
  }
}

async function removeDuplicateReport(id) {
  await apiRequest('/api/admin-remove-duplicate-report', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id })
  });
}

async function loadRejections() {
  try {
    setMessage('Loading rejected drafts...');
    const limit = encodeURIComponent(rejectedLimitInput?.value || '50');
    const data = await apiRequest(`/api/admin-rejections?limit=${limit}`);
    renderRejections(data.rejections || []);
    setMessage(`Loaded ${data.count || 0} rejected draft record(s).`);
  } catch (err) {
    setMessage(`Load rejections failed: ${err.message}`);
  }
}

async function deleteRejection(id) {
  await apiRequest('/api/admin-delete-rejection', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id })
  });
}

function openRejectModal(draftId) {
  rejectTargetDraftId = Number(draftId || 0);
  if (!rejectTargetDraftId || !rejectModalEl) return;
  rejectReasonInput.value = '';
  rejectNotesInput.value = '';
  rejectModalEl.hidden = false;
}

function closeRejectModal() {
  rejectTargetDraftId = 0;
  if (!rejectModalEl) return;
  rejectModalEl.hidden = true;
}

async function confirmRejectDraft() {
  const reason = String(rejectReasonInput?.value || '').trim();
  const notes = String(rejectNotesInput?.value || '').trim();
  if (!rejectTargetDraftId) throw new Error('Missing reject target draft');
  if (!reason) throw new Error('Select a rejection reason');

  await apiRequest('/api/admin-reject-draft', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: rejectTargetDraftId,
      reason,
      notes
    })
  });
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
  const publishAtEt = card.querySelector('.field-publish-at-et')?.value || '';
  await apiRequest('/api/admin-publish-draft', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id, publishAtEt })
  });
}

async function reportDuplicateDraft(card) {
  const id = Number(card.dataset.id);
  const duplicateType = String(card.dataset.duplicateType || 'internal').trim().toLowerCase();
  await apiRequest('/api/admin-report-duplicate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id,
      reason: 'manual_duplicate',
      duplicateType
    })
  });
}

function chooseDuplicateType(draftId) {
  const answer = window.prompt(
    `Draft #${draftId}: enter duplicate type ("internal" or "external").`,
    'internal'
  );
  if (answer === null) return null;
  const normalized = String(answer || '').trim().toLowerCase();
  if (!['internal', 'external'].includes(normalized)) {
    throw new Error('Duplicate type must be "internal" or "external".');
  }
  return normalized;
}

function onDraftListClick(event) {
  const button = event.target.closest('button');
  if (!button) return;

  const card = event.target.closest('.draft-card');
  if (!card) return;

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

  if (button.classList.contains('btn-save')) {
    saveDraft(card)
      .then(() => setMessage(`Draft #${card.dataset.id} saved.`))
      .catch((err) => setMessage(`Save failed: ${err.message}`));
  }

  if (button.classList.contains('btn-publish')) {
    saveDraft(card)
      .then(() => publishDraft(card))
      .then(async () => {
        setMessage(`Draft #${card.dataset.id} published.`);
        scrollToTopStatus();
        await loadDrafts();
      })
      .catch((err) => {
        setMessage(`Publish failed: ${err.message}`);
        scrollToTopStatus();
      });
  }

  if (button.classList.contains('btn-reject')) {
    openRejectModal(card.dataset.id);
  }

  if (button.classList.contains('btn-report-duplicate')) {
    let duplicateType;
    try {
      duplicateType = chooseDuplicateType(card.dataset.id);
    } catch (err) {
      setMessage(err.message);
      return;
    }
    if (!duplicateType) return;
    card.dataset.duplicateType = duplicateType;

    const ok = window.confirm(
      `Report draft #${card.dataset.id} as ${duplicateType} duplicate and remove it from drafts?`
    );
    if (!ok) return;

    reportDuplicateDraft(card)
      .then(async () => {
        setMessage(`Draft #${card.dataset.id} reported as duplicate and removed.`);
        scrollToTopStatus();
        await loadDrafts();
        await loadUsageDashboard();
      })
      .catch((err) => {
        setMessage(`Report duplicate failed: ${err.message}`);
        scrollToTopStatus();
      });
  }

  if (button.classList.contains('upload-dropzone')) {
    const fileInput = card.querySelector('.file-image');
    if (fileInput) fileInput.click();
  }
}

function onDuplicateListClick(event) {
  const button = event.target.closest('button');
  if (!button || !button.classList.contains('btn-remove-duplicate-report')) return;
  const card = event.target.closest('[data-report-id]');
  if (!card) return;

  const reportId = Number(card.dataset.reportId || 0);
  if (!reportId) return;
  const ok = window.confirm(`Remove duplicate report #${reportId}?`);
  if (!ok) return;

  removeDuplicateReport(reportId)
    .then(async () => {
      setMessage(`Duplicate report #${reportId} removed.`);
      await loadDuplicateReports();
      await loadUsageDashboard();
    })
    .catch((err) => setMessage(`Remove duplicate report failed: ${err.message}`));
}

function onRejectionListClick(event) {
  const button = event.target.closest('button');
  if (!button || !button.classList.contains('btn-delete-rejection')) return;
  const card = event.target.closest('[data-rejection-id]');
  if (!card) return;

  const rejectionId = Number(card.dataset.rejectionId || 0);
  if (!rejectionId) return;
  const ok = window.confirm(`Permanently delete rejection record #${rejectionId}?`);
  if (!ok) return;

  deleteRejection(rejectionId)
    .then(async () => {
      setMessage(`Rejection record #${rejectionId} deleted.`);
      await loadRejections();
      await loadUsageDashboard();
    })
    .catch((err) => setMessage(`Delete rejection failed: ${err.message}`));
}

function onRejectModalConfirm() {
  confirmRejectDraft()
    .then(async () => {
      const draftId = rejectTargetDraftId;
      closeRejectModal();
      setMessage(`Draft #${draftId} rejected and moved to rejected list.`);
      scrollToTopStatus();
      await loadDrafts();
      await loadUsageDashboard();
    })
    .catch((err) => {
      setMessage(`Reject failed: ${err.message}`);
      scrollToTopStatus();
    });
}

function onDraftListChange(event) {
  const input = event.target;
  if (!input.classList.contains('file-image')) return;
  const card = input.closest('.draft-card');
  if (!card) return;
  const file = input.files && input.files[0];
  handleCardImageUpload(card, file);
}

function onDraftListDragOver(event) {
  const zone = event.target.closest('.upload-dropzone');
  if (!zone) return;
  event.preventDefault();
  zone.classList.add('is-drag-over');
}

function onDraftListDragLeave(event) {
  const zone = event.target.closest('.upload-dropzone');
  if (!zone) return;
  zone.classList.remove('is-drag-over');
}

function onDraftListDrop(event) {
  const zone = event.target.closest('.upload-dropzone');
  if (!zone) return;
  event.preventDefault();
  zone.classList.remove('is-drag-over');

  const card = zone.closest('.draft-card');
  const file = event.dataTransfer?.files?.[0];
  if (card && file) {
    handleCardImageUpload(card, file);
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
if (loadDuplicatesBtn) loadDuplicatesBtn.addEventListener('click', loadDuplicateReports);
if (loadRejectionsBtn) loadRejectionsBtn.addEventListener('click', loadRejections);
draftListEl.addEventListener('click', onDraftListClick);
if (duplicateListEl) duplicateListEl.addEventListener('click', onDuplicateListClick);
if (rejectionListEl) rejectionListEl.addEventListener('click', onRejectionListClick);
draftListEl.addEventListener('change', onDraftListChange);
draftListEl.addEventListener('dragover', onDraftListDragOver);
draftListEl.addEventListener('dragleave', onDraftListDragLeave);
draftListEl.addEventListener('drop', onDraftListDrop);
unlockAdminBtn.addEventListener('click', unlockAdminUi);
saveBudgetBtn.addEventListener('click', saveBudget);
if (rejectConfirmBtn) rejectConfirmBtn.addEventListener('click', onRejectModalConfirm);
if (rejectCancelBtn) rejectCancelBtn.addEventListener('click', closeRejectModal);
if (rejectModalEl) {
  rejectModalEl.addEventListener('click', (event) => {
    const target = event.target;
    if (target && target.classList && target.classList.contains('admin-modal-backdrop')) {
      closeRejectModal();
    }
  });
}

loadStoredToken();
setLockState(sessionStorage.getItem('de_admin_unlocked') === '1');
if (adminUiUnlocked) {
  loadUsageDashboard().catch((err) => setMessage(`Usage load failed: ${err.message}`));
}
