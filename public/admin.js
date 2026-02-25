const tokenInput = document.getElementById('admin-token');
const lockSection = document.getElementById('admin-lock');
const appSection = document.getElementById('admin-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');
const statusFilterInput = document.getElementById('status-filter');
const limitInput = document.getElementById('list-limit');
const genCountInput = document.getElementById('gen-count');
const genProviderInput = document.getElementById('gen-provider');
const genIncludeInput = document.getElementById('gen-include');
const genExcludeInput = document.getElementById('gen-exclude');
const genTokenInput = document.getElementById('gen-admin-token');
const saveGenTokenBtn = document.getElementById('save-gen-token-btn');
const manualUsageTokensEl = document.getElementById('manual-usage-tokens');
const manualUsageRemainingEl = document.getElementById('manual-usage-remaining');
const manualTitleInput = document.getElementById('manual-title');
const manualSectionInput = document.getElementById('manual-section');
const manualTokenInput = document.getElementById('manual-admin-token');
const saveManualTokenBtn = document.getElementById('save-manual-token-btn');
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
  const draftOpsToken = (tokenInput?.value || '').trim();
  const manualToken = (manualTokenInput?.value || '').trim();
  const genToken = (genTokenInput?.value || '').trim();
  return draftOpsToken || manualToken || genToken;
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
    const message = data.error || `Request failed (${res.status})`;
    throw new Error(data.details ? `${message}: ${data.details}` : message);
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
    loadManualUsageSummary().catch(() => {});
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

function normalizeEditorHtml(value) {
  const html = String(value || '').trim();
  return html === '<br>' ? '' : html;
}

function initializeRichTextEditors(root) {
  if (!root) return;
  root.querySelectorAll('.draft-card').forEach((card) => {
    const textarea = card.querySelector('.field-content');
    const editor = card.querySelector('.field-content-editor');
    if (!textarea || !editor) return;
    editor.innerHTML = textarea.value || '';
  });
}

function getCardContentHtml(card) {
  const textarea = card.querySelector('.field-content');
  const editor = card.querySelector('.field-content-editor');
  if (!textarea) return '';
  if (!editor) return textarea.value;
  const html = normalizeEditorHtml(editor.innerHTML);
  textarea.value = html;
  return html;
}

function applyRichTextCommand(button, card) {
  const editor = card.querySelector('.field-content-editor');
  if (!editor) return;
  const command = String(button.dataset.rteCmd || '');
  const value = button.dataset.rteValue || null;
  if (!command) return;

  editor.focus();
  if (command === 'createLink') {
    const rawUrl = window.prompt('Enter URL');
    if (rawUrl == null) return;
    const url = String(rawUrl).trim();
    if (!url) return;
    document.execCommand(command, false, url);
  } else {
    document.execCommand(command, false, value);
  }
  getCardContentHtml(card);
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
          model: ${escapeHtml(draft.model || 'unknown')} |
          status: ${escapeHtml(draft.status || '')}
        </span>
      </button>
      <p class="draft-meta article-editor is-collapsed" hidden>
        section: ${escapeHtml(draft.section || '')} |
        slug: ${escapeHtml(draft.slug || '')} |
        via: ${escapeHtml(draft.createdVia || 'unknown')} |
        model: ${escapeHtml(draft.model || 'unknown')} |
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
          <div class="rte-wrap">
            <div class="rte-toolbar" role="toolbar" aria-label="Content formatting">
              <button type="button" class="btn rte-btn" data-rte-cmd="bold"><strong>B</strong></button>
              <button type="button" class="btn rte-btn" data-rte-cmd="italic"><em>I</em></button>
              <button type="button" class="btn rte-btn" data-rte-cmd="underline"><u>U</u></button>
              <button type="button" class="btn rte-btn" data-rte-cmd="insertUnorderedList">Bullets</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="insertOrderedList">Numbers</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="formatBlock" data-rte-value="h2">H2</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="formatBlock" data-rte-value="p">P</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="createLink">Link</button>
              <button type="button" class="btn rte-btn" data-rte-cmd="removeFormat">Clear</button>
            </div>
            <div class="field-content-editor rte-editor" contenteditable="true" role="textbox" aria-multiline="true"></div>
            <textarea class="field-content" hidden>${escapeHtml(draft.content || '')}</textarea>
          </div>
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
        <button class="btn btn-danger btn-delete-draft">Permanently Delete Draft</button>
      </div>
    </article>
  `).join('');
  initializeRichTextEditors(draftListEl);
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
    const requestedCount = Math.max(1, Number(genCountInput.value || 3));
    const includeSections = encodeURIComponent((genIncludeInput.value || '').trim());
    const excludeSections = encodeURIComponent((genExcludeInput.value || '').trim());
    const selectedProvider = String(genProviderInput?.value || 'anthropic').trim().toLowerCase();

    const buildUrl = (provider, count) => {
      const safeCount = encodeURIComponent(String(count));
      const safeProvider = encodeURIComponent(provider);
      return `/api/admin-generate-drafts?count=${safeCount}&provider=${safeProvider}&includeSections=${includeSections}&excludeSections=${excludeSections}&runMode=manual`;
    };

    let results = [];
    if (selectedProvider === 'all') {
      const providers = ['anthropic', 'openai', 'gemini', 'grok'];
      if (requestedCount < providers.length) {
        throw new Error(`For "All", set Count to at least ${providers.length}, or pick a single model.`);
      }
      const baseCount = Math.floor(requestedCount / providers.length);
      const remainder = requestedCount % providers.length;
      results = await Promise.all(providers.map(async (provider, index) => {
        const countForProvider = baseCount + (index < remainder ? 1 : 0);
        const data = await apiRequest(buildUrl(provider, countForProvider), { method: 'POST' });
        return { provider, data };
      }));
    } else {
      const data = await apiRequest(buildUrl(selectedProvider, requestedCount), { method: 'POST' });
      results = [{ provider: selectedProvider, data }];
    }

    const createdTotal = results.reduce((sum, item) => sum + Number(item.data?.createdCount || 0), 0);
    const skippedTotal = results.reduce((sum, item) => sum + Number(item.data?.skippedCount || 0), 0);
    const breakdown = results
      .map((item) => `${item.provider}: ${Number(item.data?.createdCount || 0)} created`)
      .join(' | ');
    setMessage(`Generated ${createdTotal} draft(s), skipped ${skippedTotal}. ${breakdown}`);
    await loadUsageDashboard();
    await loadManualUsageSummary();
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
    await loadManualUsageSummary();
    await loadDrafts();
  } catch (err) {
    setMessage(`Create draft failed: ${err.message}`);
  }
}

async function loadManualUsageSummary() {
  if (!manualUsageTokensEl || !manualUsageRemainingEl) return;
  const usage = await apiRequest('/api/admin-usage?scope=manual');
  manualUsageTokensEl.textContent = Number(usage.tokensUsedToday || 0).toLocaleString();
  manualUsageRemainingEl.textContent = `${Number(usage.budgetRemainingPercent || 0).toFixed(1)}%`;
}

async function loadUsageDashboard() {
  if (!usageTokensEl) return;
  const usage = await apiRequest('/api/admin-usage');

  usageTokensEl.textContent = Number(usage.tokensUsedToday || 0).toLocaleString();
  usageBudgetEl.textContent = Number(usage.dailyTokenBudget || 0).toLocaleString();
  usagePercentEl.textContent = `${usage.budgetPercent || 0}%`;
  usageDraftsEl.textContent = Number(usage.draftsToday || 0).toLocaleString();
  usageBudgetInput.value = Number(usage.dailyTokenBudget || 0);
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
    const savedBudget = Number(data.dailyTokenBudgetManual || data.dailyTokenBudgetAuto || dailyTokenBudget);
    setMessage(`Daily token budget updated to ${savedBudget.toLocaleString()}.`);
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
    content: getCardContentHtml(card),
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

async function deleteDraft(card) {
  const id = Number(card.dataset.id);
  await apiRequest('/api/admin-delete-draft', {
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

  if (button.classList.contains('rte-btn')) {
    applyRichTextCommand(button, card);
    return;
  }

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

  if (button.classList.contains('btn-delete-draft')) {
    const ok = window.confirm(`Permanently delete draft #${card.dataset.id}? This cannot be undone.`);
    if (!ok) return;

    deleteDraft(card)
      .then(async () => {
        setMessage(`Draft #${card.dataset.id} permanently deleted.`);
        scrollToTopStatus();
        await loadDrafts();
      })
      .catch((err) => {
        setMessage(`Delete failed: ${err.message}`);
        scrollToTopStatus();
      });
  }

  if (button.classList.contains('upload-dropzone')) {
    const fileInput = card.querySelector('.file-image');
    if (fileInput) fileInput.click();
  }
}

function onAppSectionClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  const button = target.closest('button');
  if (!button || !button.classList.contains('section-toggle')) return;
  const card = button.closest('.draft-card');
  if (!card) return;
  const editors = card.querySelectorAll('.section-editor');
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
  if (!saved) return;
  if (tokenInput) tokenInput.value = saved;
  if (manualTokenInput) manualTokenInput.value = saved;
  if (genTokenInput) genTokenInput.value = saved;
}

function saveToken() {
  const current = getToken();
  localStorage.setItem('de_admin_token', current);
  if (tokenInput && !tokenInput.value) tokenInput.value = current;
  if (manualTokenInput && !manualTokenInput.value) manualTokenInput.value = current;
  if (genTokenInput && !genTokenInput.value) genTokenInput.value = current;
  setMessage('Token saved in this browser.');
}

function syncTokenInputs(event) {
  const source = event?.target;
  const value = String(source?.value || '');
  if (tokenInput && source !== tokenInput) tokenInput.value = value;
  if (manualTokenInput && source !== manualTokenInput) manualTokenInput.value = value;
  if (genTokenInput && source !== genTokenInput) genTokenInput.value = value;
}

saveTokenBtn.addEventListener('click', saveToken);
if (saveManualTokenBtn) saveManualTokenBtn.addEventListener('click', saveToken);
if (saveGenTokenBtn) saveGenTokenBtn.addEventListener('click', saveToken);
loadDraftsBtn.addEventListener('click', loadDrafts);
generateBtn.addEventListener('click', generateDrafts);
createDraftBtn.addEventListener('click', createManualDraft);
draftListEl.addEventListener('click', onDraftListClick);
draftListEl.addEventListener('change', onDraftListChange);
draftListEl.addEventListener('dragover', onDraftListDragOver);
draftListEl.addEventListener('dragleave', onDraftListDragLeave);
draftListEl.addEventListener('drop', onDraftListDrop);
unlockAdminBtn.addEventListener('click', unlockAdminUi);
if (saveBudgetBtn) saveBudgetBtn.addEventListener('click', saveBudget);
if (appSection) appSection.addEventListener('click', onAppSectionClick);
if (tokenInput) tokenInput.addEventListener('input', syncTokenInputs);
if (manualTokenInput) manualTokenInput.addEventListener('input', syncTokenInputs);
if (genTokenInput) genTokenInput.addEventListener('input', syncTokenInputs);

loadStoredToken();
setLockState(sessionStorage.getItem('de_admin_unlocked') === '1');
if (adminUiUnlocked) {
  loadUsageDashboard().catch((err) => setMessage(`Usage load failed: ${err.message}`));
  loadManualUsageSummary().catch(() => {});
}
