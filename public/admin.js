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

function aiModelSelectHtml(defaultValue = 'anthropic:claude-sonnet-4-6') {
  const options = [
    { value: 'anthropic:claude-sonnet-4-6', label: 'Claude Sonnet 4.6' },
    { value: 'openai:gpt-5', label: 'ChatGPT (GPT-5)' },
    { value: 'gemini:gemini-3-pro-preview', label: 'Gemini 3 Pro Preview' },
    { value: 'grok:grok-4', label: 'Grok 4' }
  ];
  return options
    .map((opt) => `<option value="${opt.value}" ${opt.value === defaultValue ? 'selected' : ''}>${opt.label}</option>`)
    .join('');
}

function rewriteModelSelectHtml() {
  return `<option value="" selected>Select model...</option>${aiModelSelectHtml('__none__')}`;
}

const REWRITE_ISSUES = {
  base: {
    headline: [
      { id: 'too_generic', label: 'Too generic' },
      { id: 'not_newsworthy', label: 'Not newsworthy enough' },
      { id: 'unclear', label: 'Unclear' },
      { id: 'too_long', label: 'Too long' },
      { id: 'too_clickbait', label: 'Too clickbait' },
      { id: 'weak_hook', label: 'Weak hook' }
    ],
    description: [
      { id: 'too_vague', label: 'Too vague' },
      { id: 'too_hypey', label: 'Too hypey' },
      { id: 'too_generic', label: 'Too generic' },
      { id: 'too_wordy', label: 'Too wordy' },
      { id: 'weak_seo', label: 'Weak SEO focus' },
      { id: 'weak_hook', label: 'Weak hook' }
    ],
    article: [
      { id: 'not_long_enough', label: 'Not long enough' },
      { id: 'too_much_fluff', label: 'Too much fluff' },
      { id: 'cheesy_corny', label: 'Cheesy/corny tone' },
      { id: 'not_enough_enthusiasm', label: 'Not enough enthusiasm' },
      { id: 'too_much_enthusiasm', label: 'Too much enthusiasm' },
      { id: 'not_thought_provoking', label: 'Not thought-provoking enough' },
      { id: 'repetitive', label: 'Repetitive' },
      { id: 'unclear_structure', label: 'Unclear structure' }
    ]
  },
  provider: {
    anthropic: {
      article: [{ id: 'overcautious', label: 'Overly cautious framing' }]
    },
    gemini: {
      article: [{ id: 'overhedging', label: 'Too much hedging' }]
    },
    openai: {
      article: [{ id: 'surface_level', label: 'Too surface-level' }]
    },
    grok: {
      article: [{ id: 'hot_take_bias', label: 'Too hot-take oriented' }]
    }
  }
};

function getProviderFromModelValue(value) {
  const raw = String(value || '').trim();
  if (!raw.includes(':')) return 'anthropic';
  return raw.split(':')[0] || 'anthropic';
}

function getRewriteIssueOptions(target, provider) {
  const base = REWRITE_ISSUES.base[target] || [];
  const extras = REWRITE_ISSUES.provider[provider]?.[target] || [];
  return [...base, ...extras];
}

function rewriteIssueOptionsHtml(target, provider = 'anthropic') {
  return getRewriteIssueOptions(target, provider)
    .map((issue) => `<option value="${issue.id}">${escapeHtml(issue.label)}</option>`)
    .join('');
}

function setHeadlineOptions(card, headlineList) {
  const wrap = card.querySelector('.headline-options');
  if (!wrap) return;
  const options = Array.isArray(headlineList) ? headlineList.filter(Boolean).slice(0, 3) : [];
  if (!options.length) {
    wrap.innerHTML = '';
    return;
  }
  wrap.innerHTML = `
    <p class="draft-meta">Headline options:</p>
    <div class="admin-actions">
      ${options.map((headline, index) => `
        <button type="button" class="btn ${index === 0 ? 'btn-primary' : ''} btn-headline-option" data-headline="${escapeHtml(headline)}">
          ${index === 0 ? 'Use best: ' : 'Use alt: '}${escapeHtml(headline)}
        </button>
      `).join('')}
    </div>
  `;
}

function syncRewriteIssueSelect(card, target) {
  const modelSelect = card.querySelector(`.job-model-rewrite-${target}`);
  const issueSelect = card.querySelector(`.rewrite-issues-${target}`);
  const issueStep = card.querySelector(`.ai-step-issues-${target}`);
  const runStep = card.querySelector(`.ai-step-run-${target}`);
  const runBtn = card.querySelector(`.btn-rewrite-${target}`);
  if (!modelSelect || !issueSelect) return;
  const modelValue = String(modelSelect.value || '').trim();
  const hasModel = modelValue.includes(':');
  if (!hasModel) {
    issueSelect.innerHTML = '<option value="">Select model first</option>';
    issueSelect.disabled = true;
    if (issueStep) issueStep.setAttribute('hidden', '');
    if (runStep) runStep.setAttribute('hidden', '');
    if (runBtn) runBtn.disabled = true;
    return;
  }
  issueSelect.disabled = false;
  if (issueStep) issueStep.removeAttribute('hidden');
  if (runStep) runStep.removeAttribute('hidden');
  const provider = getProviderFromModelValue(modelSelect.value);
  const selected = Array.from(issueSelect.selectedOptions).map((opt) => opt.value);
  issueSelect.innerHTML = rewriteIssueOptionsHtml(target, provider);
  Array.from(issueSelect.options).forEach((opt) => {
    if (selected.includes(opt.value)) opt.selected = true;
  });
  if (runBtn) runBtn.disabled = getSelectedIssues(card, target).length < 1;
}

function enforceIssueLimit(selectEl, max = 3) {
  if (!selectEl) return;
  const selected = Array.from(selectEl.selectedOptions);
  if (selected.length <= max) return;
  selected[selected.length - 1].selected = false;
  setMessage(`Select up to ${max} rewrite issues.`);
}

function getSelectedIssues(card, target) {
  const issueSelect = card.querySelector(`.rewrite-issues-${target}`);
  if (!issueSelect) return [];
  return Array.from(issueSelect.selectedOptions).map((opt) => String(opt.value || '').trim()).filter(Boolean).slice(0, 3);
}

function toggleAiPanel(card, panelClass, anchorButton) {
  if (!card || !panelClass || !anchorButton) return;
  const root = card.querySelector('.draft-form') || card;
  const targetPanel = card.querySelector(`.${panelClass}`);
  if (!targetPanel) return;
  const opening = targetPanel.hasAttribute('hidden') || targetPanel.dataset.anchorClass !== panelClass;
  card.querySelectorAll('.ai-panel').forEach((panel) => panel.setAttribute('hidden', ''));
  if (!opening) return;
  targetPanel.removeAttribute('hidden');
  targetPanel.dataset.anchorClass = panelClass;
  const rootRect = root.getBoundingClientRect();
  const buttonRect = anchorButton.getBoundingClientRect();
  const panelWidth = Math.min(360, Math.max(240, Math.round(root.clientWidth * 0.42)));
  targetPanel.style.width = `${panelWidth}px`;
  const leftRaw = buttonRect.left - rootRect.left;
  const left = Math.max(8, Math.min(leftRaw, root.clientWidth - panelWidth - 8));
  const top = Math.max(8, buttonRect.bottom - rootRect.top + 6);
  targetPanel.style.left = `${left}px`;
  targetPanel.style.top = `${top}px`;
}

function closeAllAiPanels() {
  document.querySelectorAll('.ai-panel').forEach((panel) => panel.setAttribute('hidden', ''));
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
        <div class="headline-options full"></div>
        <div class="full">
          <div class="field-label-row">
            <span>Title</span>
            <span class="inline-tools">
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-headline-gen">Generate headline...</button>
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-headline-rewrite">Rewrite headline...</button>
            </span>
          </div>
          <input class="field-title" type="text" value="${escapeHtml(draft.title || '')}" />
        </div>
        <div class="full ai-panel panel-headline-gen" hidden>
          <label>
            Model
            <select class="job-model-headline-gen">
              ${aiModelSelectHtml()}
            </select>
          </label>
          <button class="btn btn-primary btn-generate-headlines" type="button">Run</button>
        </div>
        <div class="full ai-panel panel-headline-rewrite" hidden>
          <div class="ai-step ai-step-model">
            <label>
              Model
              <select class="job-model-rewrite-headline">
                ${rewriteModelSelectHtml()}
              </select>
            </label>
          </div>
          <div class="ai-step ai-step-issues ai-step-issues-headline" hidden>
            <label>
              Reasons (max 3)
              <select class="rewrite-issues-headline" multiple size="4">
                ${rewriteIssueOptionsHtml('headline', 'anthropic')}
              </select>
            </label>
          </div>
          <div class="ai-step ai-step-run ai-step-run-headline" hidden>
            <button class="btn btn-primary btn-rewrite-headline" type="button" disabled>Run</button>
          </div>
        </div>
        <div class="full">
          <div class="field-label-row">
            <span>Description</span>
            <span class="inline-tools">
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-description-gen">Generate description...</button>
              <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-description-rewrite">Rewrite description...</button>
            </span>
          </div>
          <textarea class="field-description">${escapeHtml(draft.description || '')}</textarea>
        </div>
        <div class="full ai-panel panel-description-gen" hidden>
          <label>
            Model
            <select class="job-model-description">
              ${aiModelSelectHtml()}
            </select>
          </label>
          <button class="btn btn-primary btn-generate-description" type="button">Run</button>
        </div>
        <div class="full ai-panel panel-description-rewrite" hidden>
          <div class="ai-step ai-step-model">
            <label>
              Model
              <select class="job-model-rewrite-description">
                ${rewriteModelSelectHtml()}
              </select>
            </label>
          </div>
          <div class="ai-step ai-step-issues ai-step-issues-description" hidden>
            <label>
              Reasons (max 3)
              <select class="rewrite-issues-description" multiple size="4">
                ${rewriteIssueOptionsHtml('description', 'anthropic')}
              </select>
            </label>
          </div>
          <div class="ai-step ai-step-run ai-step-run-description" hidden>
            <button class="btn btn-primary btn-rewrite-description" type="button" disabled>Run</button>
          </div>
        </div>
        <div class="full">
          <div class="field-label-row">
            <span>Content</span>
          </div>
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
              <span class="rte-toolbar-spacer"></span>
              <span class="inline-tools">
                <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-article-gen">Generate article...</button>
                <button class="btn rte-btn ai-action-toggle" type="button" data-panel="panel-article-rewrite">Rewrite article...</button>
              </span>
            </div>
            <div class="ai-panel ai-panel-inline panel-article-gen" hidden>
              <label>
                Model
                <select class="job-model-article">
                  ${aiModelSelectHtml()}
                </select>
              </label>
              <button class="btn btn-primary btn-generate-article" type="button">Run</button>
            </div>
            <div class="ai-panel ai-panel-inline panel-article-rewrite" hidden>
              <div class="ai-step ai-step-model">
                <label>
                  Model
                  <select class="job-model-rewrite-article">
                    ${rewriteModelSelectHtml()}
                  </select>
                </label>
              </div>
              <div class="ai-step ai-step-issues ai-step-issues-article" hidden>
                <label>
                  Reasons (max 3)
                  <select class="rewrite-issues-article" multiple size="5">
                    ${rewriteIssueOptionsHtml('article', 'anthropic')}
                  </select>
                </label>
              </div>
              <div class="ai-step ai-step-run ai-step-run-article" hidden>
                <button class="btn btn-primary btn-rewrite-article" type="button" disabled>Run</button>
              </div>
            </div>
            <div class="field-content-editor rte-editor" contenteditable="true" role="textbox" aria-multiline="true"></div>
            <textarea class="field-content" hidden>${escapeHtml(draft.content || '')}</textarea>
          </div>
        </div>
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
  draftListEl.querySelectorAll('.draft-card').forEach(hydrateJobModelSelections);
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

function getJobModelSelection(card, selectorClass) {
  const raw = String(card.querySelector(selectorClass)?.value || '').trim();
  const [provider, ...modelParts] = raw.split(':');
  const model = modelParts.join(':').trim();
  if (!provider || !model) {
    throw new Error('Select a model first');
  }
  return { provider: provider.trim(), model };
}

function persistJobModelSelection(card, selectorClass, storageKey) {
  const selected = String(card.querySelector(selectorClass)?.value || '').trim();
  if (selected) localStorage.setItem(storageKey, selected);
}

function hydrateJobModelSelections(card) {
  const headlineGenModel = localStorage.getItem('de_job_model_headline_gen') || '';
  const articleModel = localStorage.getItem('de_job_model_article') || '';
  const descriptionModel = localStorage.getItem('de_job_model_description') || '';
  const rewriteHeadlineModel = localStorage.getItem('de_job_model_rewrite_headline') || '';
  const rewriteArticleModel = localStorage.getItem('de_job_model_rewrite_article') || '';
  const rewriteDescriptionModel = localStorage.getItem('de_job_model_rewrite_description') || '';
  if (headlineGenModel) {
    const select = card.querySelector('.job-model-headline-gen');
    if (select) select.value = headlineGenModel;
  }
  if (articleModel) {
    const select = card.querySelector('.job-model-article');
    if (select) select.value = articleModel;
  }
  if (descriptionModel) {
    const select = card.querySelector('.job-model-description');
    if (select) select.value = descriptionModel;
  }
  if (rewriteHeadlineModel) {
    const select = card.querySelector('.job-model-rewrite-headline');
    if (select) select.value = rewriteHeadlineModel;
  }
  if (rewriteArticleModel) {
    const select = card.querySelector('.job-model-rewrite-article');
    if (select) select.value = rewriteArticleModel;
  }
  if (rewriteDescriptionModel) {
    const select = card.querySelector('.job-model-rewrite-description');
    if (select) select.value = rewriteDescriptionModel;
  }
  syncRewriteIssueSelect(card, 'headline');
  syncRewriteIssueSelect(card, 'article');
  syncRewriteIssueSelect(card, 'description');
}

async function generateArticleForDraft(card) {
  const title = String(card.querySelector('.field-title')?.value || '').trim();
  const section = String(card.querySelector('.field-section')?.value || 'local').trim();
  const sourceTitle = String(card.querySelector('.draft-meta a')?.textContent || '').trim();
  const sourceUrl = String(card.querySelector('.draft-meta a')?.getAttribute('href') || '').trim();
  if (!title) throw new Error('Title is required before generating article');
  const { provider, model } = getJobModelSelection(card, '.job-model-article');
  persistJobModelSelection(card, '.job-model-article', 'de_job_model_article');

  const data = await apiRequest('/api/admin-generate-article', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title, section, provider, model, sourceTitle, sourceUrl })
  });

  const description = String(data?.article?.description || '').trim();
  const content = String(data?.article?.content || '').trim();
  if (!content) throw new Error('Generation returned empty article content');

  const descriptionField = card.querySelector('.field-description');
  const contentEditor = card.querySelector('.field-content-editor');
  const contentTextarea = card.querySelector('.field-content');
  if (descriptionField && description) descriptionField.value = description;
  if (contentEditor) contentEditor.innerHTML = content;
  if (contentTextarea) contentTextarea.value = content;
}

async function generateDescriptionForDraft(card) {
  const title = String(card.querySelector('.field-title')?.value || '').trim();
  const content = getCardContentHtml(card);
  const section = String(card.querySelector('.field-section')?.value || 'local').trim();
  if (!title) throw new Error('Title is required before generating description');
  if (!String(content || '').trim()) throw new Error('Content is required before generating description');
  const { provider, model } = getJobModelSelection(card, '.job-model-description');
  persistJobModelSelection(card, '.job-model-description', 'de_job_model_description');

  const data = await apiRequest('/api/admin-generate-description', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title, content, section, provider, model })
  });

  const description = String(data?.description || '').trim();
  if (!description) throw new Error('Generation returned empty description');
  const descriptionField = card.querySelector('.field-description');
  if (descriptionField) descriptionField.value = description;
}

async function generateHeadlinesForDraft(card) {
  const topic = String(card.querySelector('.field-title')?.value || '').trim();
  const section = String(card.querySelector('.field-section')?.value || 'local').trim();
  const { provider, model } = getJobModelSelection(card, '.job-model-headline-gen');
  persistJobModelSelection(card, '.job-model-headline-gen', 'de_job_model_headline_gen');
  const data = await apiRequest('/api/admin-generate-headlines', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ topic, title: topic, section, provider, model })
  });
  const best = String(data?.bestHeadline || '').trim();
  const alternates = Array.isArray(data?.alternates) ? data.alternates.map((v) => String(v || '').trim()).filter(Boolean) : [];
  const titleField = card.querySelector('.field-title');
  if (titleField && best) titleField.value = best;
  setHeadlineOptions(card, [best, ...alternates]);
}

async function rewriteDraftContent(card, target) {
  const title = String(card.querySelector('.field-title')?.value || '').trim();
  const description = String(card.querySelector('.field-description')?.value || '').trim();
  const content = getCardContentHtml(card);
  const issues = getSelectedIssues(card, target);
  if (issues.length < 1 || issues.length > 3) throw new Error('Select 1 to 3 rewrite issues');
  const { provider, model } = getJobModelSelection(card, `.job-model-rewrite-${target}`);
  persistJobModelSelection(card, `.job-model-rewrite-${target}`, `de_job_model_rewrite_${target}`);

  const data = await apiRequest('/api/admin-rewrite-content', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ target, title, description, content, issues, provider, model })
  });

  if (target === 'headline') {
    const nextTitle = String(data?.headline || '').trim();
    if (!nextTitle) throw new Error('Rewrite returned empty headline');
    const titleField = card.querySelector('.field-title');
    if (titleField) titleField.value = nextTitle;
    return;
  }
  if (target === 'description') {
    const nextDescription = String(data?.description || '').trim();
    if (!nextDescription) throw new Error('Rewrite returned empty description');
    const descriptionField = card.querySelector('.field-description');
    if (descriptionField) descriptionField.value = nextDescription;
    return;
  }
  const nextContent = String(data?.content || '').trim();
  if (!nextContent) throw new Error('Rewrite returned empty article content');
  const contentEditor = card.querySelector('.field-content-editor');
  const contentTextarea = card.querySelector('.field-content');
  if (contentEditor) contentEditor.innerHTML = nextContent;
  if (contentTextarea) contentTextarea.value = nextContent;
}

function onDraftListClick(event) {
  const button = event.target.closest('button');
  if (!button) return;

  const card = event.target.closest('.draft-card');
  if (!card) return;

  if (button.classList.contains('ai-action-toggle')) {
    toggleAiPanel(card, String(button.dataset.panel || ''), button);
    return;
  }

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

  if (button.classList.contains('btn-generate-article')) {
    generateArticleForDraft(card)
      .then(() => setMessage(`Draft #${card.dataset.id} article generated.`))
      .catch((err) => setMessage(`Generate article failed: ${err.message}`));
  }

  if (button.classList.contains('btn-generate-headlines')) {
    generateHeadlinesForDraft(card)
      .then(() => setMessage(`Draft #${card.dataset.id} headline options generated.`))
      .catch((err) => setMessage(`Generate headlines failed: ${err.message}`));
  }

  if (button.classList.contains('btn-generate-description')) {
    generateDescriptionForDraft(card)
      .then(() => setMessage(`Draft #${card.dataset.id} description generated.`))
      .catch((err) => setMessage(`Generate description failed: ${err.message}`));
  }

  if (button.classList.contains('btn-rewrite-headline')) {
    rewriteDraftContent(card, 'headline')
      .then(() => setMessage(`Draft #${card.dataset.id} headline rewritten.`))
      .catch((err) => setMessage(`Rewrite headline failed: ${err.message}`));
  }

  if (button.classList.contains('btn-rewrite-description')) {
    rewriteDraftContent(card, 'description')
      .then(() => setMessage(`Draft #${card.dataset.id} description rewritten.`))
      .catch((err) => setMessage(`Rewrite description failed: ${err.message}`));
  }

  if (button.classList.contains('btn-rewrite-article')) {
    rewriteDraftContent(card, 'article')
      .then(() => setMessage(`Draft #${card.dataset.id} article rewritten.`))
      .catch((err) => setMessage(`Rewrite article failed: ${err.message}`));
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

  if (button.classList.contains('btn-headline-option')) {
    const titleField = card.querySelector('.field-title');
    const headline = String(button.dataset.headline || '').trim();
    if (titleField && headline) titleField.value = headline;
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
  if (input.classList.contains('job-model-headline-gen')) {
    localStorage.setItem('de_job_model_headline_gen', String(input.value || ''));
    return;
  }
  if (input.classList.contains('job-model-article')) {
    localStorage.setItem('de_job_model_article', String(input.value || ''));
    return;
  }
  if (input.classList.contains('job-model-description')) {
    localStorage.setItem('de_job_model_description', String(input.value || ''));
    return;
  }
  if (input.classList.contains('job-model-rewrite-headline')) {
    localStorage.setItem('de_job_model_rewrite_headline', String(input.value || ''));
    const card = input.closest('.draft-card');
    if (card) syncRewriteIssueSelect(card, 'headline');
    return;
  }
  if (input.classList.contains('job-model-rewrite-article')) {
    localStorage.setItem('de_job_model_rewrite_article', String(input.value || ''));
    const card = input.closest('.draft-card');
    if (card) syncRewriteIssueSelect(card, 'article');
    return;
  }
  if (input.classList.contains('job-model-rewrite-description')) {
    localStorage.setItem('de_job_model_rewrite_description', String(input.value || ''));
    const card = input.closest('.draft-card');
    if (card) syncRewriteIssueSelect(card, 'description');
    return;
  }
  if (input.classList.contains('rewrite-issues-headline') || input.classList.contains('rewrite-issues-article') || input.classList.contains('rewrite-issues-description')) {
    enforceIssueLimit(input, 3);
    const card = input.closest('.draft-card');
    if (card && input.classList.contains('rewrite-issues-headline')) {
      const runBtn = card.querySelector('.btn-rewrite-headline');
      if (runBtn) runBtn.disabled = getSelectedIssues(card, 'headline').length < 1;
    }
    if (card && input.classList.contains('rewrite-issues-description')) {
      const runBtn = card.querySelector('.btn-rewrite-description');
      if (runBtn) runBtn.disabled = getSelectedIssues(card, 'description').length < 1;
    }
    if (card && input.classList.contains('rewrite-issues-article')) {
      const runBtn = card.querySelector('.btn-rewrite-article');
      if (runBtn) runBtn.disabled = getSelectedIssues(card, 'article').length < 1;
    }
    return;
  }
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
document.addEventListener('click', (event) => {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  if (target.closest('.ai-panel') || target.closest('.ai-action-toggle')) return;
  closeAllAiPanels();
});

loadStoredToken();
setLockState(sessionStorage.getItem('de_admin_unlocked') === '1');
if (adminUiUnlocked) {
  loadUsageDashboard().catch((err) => setMessage(`Usage load failed: ${err.message}`));
  loadManualUsageSummary().catch(() => {});
}
