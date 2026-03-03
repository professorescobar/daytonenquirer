const lockSection = document.getElementById('admin-lock');
const appSection = document.getElementById('admin-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');
const tokenInput = document.getElementById('admin-token');
const saveTokenBtn = document.getElementById('save-token-btn');
const messageEl = document.getElementById('admin-message');

const uploadFileInput = document.getElementById('upload-file');
const uploadSectionInput = document.getElementById('upload-section');
const uploadBeatInput = document.getElementById('upload-beat');
const uploadPersonaInput = document.getElementById('upload-persona');
const uploadTitleInput = document.getElementById('upload-title');
const uploadDescriptionInput = document.getElementById('upload-description');
const uploadTagsInput = document.getElementById('upload-tags');
const uploadEntitiesInput = document.getElementById('upload-entities');
const uploadToneInput = document.getElementById('upload-tone');
const uploadCreditInput = document.getElementById('upload-credit');
const uploadLicenseTypeInput = document.getElementById('upload-license-type');
const uploadLicenseUrlInput = document.getElementById('upload-license-url');
const uploadApprovedInput = document.getElementById('upload-approved');
const uploadSaveBtn = document.getElementById('upload-save-btn');
const uploadAutotagBtn = document.getElementById('upload-autotag-btn');
const uploadStatusFeedEl = document.getElementById('upload-status-feed');

const filterSectionInput = document.getElementById('filter-section');
const filterApprovedInput = document.getElementById('filter-approved');
const filterPersonaInput = document.getElementById('filter-persona');
const filterBeatInput = document.getElementById('filter-beat');
const filterQInput = document.getElementById('filter-q');
const filterLimitInput = document.getElementById('filter-limit');
const loadImagesBtn = document.getElementById('load-images-btn');
const imagesListEl = document.getElementById('images-list');

const CLOUDINARY_CLOUD_NAME = 'dtlkzlp87';
const CLOUDINARY_UPLOAD_PRESET = 'dayton-enquirer';
const CLOUDINARY_WIDTH = 1600;

const BEAT_OPTIONS_BY_SECTION = {
  local: ['general-local'],
  national: ['general-national'],
  world: ['general-world'],
  business: ['general-business'],
  sports: ['general-sports'],
  health: ['general-health'],
  entertainment: ['gaming'],
  technology: ['general-technology']
};

const PERSONA_OPTIONS_BY_BEAT = {
  gaming: ['Tsuki Tamara']
};

let adminUiUnlocked = false;
let uploadDraftAsset = null;

function getToken() {
  return String(tokenInput?.value || '').trim();
}

function setMessage(text) {
  if (!messageEl) return;
  messageEl.hidden = !text;
  messageEl.textContent = text || '';
}

function setUploadStatus(text) {
  if (!uploadStatusFeedEl) return;
  uploadStatusFeedEl.textContent = text || '';
}

function setLockState(unlocked) {
  adminUiUnlocked = unlocked;
  if (lockSection) lockSection.hidden = unlocked;
  if (appSection) appSection.hidden = !unlocked;
}

function escapeHtml(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function parseCsv(text) {
  return String(text || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

function setSelectOptions(selectEl, values, fallbackLabel) {
  if (!selectEl) return;
  const options = (values || []).filter(Boolean);
  if (!options.length && fallbackLabel) {
    selectEl.innerHTML = `<option value="${escapeHtml(fallbackLabel)}">${escapeHtml(fallbackLabel)}</option>`;
    return;
  }
  selectEl.innerHTML = options.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join('');
}

function getBeatsForSection(section) {
  return BEAT_OPTIONS_BY_SECTION[section] || ['general'];
}

function getPersonasForBeat(beat) {
  return PERSONA_OPTIONS_BY_BEAT[beat] || ['General Desk'];
}

function syncUploadBeatOptions() {
  const section = String(uploadSectionInput?.value || 'entertainment').trim();
  const prev = String(uploadBeatInput?.value || '').trim();
  const beats = getBeatsForSection(section);
  setSelectOptions(uploadBeatInput, beats, 'general');
  if (prev && beats.includes(prev)) uploadBeatInput.value = prev;
}

function syncUploadPersonaOptions() {
  const beat = String(uploadBeatInput?.value || '').trim();
  const prev = String(uploadPersonaInput?.value || '').trim();
  const personas = getPersonasForBeat(beat);
  setSelectOptions(uploadPersonaInput, personas, 'General Desk');
  if (prev && personas.includes(prev)) uploadPersonaInput.value = prev;
}

function setMetadataLoading(loading) {
  const targets = document.querySelectorAll('.metadata-target');
  targets.forEach((el) => {
    if (loading) el.classList.add('is-loading');
    else el.classList.remove('is-loading');
    el.querySelectorAll('input, textarea, select').forEach((field) => {
      field.disabled = loading;
    });
  });
  if (uploadSaveBtn) uploadSaveBtn.disabled = loading;
  if (uploadAutotagBtn) uploadAutotagBtn.disabled = loading;
}

function toCsv(value) {
  if (!Array.isArray(value)) return '';
  return value.map((v) => String(v || '').trim()).filter(Boolean).join(', ');
}

function toBool(value, fallback = false) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return fallback;
  if (['true', '1', 'yes'].includes(raw)) return true;
  if (['false', '0', 'no'].includes(raw)) return false;
  return fallback;
}

function optimizedCloudinaryUrl(publicId) {
  const safeId = encodeURIComponent(publicId || '').replace(/%2F/g, '/');
  return `https://res.cloudinary.com/${CLOUDINARY_CLOUD_NAME}/image/upload/f_auto,q_auto,c_limit,w_${CLOUDINARY_WIDTH}/${safeId}`;
}

async function uploadImageToCloudinary(file) {
  if (!file) throw new Error('Select an image file first.');
  const form = new FormData();
  form.append('file', file);
  form.append('upload_preset', CLOUDINARY_UPLOAD_PRESET);

  const res = await fetch(
    `https://api.cloudinary.com/v1_1/${CLOUDINARY_CLOUD_NAME}/image/upload`,
    { method: 'POST', body: form }
  );
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.public_id) {
    throw new Error(data.error?.message || 'Cloudinary upload failed');
  }

  return {
    imageUrl: optimizedCloudinaryUrl(data.public_id),
    imagePublicId: data.public_id
  };
}

async function resolveUploadAsset() {
  if (uploadDraftAsset?.imageUrl && uploadDraftAsset?.imagePublicId) return uploadDraftAsset;
  const file = uploadFileInput?.files?.[0];
  if (!file) throw new Error('Select an image file first.');
  if (!String(file.type || '').startsWith('image/')) throw new Error('Please choose an image file.');
  uploadDraftAsset = await uploadImageToCloudinary(file);
  return uploadDraftAsset;
}

async function apiRequest(url, options = {}) {
  if (!adminUiUnlocked) throw new Error('Admin UI is locked');
  const token = getToken();
  if (!token) throw new Error('Missing admin token');

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

async function unlockAdminUi() {
  try {
    setMessage('');
    const password = String(adminUiPasswordInput?.value || '').trim();
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
    if (!res.ok) throw new Error(data.error || 'Unlock failed');

    sessionStorage.setItem('de_admin_unlocked', '1');
    setLockState(true);
    setMessage('Admin unlocked.');
  } catch (error) {
    setMessage(`Unlock failed: ${error.message}`);
  }
}

function collectUploadPayload(uploaded) {
  return {
    section: uploadSectionInput?.value || 'entertainment',
    beat: uploadBeatInput?.value || '',
    persona: uploadPersonaInput?.value || '',
    title: uploadTitleInput?.value || '',
    description: uploadDescriptionInput?.value || '',
    tags: parseCsv(uploadTagsInput?.value || ''),
    entities: parseCsv(uploadEntitiesInput?.value || ''),
    tone: uploadToneInput?.value || '',
    credit: uploadCreditInput?.value || '',
    licenseType: uploadLicenseTypeInput?.value || '',
    licenseSourceUrl: uploadLicenseUrlInput?.value || '',
    approved: toBool(uploadApprovedInput?.value || 'false', false),
    imageUrl: uploaded.imageUrl,
    imagePublicId: uploaded.imagePublicId
  };
}

async function createImageRecord() {
  uploadSaveBtn.disabled = true;
  try {
    setMessage('Preparing upload...');
    const uploaded = await resolveUploadAsset();
    setMessage('Saving metadata...');
    await apiRequest('/api/admin-images', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(collectUploadPayload(uploaded))
    });

    setMessage('Image saved to media library.');
    setUploadStatus('');
    uploadFileInput.value = '';
    uploadDraftAsset = null;
    await loadImages();
  } catch (error) {
    setMessage(`Upload failed: ${error.message}`);
  } finally {
    uploadSaveBtn.disabled = false;
  }
}

function applyAutotagMetadata(meta) {
  if (!meta) return;
  if (!String(uploadTitleInput?.value || '').trim() && meta.title) uploadTitleInput.value = meta.title;
  if (!String(uploadDescriptionInput?.value || '').trim() && meta.description) uploadDescriptionInput.value = meta.description;
  if (!String(uploadTagsInput?.value || '').trim() && Array.isArray(meta.tags) && meta.tags.length) uploadTagsInput.value = toCsv(meta.tags);
  if (!String(uploadEntitiesInput?.value || '').trim() && Array.isArray(meta.entities) && meta.entities.length) uploadEntitiesInput.value = toCsv(meta.entities);
  if (!String(uploadToneInput?.value || '').trim() && meta.tone) uploadToneInput.value = meta.tone;
}

function countNewlyFilled(before, after) {
  return Object.keys(after).filter((key) => !before[key] && after[key]).length;
}

async function autotagUploadFields() {
  try {
    setUploadStatus('');
    setMetadataLoading(true);
    setMessage('Uploading image for tagging...');
    const asset = await resolveUploadAsset();
    setMessage('Generating metadata with Gemini...');
    setUploadStatus('Generating metadata with Gemini...');
    const result = await apiRequest('/api/admin-images-autotag', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        imageUrl: asset.imageUrl,
        section: uploadSectionInput?.value || 'entertainment',
        beat: uploadBeatInput?.value || '',
        persona: uploadPersonaInput?.value || '',
        title: uploadTitleInput?.value || '',
        description: uploadDescriptionInput?.value || '',
        tone: uploadToneInput?.value || '',
        tags: parseCsv(uploadTagsInput?.value || ''),
        entities: parseCsv(uploadEntitiesInput?.value || '')
      })
    });
    const before = {
      title: String(uploadTitleInput?.value || '').trim(),
      description: String(uploadDescriptionInput?.value || '').trim(),
      tags: String(uploadTagsInput?.value || '').trim(),
      entities: String(uploadEntitiesInput?.value || '').trim(),
      tone: String(uploadToneInput?.value || '').trim()
    };
    applyAutotagMetadata(result.metadata || null);
    const after = {
      title: String(uploadTitleInput?.value || '').trim(),
      description: String(uploadDescriptionInput?.value || '').trim(),
      tags: String(uploadTagsInput?.value || '').trim(),
      entities: String(uploadEntitiesInput?.value || '').trim(),
      tone: String(uploadToneInput?.value || '').trim()
    };
    const filledCount = countNewlyFilled(before, after);
    setMessage(`Metadata generated with ${result.model || 'Gemini'} (${filledCount} field${filledCount === 1 ? '' : 's'} filled).`);
    setUploadStatus(
      filledCount
        ? 'Metadata ready. Review and click Upload + Save.'
        : 'No blank metadata fields were filled. Existing values were preserved.'
    );
  } catch (error) {
    setMessage(`Auto-tag failed: ${error.message}`);
    setUploadStatus('');
  } finally {
    setMetadataLoading(false);
  }
}

function renderImageCard(image) {
  const card = document.createElement('article');
  card.className = 'image-library-card';
  card.dataset.id = String(image.id || '');
  card.innerHTML = `
    <img src="${escapeHtml(image.imageUrl)}" alt="${escapeHtml(image.title || 'Library image')}" loading="lazy" />
    <div class="image-library-fields">
      <label>
        Section
        <input class="field-section" type="text" value="${escapeHtml(image.section || '')}" />
      </label>
      <label>
        Beat
        <input class="field-beat" type="text" value="${escapeHtml(image.beat || '')}" />
      </label>
      <label>
        Persona
        <input class="field-persona" type="text" value="${escapeHtml(image.persona || '')}" />
      </label>
      <label class="full">
        Title
        <input class="field-title" type="text" value="${escapeHtml(image.title || '')}" />
      </label>
      <label class="full">
        Description
        <textarea class="field-description">${escapeHtml(image.description || '')}</textarea>
      </label>
      <label class="full">
        Tags (comma list)
        <input class="field-tags" type="text" value="${escapeHtml(toCsv(image.tags))}" />
      </label>
      <label class="full">
        Entities (comma list)
        <input class="field-entities" type="text" value="${escapeHtml(toCsv(image.entities))}" />
      </label>
      <label>
        Tone
        <input class="field-tone" type="text" value="${escapeHtml(image.tone || '')}" />
      </label>
      <label>
        Credit
        <input class="field-credit" type="text" value="${escapeHtml(image.credit || '')}" />
      </label>
      <label>
        License Type
        <input class="field-license-type" type="text" value="${escapeHtml(image.licenseType || '')}" />
      </label>
      <label class="full">
        License URL
        <input class="field-license-url" type="url" value="${escapeHtml(image.licenseSourceUrl || '')}" />
      </label>
      <label>
        Approved
        <select class="field-approved">
          <option value="true" ${image.approved ? 'selected' : ''}>true</option>
          <option value="false" ${!image.approved ? 'selected' : ''}>false</option>
        </select>
      </label>
      <label class="full">
        Image URL
        <input class="field-image-url" type="url" value="${escapeHtml(image.imageUrl || '')}" />
      </label>
      <label class="full">
        Cloudinary Public ID
        <input class="field-image-public-id" type="text" value="${escapeHtml(image.imagePublicId || '')}" />
      </label>
    </div>
    <div class="admin-actions">
      <button class="btn btn-autotag-image" type="button">Auto-tag (Gemini)</button>
      <button class="btn btn-save-image" type="button">Save</button>
      <button class="btn btn-danger btn-delete-image" type="button">Delete</button>
    </div>
  `;
  return card;
}

function collectCardPayload(card) {
  return {
    id: Number(card.dataset.id || 0),
    section: card.querySelector('.field-section')?.value || '',
    beat: card.querySelector('.field-beat')?.value || '',
    persona: card.querySelector('.field-persona')?.value || '',
    title: card.querySelector('.field-title')?.value || '',
    description: card.querySelector('.field-description')?.value || '',
    tags: parseCsv(card.querySelector('.field-tags')?.value || ''),
    entities: parseCsv(card.querySelector('.field-entities')?.value || ''),
    tone: card.querySelector('.field-tone')?.value || '',
    credit: card.querySelector('.field-credit')?.value || '',
    licenseType: card.querySelector('.field-license-type')?.value || '',
    licenseSourceUrl: card.querySelector('.field-license-url')?.value || '',
    approved: toBool(card.querySelector('.field-approved')?.value || 'false', false),
    imageUrl: card.querySelector('.field-image-url')?.value || '',
    imagePublicId: card.querySelector('.field-image-public-id')?.value || ''
  };
}

async function saveImageCard(card) {
  const payload = collectCardPayload(card);
  await apiRequest('/api/admin-images', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
}

async function autotagImageCard(card) {
  const imageUrl = String(card.querySelector('.field-image-url')?.value || '').trim();
  if (!imageUrl) throw new Error('Missing image URL');
  const data = await apiRequest('/api/admin-images-autotag', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      imageUrl,
      section: card.querySelector('.field-section')?.value || '',
      beat: card.querySelector('.field-beat')?.value || '',
      persona: card.querySelector('.field-persona')?.value || '',
      title: card.querySelector('.field-title')?.value || '',
      description: card.querySelector('.field-description')?.value || '',
      tone: card.querySelector('.field-tone')?.value || '',
      tags: parseCsv(card.querySelector('.field-tags')?.value || ''),
      entities: parseCsv(card.querySelector('.field-entities')?.value || ''),
      overwrite: false
    })
  });
  return data?.metadata || null;
}

function applyMetadataToCardBlanks(card, meta) {
  if (!card || !meta) return 0;
  let filled = 0;
  const titleField = card.querySelector('.field-title');
  const descriptionField = card.querySelector('.field-description');
  const tagsField = card.querySelector('.field-tags');
  const entitiesField = card.querySelector('.field-entities');
  const toneField = card.querySelector('.field-tone');

  if (titleField && !String(titleField.value || '').trim() && meta.title) {
    titleField.value = meta.title;
    filled += 1;
  }
  if (descriptionField && !String(descriptionField.value || '').trim() && meta.description) {
    descriptionField.value = meta.description;
    filled += 1;
  }
  if (tagsField && !String(tagsField.value || '').trim() && Array.isArray(meta.tags) && meta.tags.length) {
    tagsField.value = toCsv(meta.tags);
    filled += 1;
  }
  if (entitiesField && !String(entitiesField.value || '').trim() && Array.isArray(meta.entities) && meta.entities.length) {
    entitiesField.value = toCsv(meta.entities);
    filled += 1;
  }
  if (toneField && !String(toneField.value || '').trim() && meta.tone) {
    toneField.value = meta.tone;
    filled += 1;
  }
  return filled;
}

async function deleteImageCard(card) {
  const id = Number(card.dataset.id || 0);
  if (!id) return;
  await apiRequest(`/api/admin-images?id=${encodeURIComponent(id)}`, { method: 'DELETE' });
  card.remove();
}

async function loadImages() {
  try {
    setMessage('Loading images...');
    const params = new URLSearchParams();
    if (filterSectionInput?.value) params.set('section', filterSectionInput.value);
    if (filterApprovedInput?.value) params.set('approved', filterApprovedInput.value);
    if (filterPersonaInput?.value.trim()) params.set('persona', filterPersonaInput.value.trim());
    if (filterBeatInput?.value.trim()) params.set('beat', filterBeatInput.value.trim());
    if (filterQInput?.value.trim()) params.set('q', filterQInput.value.trim());
    if (filterLimitInput?.value) params.set('limit', filterLimitInput.value);

    const query = params.toString();
    const data = await apiRequest(`/api/admin-images${query ? `?${query}` : ''}`);
    const images = Array.isArray(data.images) ? data.images : [];

    imagesListEl.innerHTML = '';
    images.forEach((image) => imagesListEl.appendChild(renderImageCard(image)));
    setMessage(`Loaded ${images.length} image${images.length === 1 ? '' : 's'}.`);
  } catch (error) {
    setMessage(`Load failed: ${error.message}`);
  }
}

async function onImagesListClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const card = target.closest('.image-library-card');
  if (!card) return;

  if (target.classList.contains('btn-save-image')) {
    target.setAttribute('disabled', 'true');
    try {
      await saveImageCard(card);
      setMessage('Image metadata saved.');
    } catch (error) {
      setMessage(`Save failed: ${error.message}`);
    } finally {
      target.removeAttribute('disabled');
    }
    return;
  }

  if (target.classList.contains('btn-autotag-image')) {
    target.setAttribute('disabled', 'true');
    try {
      setMessage('Running Gemini auto-tag...');
      const metadata = await autotagImageCard(card);
      const filledCount = applyMetadataToCardBlanks(card, metadata);
      if (!filledCount) {
        setMessage('No blank fields were filled. Existing values were preserved.');
      } else {
        setMessage(`Gemini metadata applied (${filledCount} field${filledCount === 1 ? '' : 's'} filled). Click Save to persist.`);
      }
    } catch (error) {
      setMessage(`Auto-tag failed: ${error.message}`);
    } finally {
      target.removeAttribute('disabled');
    }
    return;
  }

  if (target.classList.contains('btn-delete-image')) {
    const ok = window.confirm('Permanently delete this image record?');
    if (!ok) return;
    target.setAttribute('disabled', 'true');
    try {
      await deleteImageCard(card);
      setMessage('Image record deleted.');
    } catch (error) {
      setMessage(`Delete failed: ${error.message}`);
      target.removeAttribute('disabled');
    }
  }
}

function saveToken() {
  const token = getToken();
  if (!token) {
    setMessage('Enter an admin token first.');
    return;
  }
  localStorage.setItem('de_admin_token', token);
  setMessage('Admin token saved locally.');
}

function init() {
  const savedToken = localStorage.getItem('de_admin_token');
  if (savedToken && tokenInput) tokenInput.value = savedToken;

  setLockState(sessionStorage.getItem('de_admin_unlocked') === '1');
  if (adminUiUnlocked) setMessage('Admin unlocked.');

  unlockAdminBtn?.addEventListener('click', unlockAdminUi);
  adminUiPasswordInput?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') unlockAdminUi();
  });
  saveTokenBtn?.addEventListener('click', saveToken);
  uploadSaveBtn?.addEventListener('click', createImageRecord);
  uploadAutotagBtn?.addEventListener('click', autotagUploadFields);
  uploadSectionInput?.addEventListener('change', () => {
    syncUploadBeatOptions();
    syncUploadPersonaOptions();
  });
  uploadBeatInput?.addEventListener('change', syncUploadPersonaOptions);
  uploadFileInput?.addEventListener('change', () => {
    uploadDraftAsset = null;
    setUploadStatus('');
  });
  loadImagesBtn?.addEventListener('click', loadImages);
  imagesListEl?.addEventListener('click', onImagesListClick);

  syncUploadBeatOptions();
  syncUploadPersonaOptions();
}

init();
