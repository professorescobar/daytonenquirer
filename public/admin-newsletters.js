const lockSection = document.getElementById('newsletter-lock');
const appSection = document.getElementById('newsletter-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');

const tokenInput = document.getElementById('admin-token');
const saveTokenBtn = document.getElementById('save-token-btn');
const newCampaignBtn = document.getElementById('new-campaign-btn');
const saveCampaignBtn = document.getElementById('save-campaign-btn');
const sendCampaignBtn = document.getElementById('send-campaign-btn');
const refreshStatusBtn = document.getElementById('refresh-status-btn');
const loadCampaignsBtn = document.getElementById('load-campaigns-btn');
const messageEl = document.getElementById('newsletter-message');

const campaignIdInput = document.getElementById('campaign-id');
const campaignStatusInput = document.getElementById('campaign-status');
const campaignTitleInput = document.getElementById('campaign-title');
const campaignSubjectInput = document.getElementById('campaign-subject');
const campaignPreviewTextInput = document.getElementById('campaign-preview-text');
const campaignDescriptionInput = document.getElementById('campaign-description');
const campaignSegmentIdsInput = document.getElementById('campaign-segment-ids');
const campaignTagIdsInput = document.getElementById('campaign-tag-ids');
const campaignContentHtmlInput = document.getElementById('campaign-content-html');
const campaignContentTextInput = document.getElementById('campaign-content-text');
const campaignStatusFilterInput = document.getElementById('campaign-status-filter');
const campaignListLimitInput = document.getElementById('campaign-list-limit');
const campaignListEl = document.getElementById('campaign-list');

let unlocked = false;
let selectedCampaignId = null;

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
  return String(tokenInput.value || '').trim();
}

function getCampaignId() {
  const value = Number(campaignIdInput.value || selectedCampaignId);
  return Number.isInteger(value) && value > 0 ? value : null;
}

function formatDate(value) {
  if (!value) return 'n/a';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString();
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function toCsv(value) {
  if (!Array.isArray(value)) return '';
  return value.map((item) => Number(item)).filter((n) => Number.isInteger(n) && n > 0).join(',');
}

function fromCsv(value) {
  return String(value || '')
    .split(',')
    .map((item) => Number(item.trim()))
    .filter((n) => Number.isInteger(n) && n > 0);
}

function clearComposer() {
  selectedCampaignId = null;
  campaignIdInput.value = '';
  campaignStatusInput.value = 'draft';
  campaignTitleInput.value = '';
  campaignSubjectInput.value = '';
  campaignPreviewTextInput.value = '';
  campaignDescriptionInput.value = '';
  campaignSegmentIdsInput.value = '';
  campaignTagIdsInput.value = '';
  campaignContentHtmlInput.value = '';
  campaignContentTextInput.value = '';
}

function applyCampaign(campaign) {
  if (!campaign) return;
  selectedCampaignId = campaign.id;
  campaignIdInput.value = campaign.id;
  campaignStatusInput.value = campaign.status || 'draft';
  campaignTitleInput.value = campaign.title || '';
  campaignSubjectInput.value = campaign.subject || '';
  campaignPreviewTextInput.value = campaign.previewText || '';
  campaignDescriptionInput.value = campaign.description || '';
  campaignSegmentIdsInput.value = toCsv(campaign.segmentIds);
  campaignTagIdsInput.value = toCsv(campaign.tagIds);
  campaignContentHtmlInput.value = campaign.contentHtml || '';
  campaignContentTextInput.value = campaign.contentText || '';
}

function getComposerPayload() {
  return {
    title: String(campaignTitleInput.value || '').trim(),
    subject: String(campaignSubjectInput.value || '').trim(),
    previewText: String(campaignPreviewTextInput.value || '').trim(),
    description: String(campaignDescriptionInput.value || '').trim(),
    segmentIds: fromCsv(campaignSegmentIdsInput.value),
    tagIds: fromCsv(campaignTagIdsInput.value),
    contentHtml: campaignContentHtmlInput.value || '',
    contentText: campaignContentTextInput.value || ''
  };
}

async function apiRequest(url, options = {}) {
  if (!unlocked) throw new Error('Page is locked');
  const token = getToken();
  if (!token) throw new Error('Missing admin token');

  const res = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'x-admin-token': token,
      ...(options.headers || {})
    }
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `Request failed (${res.status})`);
  }
  return data;
}

async function unlock() {
  try {
    const password = String(adminUiPasswordInput.value || '').trim();
    if (!password) throw new Error('Enter admin UI password');
    const res = await fetch('/api/admin-ui-auth', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'Unlock failed');

    sessionStorage.setItem('de_admin_unlocked_newsletters', '1');
    setLockState(true);
    setMessage('Newsletters unlocked.');
    if (getToken()) await loadCampaigns();
  } catch (error) {
    setMessage(`Unlock failed: ${error.message}`);
  }
}

async function saveCampaign() {
  const id = getCampaignId();
  const payload = getComposerPayload();
  const body = id ? JSON.stringify({ id, ...payload }) : JSON.stringify(payload);
  const method = id ? 'PATCH' : 'POST';
  const data = await apiRequest('/api/admin-newsletters', { method, body });
  applyCampaign(data.campaign);
  return data.campaign;
}

async function sendCampaign() {
  const campaign = await saveCampaign();
  const id = campaign.id;
  const data = await apiRequest('/api/admin-newsletter-send', {
    method: 'POST',
    body: JSON.stringify({ id })
  });
  applyCampaign(data.campaign);
  return data.campaign;
}

async function refreshCampaignStatus() {
  const id = getCampaignId();
  if (!id) throw new Error('Select or save a campaign first');
  const data = await apiRequest(`/api/admin-newsletter-status?id=${id}&refresh=true`);
  applyCampaign(data.campaign);
  return data.campaign;
}

function renderCampaignList(campaigns) {
  if (!Array.isArray(campaigns) || campaigns.length === 0) {
    campaignListEl.innerHTML = '<p>No campaigns found.</p>';
    return;
  }

  campaignListEl.innerHTML = campaigns.map((campaign) => `
    <article class="draft-card" data-campaign-id="${campaign.id}">
      <button class="draft-header btn-reset campaign-select-btn" type="button">
        <strong>#${campaign.id} - ${escapeHtml(campaign.title || '(untitled)')}</strong>
        <span class="draft-meta">
          status: ${escapeHtml(campaign.status || 'draft')} |
          subject: ${escapeHtml(campaign.subject || 'n/a')} |
          updated: ${escapeHtml(formatDate(campaign.updatedAt))}
        </span>
      </button>
    </article>
  `).join('');
}

async function loadCampaigns() {
  const status = encodeURIComponent(String(campaignStatusFilterInput.value || 'all'));
  const limit = Math.min(Math.max(Number(campaignListLimitInput.value || 25), 1), 200);
  const data = await apiRequest(`/api/admin-newsletters?status=${status}&limit=${limit}`);
  renderCampaignList(data.campaigns);
}

campaignListEl.addEventListener('click', async (event) => {
  const button = event.target.closest('.campaign-select-btn');
  if (!button) return;
  const card = button.closest('[data-campaign-id]');
  const id = Number(card?.dataset?.campaignId);
  if (!Number.isInteger(id) || id <= 0) return;
  try {
    const data = await apiRequest(`/api/admin-newsletter-status?id=${id}`);
    applyCampaign(data.campaign);
    setMessage(`Loaded campaign #${id}.`);
  } catch (error) {
    setMessage(`Load failed: ${error.message}`);
  }
});

unlockAdminBtn.addEventListener('click', unlock);
adminUiPasswordInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') unlock();
});

saveTokenBtn.addEventListener('click', () => {
  localStorage.setItem('de_admin_token', getToken());
  setMessage('Admin token saved.');
});

newCampaignBtn.addEventListener('click', () => {
  clearComposer();
  setMessage('Ready for a new campaign draft.');
});

saveCampaignBtn.addEventListener('click', async () => {
  try {
    const campaign = await saveCampaign();
    setMessage(`Saved campaign #${campaign.id}.`);
    await loadCampaigns();
  } catch (error) {
    setMessage(`Save failed: ${error.message}`);
  }
});

sendCampaignBtn.addEventListener('click', async () => {
  try {
    const campaign = await sendCampaign();
    setMessage(`Campaign #${campaign.id} queued in Kit.`);
    await loadCampaigns();
  } catch (error) {
    setMessage(`Send failed: ${error.message}`);
  }
});

refreshStatusBtn.addEventListener('click', async () => {
  try {
    const campaign = await refreshCampaignStatus();
    setMessage(`Status refreshed: ${campaign.status} (${campaign.kitStatus || 'n/a'}).`);
    await loadCampaigns();
  } catch (error) {
    setMessage(`Refresh failed: ${error.message}`);
  }
});

loadCampaignsBtn.addEventListener('click', async () => {
  try {
    await loadCampaigns();
    setMessage('Campaign list loaded.');
  } catch (error) {
    setMessage(`Load failed: ${error.message}`);
  }
});

tokenInput.value = localStorage.getItem('de_admin_token') || '';
setLockState(sessionStorage.getItem('de_admin_unlocked_newsletters') === '1');
if (unlocked && getToken()) {
  loadCampaigns().catch((error) => setMessage(`Initial load failed: ${error.message}`));
}
