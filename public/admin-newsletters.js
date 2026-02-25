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
const campaignEditorToolbar = document.getElementById('campaign-editor-toolbar');
const campaignContentEditor = document.getElementById('campaign-content-editor');
const campaignContentTextInput = document.getElementById('campaign-content-text');
const campaignStatusFilterInput = document.getElementById('campaign-status-filter');
const campaignListLimitInput = document.getElementById('campaign-list-limit');
const campaignListEl = document.getElementById('campaign-list');
const newsletterArticleSectionInput = document.getElementById('newsletter-article-section');
const newsletterArticleLimitInput = document.getElementById('newsletter-article-limit');
const newsletterArticleSearchInput = document.getElementById('newsletter-article-search');
const loadNewsletterArticlesBtn = document.getElementById('load-newsletter-articles-btn');
const clearNewsletterSelectionBtn = document.getElementById('clear-newsletter-selection-btn');
const buildNewsletterTemplateBtn = document.getElementById('build-newsletter-template-btn');
const newsletterArticleResultsEl = document.getElementById('newsletter-article-results');
const newsletterSelectedLeadEl = document.getElementById('newsletter-selected-lead');
const newsletterSelectedStoriesEl = document.getElementById('newsletter-selected-stories');

let unlocked = false;
let selectedCampaignId = null;
let availableArticles = [];
let selectedLeadArticle = null;
let selectedStoryArticles = [];
const HOMEPAGE_SECTION_ORDER = [
  'local',
  'national',
  'world',
  'business',
  'sports',
  'health',
  'entertainment',
  'technology'
];

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

function htmlToText(value) {
  return String(value || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function summarizeText(value, maxLength = 180) {
  const text = htmlToText(value || '');
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 3).trim()}...`;
}

function formatSection(value) {
  const section = String(value || '').trim().toLowerCase();
  if (!section) return 'general';
  return section.charAt(0).toUpperCase() + section.slice(1);
}

function getSectionRank(section) {
  const key = String(section || '').trim().toLowerCase();
  const index = HOMEPAGE_SECTION_ORDER.indexOf(key);
  return index >= 0 ? index : HOMEPAGE_SECTION_ORDER.length;
}

function getSectionOrderedStories() {
  return selectedStoryArticles
    .map((article, index) => ({ article, index }))
    .sort((a, b) => {
      const rankDiff = getSectionRank(a.article.section) - getSectionRank(b.article.section);
      if (rankDiff !== 0) return rankDiff;
      return a.index - b.index;
    })
    .map((entry) => entry.article);
}

function getArticleUrl(article) {
  const slug = String(article?.slug || '').trim();
  if (!slug) return window.location.origin;
  return new URL(`article.html?slug=${encodeURIComponent(slug)}`, window.location.origin).toString();
}

function getArticleById(id) {
  return availableArticles.find((article) => Number(article.id) === Number(id)) || null;
}

function textToHtml(value) {
  const trimmed = String(value || '').trim();
  if (!trimmed) return '';
  return trimmed
    .split(/\n{2,}/)
    .map((paragraph) => `<p>${escapeHtml(paragraph).replace(/\n/g, '<br>')}</p>`)
    .join('');
}

function getEditorHtml() {
  if (!campaignContentEditor) return '';
  const html = String(campaignContentEditor.innerHTML || '').trim();
  return html === '<br>' ? '' : html;
}

function getEditorText() {
  if (!campaignContentEditor) return String(campaignContentTextInput.value || '').trim();
  return String(campaignContentEditor.innerText || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function syncHiddenTextInput() {
  campaignContentTextInput.value = getEditorText();
}

function setEditorHtml(value) {
  if (!campaignContentEditor) {
    campaignContentTextInput.value = htmlToText(value);
    return;
  }
  campaignContentEditor.innerHTML = String(value || '').trim();
  syncHiddenTextInput();
}

function execEditorCommand(command, value = null) {
  if (!campaignContentEditor) return;
  campaignContentEditor.focus();
  document.execCommand(command, false, value);
  syncHiddenTextInput();
}

function clearStorySelection() {
  selectedLeadArticle = null;
  selectedStoryArticles = [];
  renderSelectedStories();
  renderAvailableArticles();
}

function renderSelectedStories() {
  if (!newsletterSelectedLeadEl || !newsletterSelectedStoriesEl) return;
  const orderedStories = getSectionOrderedStories();

  if (!selectedLeadArticle) {
    newsletterSelectedLeadEl.innerHTML = '<p class="hint">No lead story selected.</p>';
  } else {
    newsletterSelectedLeadEl.innerHTML = `
      <article class="draft-card">
        <div class="newsletter-story-card-body">
          <strong>${escapeHtml(selectedLeadArticle.title || '(untitled)')}</strong>
          <span class="draft-meta">${escapeHtml(formatSection(selectedLeadArticle.section))} | ${escapeHtml(formatDate(selectedLeadArticle.pubDate))}</span>
          <div class="newsletter-story-actions">
            <button type="button" class="btn" data-selected-action="remove-lead">Remove Lead</button>
          </div>
        </div>
      </article>
    `;
  }

  if (orderedStories.length === 0) {
    newsletterSelectedStoriesEl.innerHTML = '<p class="hint">No stories selected yet.</p>';
  } else {
    newsletterSelectedStoriesEl.innerHTML = orderedStories.map((article, index) => `
      <article class="draft-card" data-selected-story-id="${article.id}">
        <div class="newsletter-story-card-body">
          <strong>${index + 1}. ${escapeHtml(article.title || '(untitled)')}</strong>
          <span class="draft-meta">${escapeHtml(formatSection(article.section))} | ${escapeHtml(formatDate(article.pubDate))}</span>
          <div class="newsletter-story-actions">
            <button type="button" class="btn" data-selected-action="promote-lead">Set Lead</button>
            <button type="button" class="btn" data-selected-action="remove-story">Remove</button>
          </div>
        </div>
      </article>
    `).join('');
  }
}

function renderAvailableArticles() {
  if (!newsletterArticleResultsEl) return;
  const query = String(newsletterArticleSearchInput?.value || '').trim().toLowerCase();
  const selectedStoryIds = new Set(selectedStoryArticles.map((article) => Number(article.id)));
  const leadId = Number(selectedLeadArticle?.id || 0);

  const filtered = availableArticles.filter((article) => {
    if (!query) return true;
    const haystack = [
      article.title,
      article.description,
      article.section
    ].map((value) => String(value || '').toLowerCase()).join(' ');
    return haystack.includes(query);
  });

  if (filtered.length === 0) {
    newsletterArticleResultsEl.innerHTML = '<p class="hint">No articles match your filters yet.</p>';
    return;
  }

  newsletterArticleResultsEl.innerHTML = filtered.map((article) => {
    const id = Number(article.id);
    const inLead = id === leadId;
    const inStories = selectedStoryIds.has(id);
    return `
      <article class="draft-card" data-article-id="${id}">
        <div class="newsletter-story-card-body">
          <strong>${escapeHtml(article.title || '(untitled)')}</strong>
          <span class="draft-meta">${escapeHtml(formatSection(article.section))} | ${escapeHtml(formatDate(article.pubDate))}</span>
          <p class="newsletter-story-summary">${escapeHtml(summarizeText(article.description || article.content || '', 180))}</p>
          <div class="newsletter-story-actions">
            <button type="button" class="btn" data-article-action="add-lead"${inLead ? ' disabled' : ''}>${inLead ? 'Lead Selected' : 'Use as Lead'}</button>
            <button type="button" class="btn" data-article-action="add-story"${inStories ? ' disabled' : ''}>${inStories ? 'Added' : 'Add Story'}</button>
          </div>
        </div>
      </article>
    `;
  }).join('');
}

function buildNewsletterMarkup() {
  const candidates = [];
  const orderedStories = getSectionOrderedStories();
  if (selectedLeadArticle) candidates.push(selectedLeadArticle);
  orderedStories.forEach((article) => {
    if (!candidates.some((item) => Number(item.id) === Number(article.id))) {
      candidates.push(article);
    }
  });

  if (candidates.length === 0) {
    throw new Error('Select at least one article to build the newsletter template');
  }

  const lead = candidates[0];
  const stories = candidates.slice(1);
  const heading = String(campaignTitleInput.value || '').trim() || 'The Dayton Enquirer Weekly Brief';
  const intro = String(campaignPreviewTextInput.value || '').trim() || 'Top Dayton stories this week.';

  const leadUrl = getArticleUrl(lead);
  const leadImage = String(lead.image || '').trim();
  const leadDescription = summarizeText(lead.description || lead.content || '', 260);

  const storyRows = stories.map((article, index) => {
    const url = getArticleUrl(article);
    const description = summarizeText(article.description || article.content || '', 160);
    return `
      <tr>
        <td style="padding:0 0 16px 0;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
            <tr>
              <td style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#666666;padding:0 0 4px 0;">${index + 1}. ${escapeHtml(formatSection(article.section))}</td>
            </tr>
            <tr>
              <td style="font-family:Arial,Helvetica,sans-serif;font-size:18px;line-height:1.35;font-weight:700;padding:0 0 6px 0;">
                <a href="${escapeHtml(url)}" style="color:#0b3d91;text-decoration:none;">${escapeHtml(article.title || '(untitled)')}</a>
              </td>
            </tr>
            <tr>
              <td style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.5;color:#222222;">${escapeHtml(description)}</td>
            </tr>
          </table>
        </td>
      </tr>
    `;
  }).join('');

  const html = `
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:700px;margin:0 auto;border-collapse:collapse;background:#ffffff;">
      <tr>
        <td style="padding:20px 20px 8px 20px;border-bottom:2px solid #111111;font-family:Arial,Helvetica,sans-serif;">
          <div style="font-size:24px;font-weight:800;line-height:1.2;color:#111111;">${escapeHtml(heading)}</div>
          <div style="margin-top:8px;font-size:14px;line-height:1.5;color:#444444;">${escapeHtml(intro)}</div>
        </td>
      </tr>
      <tr>
        <td style="padding:18px 20px 0 20px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
            <tr>
              <td style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#666666;padding:0 0 4px 0;">Lead Story • ${escapeHtml(formatSection(lead.section))}</td>
            </tr>
            <tr>
              <td style="font-family:Arial,Helvetica,sans-serif;font-size:26px;line-height:1.25;font-weight:800;padding:0 0 10px 0;">
                <a href="${escapeHtml(leadUrl)}" style="color:#111111;text-decoration:none;">${escapeHtml(lead.title || '(untitled)')}</a>
              </td>
            </tr>
            ${leadImage ? `
            <tr>
              <td style="padding:0 0 10px 0;">
                <a href="${escapeHtml(leadUrl)}"><img src="${escapeHtml(leadImage)}" alt="${escapeHtml(lead.title || 'Lead story image')}" style="display:block;width:100%;height:auto;border:0;" /></a>
              </td>
            </tr>` : ''}
            <tr>
              <td style="font-family:Arial,Helvetica,sans-serif;font-size:15px;line-height:1.6;color:#222222;padding:0 0 16px 0;">${escapeHtml(leadDescription)}</td>
            </tr>
          </table>
        </td>
      </tr>
      ${stories.length > 0 ? `
      <tr>
        <td style="padding:10px 20px 2px 20px;font-family:Arial,Helvetica,sans-serif;font-size:20px;line-height:1.3;font-weight:700;color:#111111;border-top:1px solid #e5e5e5;">More Stories</td>
      </tr>
      <tr>
        <td style="padding:10px 20px 12px 20px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
            ${storyRows}
          </table>
        </td>
      </tr>` : ''}
    </table>
  `.trim();

  const textLines = [
    heading,
    '',
    intro,
    '',
    `Lead Story: ${lead.title || '(untitled)'}`,
    leadUrl,
    leadDescription,
    ''
  ];
  if (stories.length > 0) {
    textLines.push('More Stories:');
    stories.forEach((article, index) => {
      textLines.push(`${index + 1}. ${article.title || '(untitled)'}`);
      textLines.push(getArticleUrl(article));
      const description = summarizeText(article.description || article.content || '', 180);
      if (description) textLines.push(description);
      textLines.push('');
    });
  }

  return {
    html,
    text: textLines.join('\n').trim()
  };
}

async function loadAvailableArticles() {
  const section = encodeURIComponent(String(newsletterArticleSectionInput?.value || 'all'));
  const limit = Math.min(Math.max(Number(newsletterArticleLimitInput?.value || 120), 10), 300);
  const data = await apiRequest(`/api/admin-articles?section=${section}&limit=${limit}`);
  availableArticles = Array.isArray(data.articles)
    ? data.articles.filter((article) => String(article.status || '').toLowerCase() === 'published')
    : [];
  renderAvailableArticles();
  setMessage(`Loaded ${availableArticles.length} published article(s) for story picking.`);
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
  setEditorHtml('');
  campaignContentTextInput.value = '';
  clearStorySelection();
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
  const html = String(campaign.contentHtml || '').trim();
  if (html) {
    setEditorHtml(html);
  } else {
    setEditorHtml(textToHtml(campaign.contentText || ''));
  }
  clearStorySelection();
}

function getComposerPayload() {
  const contentHtml = getEditorHtml();
  const contentText = getEditorText();
  campaignContentTextInput.value = contentText;

  return {
    title: String(campaignTitleInput.value || '').trim(),
    subject: String(campaignSubjectInput.value || '').trim(),
    previewText: String(campaignPreviewTextInput.value || '').trim(),
    description: String(campaignDescriptionInput.value || '').trim(),
    segmentIds: fromCsv(campaignSegmentIdsInput.value),
    tagIds: fromCsv(campaignTagIdsInput.value),
    contentHtml,
    contentText
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

loadNewsletterArticlesBtn?.addEventListener('click', async () => {
  try {
    await loadAvailableArticles();
  } catch (error) {
    setMessage(`Load articles failed: ${error.message}`);
  }
});

newsletterArticleSearchInput?.addEventListener('input', () => {
  renderAvailableArticles();
});

clearNewsletterSelectionBtn?.addEventListener('click', () => {
  clearStorySelection();
  setMessage('Story picker selection cleared.');
});

buildNewsletterTemplateBtn?.addEventListener('click', () => {
  try {
    const { html, text } = buildNewsletterMarkup();
    setEditorHtml(html);
    campaignContentTextInput.value = text;
    setMessage('Newsletter content rebuilt from selected stories.');
  } catch (error) {
    setMessage(`Build failed: ${error.message}`);
  }
});

newsletterArticleResultsEl?.addEventListener('click', (event) => {
  const button = event.target.closest('button[data-article-action]');
  if (!button) return;
  const action = String(button.dataset.articleAction || '');
  const card = button.closest('[data-article-id]');
  const id = Number(card?.dataset?.articleId);
  const article = getArticleById(id);
  if (!article) return;

  if (action === 'add-lead') {
    selectedLeadArticle = article;
    selectedStoryArticles = selectedStoryArticles.filter((item) => Number(item.id) !== Number(article.id));
    renderSelectedStories();
    renderAvailableArticles();
    return;
  }

  if (action === 'add-story') {
    if (selectedLeadArticle && Number(selectedLeadArticle.id) === Number(article.id)) return;
    if (selectedStoryArticles.some((item) => Number(item.id) === Number(article.id))) return;
    selectedStoryArticles.push(article);
    renderSelectedStories();
    renderAvailableArticles();
  }
});

newsletterSelectedLeadEl?.addEventListener('click', (event) => {
  const button = event.target.closest('button[data-selected-action]');
  if (!button) return;
  const action = String(button.dataset.selectedAction || '');
  if (action === 'remove-lead') {
    selectedLeadArticle = null;
    renderSelectedStories();
    renderAvailableArticles();
  }
});

newsletterSelectedStoriesEl?.addEventListener('click', (event) => {
  const button = event.target.closest('button[data-selected-action]');
  if (!button) return;
  const action = String(button.dataset.selectedAction || '');
  const card = button.closest('[data-selected-story-id]');
  const id = Number(card?.dataset?.selectedStoryId);
  const index = selectedStoryArticles.findIndex((article) => Number(article.id) === id);
  if (index < 0) return;

  if (action === 'remove-story') {
    selectedStoryArticles.splice(index, 1);
  } else if (action === 'promote-lead') {
    const promoted = selectedStoryArticles.splice(index, 1)[0];
    if (selectedLeadArticle) {
      selectedStoryArticles.unshift(selectedLeadArticle);
    }
    selectedLeadArticle = promoted;
  } else {
    return;
  }

  renderSelectedStories();
  renderAvailableArticles();
});

campaignEditorToolbar?.addEventListener('click', (event) => {
  const button = event.target.closest('button');
  if (!button) return;

  const action = String(button.dataset.action || '');
  const command = String(button.dataset.cmd || '');
  const value = button.dataset.value || null;

  if (action === 'link') {
    const rawUrl = window.prompt('Enter URL');
    if (rawUrl == null) return;
    const url = String(rawUrl).trim();
    if (!url) return;
    execEditorCommand('createLink', url);
    return;
  }

  if (command) {
    execEditorCommand(command, value);
  }
});

campaignContentEditor?.addEventListener('input', syncHiddenTextInput);
campaignContentEditor?.addEventListener('blur', syncHiddenTextInput);

tokenInput.value = localStorage.getItem('de_admin_token') || '';
setLockState(sessionStorage.getItem('de_admin_unlocked_newsletters') === '1');
setEditorHtml(textToHtml(campaignContentTextInput.value || ''));
renderSelectedStories();
renderAvailableArticles();
if (unlocked && getToken()) {
  loadCampaigns().catch((error) => setMessage(`Initial load failed: ${error.message}`));
}
