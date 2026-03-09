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
const campaignPreviewFrame = document.getElementById('newsletter-preview-frame');
const campaignContentHtmlInput = document.getElementById('campaign-content-html');
const campaignContentTextInput = document.getElementById('campaign-content-text');
const campaignStatusFilterInput = document.getElementById('campaign-status-filter');
const campaignListLimitInput = document.getElementById('campaign-list-limit');
const campaignListEl = document.getElementById('campaign-list');
const newsletterArticleSectionInput = document.getElementById('newsletter-article-section');
const newsletterArticleSearchInput = document.getElementById('newsletter-article-search');
const newsletterPagePrevBtn = document.getElementById('newsletter-page-prev');
const newsletterPageNextBtn = document.getElementById('newsletter-page-next');
const newsletterPageLabelEl = document.getElementById('newsletter-page-label');
const newsletterArticleResultsEl = document.getElementById('newsletter-article-results');

let unlocked = false;
let selectedCampaignId = null;
let availableArticles = [];
let selectedLeadArticle = null;
let selectedStoryArticles = [];
let articlePage = 1;
const ARTICLE_PAGE_SIZE = 9;
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

function getExcerptSource(article) {
  const description = htmlToText(article?.description || '');
  const content = htmlToText(article?.content || '');
  if (content.length > description.length) return content;
  return description || content;
}

function formatSection(value) {
  const section = String(value || '').trim().toLowerCase();
  if (!section) return 'general';
  return section.charAt(0).toUpperCase() + section.slice(1);
}

function getSectionDisplayName(section) {
  const key = String(section || '').trim().toLowerCase();
  const labels = {
    local: 'Local News',
    national: 'National News',
    world: 'World News',
    business: 'Business',
    sports: 'Sports',
    health: 'Health',
    entertainment: 'Entertainment',
    technology: 'Technology'
  };
  return labels[key] || formatSection(key || 'General');
}

function getSectionPageUrl(section) {
  const key = String(section || '').trim().toLowerCase();
  if (!key) return window.location.origin;
  return new URL(`section.html?s=${encodeURIComponent(key)}`, window.location.origin).toString();
}

function getEmailDate(value) {
  if (!value) return '';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '';
  return parsed.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
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

function normalizeImageStatus(article) {
  const raw = String(article?.imageStatus || article?.renderClass || '').trim().toLowerCase();
  if (raw === 'with_image' || raw === 'text_only') return raw;
  return '';
}

function normalizePlacementEligible(article) {
  if (!Array.isArray(article?.placementEligible)) return [];
  return article.placementEligible
    .map((slot) => String(slot || '').trim().toLowerCase())
    .filter(Boolean);
}

function hasAnyPlacement(article, slots) {
  const eligible = normalizePlacementEligible(article);
  if (!eligible.length) return false;
  return slots.some((slot) => eligible.includes(slot));
}

function canUseForFeaturedImage(article) {
  return normalizeImageStatus(article) === 'with_image'
    && Boolean(String(article?.image || '').trim())
    && hasAnyPlacement(article, ['main', 'top', 'grid']);
}

function canUseInNewsletter(article) {
  const status = normalizeImageStatus(article);
  if (!status) return false;
  if (!hasAnyPlacement(article, ['main', 'top', 'grid', 'sidebar', 'extra_headlines'])) return false;
  if (status === 'with_image') return Boolean(String(article?.image || '').trim());
  return true;
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

function renderNewsletterPreview(html) {
  if (!campaignPreviewFrame) return;
  const bodyContent = String(html || '').trim()
    || '<div style="padding:20px;font-family:Arial,Helvetica,sans-serif;color:#555;">Build a newsletter from selected stories to preview it here.</div>';
  const doc = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { margin: 0; padding: 16px; background: #f1f3f6; }
  </style>
</head>
<body>${bodyContent}</body>
</html>`;
  campaignPreviewFrame.srcdoc = doc;
}

function setCampaignContent(html, text = '') {
  const normalizedHtml = String(html || '').trim();
  const normalizedText = String(text || '').trim() || htmlToText(normalizedHtml);
  campaignContentHtmlInput.value = normalizedHtml;
  campaignContentTextInput.value = normalizedText;
  renderNewsletterPreview(normalizedHtml);
}

function getCampaignContent() {
  const html = String(campaignContentHtmlInput.value || '').trim();
  const text = String(campaignContentTextInput.value || '').trim() || htmlToText(html);
  return { html, text };
}

function clearStorySelection(options = {}) {
  selectedLeadArticle = null;
  selectedStoryArticles = [];
  if (options.clearContent) {
    setCampaignContent('', '');
  }
  articlePage = 1;
  renderAvailableArticles();
}

function getFilteredArticles() {
  const query = String(newsletterArticleSearchInput?.value || '').trim().toLowerCase();
  const section = String(newsletterArticleSectionInput?.value || 'all').toLowerCase();
  return availableArticles.filter((article) => {
    if (section !== 'all' && String(article.section || '').toLowerCase() !== section) {
      return false;
    }
    if (!query) return true;
    const haystack = [article.title, article.description, article.section]
      .map((value) => String(value || '').toLowerCase())
      .join(' ');
    return haystack.includes(query);
  });
}

function refreshNewsletterFromSelection() {
  const hasSelection = !!selectedLeadArticle || selectedStoryArticles.length > 0;
  if (!hasSelection) {
    setCampaignContent('', '');
    return;
  }
  const { html, text } = buildNewsletterMarkup();
  setCampaignContent(html, text);
}

function renderAvailableArticles() {
  if (!newsletterArticleResultsEl) return;
  const filtered = getFilteredArticles();
  const totalPages = Math.max(1, Math.ceil(filtered.length / ARTICLE_PAGE_SIZE));
  articlePage = Math.min(Math.max(articlePage, 1), totalPages);

  const start = (articlePage - 1) * ARTICLE_PAGE_SIZE;
  const pageArticles = filtered.slice(start, start + ARTICLE_PAGE_SIZE);
  const selectedStoryIds = new Set(selectedStoryArticles.map((article) => Number(article.id)));
  const leadId = Number(selectedLeadArticle?.id || 0);

  if (pageArticles.length === 0) {
    newsletterArticleResultsEl.innerHTML = '<p class="hint">No recent stories found for this filter.</p>';
  } else {
    newsletterArticleResultsEl.innerHTML = pageArticles.map((article) => {
      const id = Number(article.id);
      const inLead = id === leadId;
      const inStories = selectedStoryIds.has(id);
      const image = String(article.image || '').trim();
      const badge = inLead ? 'Lead' : (inStories ? 'Selected' : '');
      return `
        <article class="draft-card newsletter-story-card ${inLead || inStories ? 'is-selected' : ''}" data-article-id="${id}">
          ${badge ? `<span class="newsletter-story-badge">${escapeHtml(badge)}</span>` : ''}
          ${image
            ? `<img class="newsletter-story-image" src="${escapeHtml(image)}" alt="${escapeHtml(article.title || 'Article image')}" loading="lazy" />`
            : '<div class="newsletter-story-image newsletter-story-image-empty">No image</div>'}
          <div class="newsletter-story-card-body">
            <strong>${escapeHtml(article.title || '(untitled)')}</strong>
            <span class="draft-meta">${escapeHtml(formatDate(article.pubDate))}</span>
            <div class="newsletter-story-actions">
              <button type="button" class="btn" data-article-action="set-lead">${inLead ? 'Lead Selected' : 'Set Lead'}</button>
              <button type="button" class="btn" data-article-action="toggle-story">${inLead || inStories ? 'Remove' : 'Add'}</button>
            </div>
          </div>
        </article>
      `;
    }).join('');
  }

  if (newsletterPageLabelEl) {
    newsletterPageLabelEl.textContent = `Page ${articlePage} of ${totalPages}`;
  }
  if (newsletterPagePrevBtn) newsletterPagePrevBtn.disabled = articlePage <= 1;
  if (newsletterPageNextBtn) newsletterPageNextBtn.disabled = articlePage >= totalPages;
}

function buildNewsletterMarkup() {
  const orderedStories = getSectionOrderedStories();
  const selected = [];
  if (selectedLeadArticle) selected.push(selectedLeadArticle);
  orderedStories.forEach((article) => {
    if (!selected.some((item) => Number(item.id) === Number(article.id))) {
      selected.push(article);
    }
  });

  if (selected.length === 0) {
    throw new Error('Select at least one article to build the newsletter template');
  }

  const sections = new Map();
  selected.forEach((article) => {
    const key = String(article.section || 'general').toLowerCase();
    if (!sections.has(key)) sections.set(key, []);
    sections.get(key).push(article);
  });
  const orderedSectionKeys = [
    ...HOMEPAGE_SECTION_ORDER.filter((key) => sections.has(key)),
    ...Array.from(sections.keys()).filter((key) => !HOMEPAGE_SECTION_ORDER.includes(key))
  ];

  const lead = selected[0];
  const heading = String(campaignTitleInput.value || '').trim() || 'The Dayton Enquirer Weekly Brief';
  const intro = String(campaignPreviewTextInput.value || '').trim() || 'Top Dayton stories this week.';
  const leadDescription = summarizeText(getExcerptSource(lead), 1260);

  const sectionBlocks = orderedSectionKeys.map((sectionKey) => {
    const sectionStories = sections.get(sectionKey) || [];
    if (sectionStories.length === 0) return '';
    const featured = sectionStories.find((story) => canUseForFeaturedImage(story)) || sectionStories[0];
    const sidebar = sectionStories.filter((story) => Number(story.id) !== Number(featured.id));
    const featuredUrl = getArticleUrl(featured);
    const featuredImage = canUseForFeaturedImage(featured) ? String(featured.image || '').trim() : '';
    const featuredDate = getEmailDate(featured.pubDate || featured.updatedAt || featured.createdAt);
    const sectionTitle = getSectionDisplayName(sectionKey);
    const sectionUrl = getSectionPageUrl(sectionKey);

    const sidebarItems = sidebar.map((item) => {
      const itemUrl = getArticleUrl(item);
      const itemDate = getEmailDate(item.pubDate || item.updatedAt || item.createdAt);
      return `
        <tr>
          <td style="padding:0 0 10px 0;font-family:Georgia,'Times New Roman',serif;font-size:15px;line-height:1.35;">
            <span style="color:#111111;font-weight:700;">• </span><a href="${escapeHtml(itemUrl)}" style="color:#111111;text-decoration:none;">${escapeHtml(item.title || '(untitled)')}</a>
            ${itemDate ? `<div style="margin-top:2px;font-family:Arial,Helvetica,sans-serif;font-size:11px;color:#666666;">${escapeHtml(itemDate)}</div>` : ''}
          </td>
        </tr>
      `;
    }).join('');

    return `
      <tr>
        <td style="padding:0 0 18px 0;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #dddddd;background:#ffffff;">
            <tr>
              <td style="padding:14px 16px 8px 16px;font-family:Georgia,'Times New Roman',serif;font-size:32px;line-height:1.1;font-weight:700;border-bottom:2px solid #000000;">
                <a href="${escapeHtml(sectionUrl)}" style="color:#111111;text-decoration:none;">${escapeHtml(sectionTitle)}</a>
              </td>
            </tr>
            <tr>
              <td style="padding:14px 16px 14px 16px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                  <tr>
                    <td valign="top" style="width:${sidebar.length > 0 ? '62%' : '100%'};padding:0 ${sidebar.length > 0 ? '14px 0 0' : '0'};">
                      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                        ${featuredImage ? `
                        <tr>
                          <td style="padding:0 0 10px 0;">
                            <a href="${escapeHtml(featuredUrl)}"><img src="${escapeHtml(featuredImage)}" alt="${escapeHtml(featured.title || 'Story image')}" style="display:block;width:100%;height:220px;object-fit:cover;border:0;border-radius:4px;" /></a>
                          </td>
                        </tr>` : ''}
                        <tr>
                          <td style="padding:0 0 6px 0;font-family:Georgia,'Times New Roman',serif;font-size:28px;line-height:1.18;font-weight:700;color:#111111;">
                            <a href="${escapeHtml(featuredUrl)}" style="color:#111111;text-decoration:none;">${escapeHtml(featured.title || '(untitled)')}</a>
                          </td>
                        </tr>
                        ${featuredDate ? `
                        <tr>
                          <td style="padding:0 0 8px 0;font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#666666;">${escapeHtml(featuredDate)}</td>
                        </tr>` : ''}
                        <tr>
                          <td style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.55;color:#222222;">
                            ${escapeHtml(summarizeText(getExcerptSource(featured), 1950))}
                            <br><br>
                            <a href="${escapeHtml(featuredUrl)}" style="font-family:Arial,Helvetica,sans-serif;font-size:13px;font-weight:700;color:#0b3d91;text-decoration:underline;">Read full article here...</a>
                          </td>
                        </tr>
                      </table>
                    </td>
                    ${sidebar.length > 0 ? `
                    <td valign="top" style="width:38%;padding:0;">
                      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                        ${sidebarItems}
                      </table>
                    </td>` : ''}
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    `;
  }).join('');

  const html = `
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:1120px;margin:0 auto;border-collapse:collapse;background:#f8f8f8;">
      <tr>
        <td style="padding:16px 18px 8px 18px;text-align:center;border-bottom:2px solid #dddddd;background:#ffffff;">
          <div style="font-family:Georgia,'Times New Roman',serif;font-size:46px;line-height:1.05;font-weight:700;color:#000000;">The Dayton Enquirer</div>
        </td>
      </tr>
      <tr>
        <td style="padding:14px 18px 8px 18px;font-family:Arial,Helvetica,sans-serif;">
          <div style="font-size:24px;font-weight:800;line-height:1.2;color:#111111;">${escapeHtml(heading)}</div>
          <div style="margin-top:6px;font-size:14px;line-height:1.5;color:#444444;">${escapeHtml(intro)}</div>
        </td>
      </tr>
      <tr>
        <td style="padding:0 18px 0 18px;font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#555555;">
          Lead: <a href="${escapeHtml(getArticleUrl(lead))}" style="color:#111111;text-decoration:none;font-weight:700;">${escapeHtml(lead.title || '(untitled)')}</a>
          ${leadDescription ? `<span style="display:block;margin-top:4px;color:#666666;">${escapeHtml(leadDescription)}</span>` : ''}
        </td>
      </tr>
      <tr>
        <td style="padding:12px 18px 14px 18px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
            ${sectionBlocks}
          </table>
        </td>
      </tr>
    </table>
  `.trim();

  const textLines = [
    'The Dayton Enquirer',
    '',
    heading,
    '',
    intro,
    '',
    `Lead Story: ${lead.title || '(untitled)'}`,
    getArticleUrl(lead),
    leadDescription,
    ''
  ];
  orderedSectionKeys.forEach((sectionKey) => {
    const sectionStories = sections.get(sectionKey) || [];
    if (sectionStories.length === 0) return;
    textLines.push(`${getSectionDisplayName(sectionKey)}:`);
    sectionStories.forEach((article, index) => {
      textLines.push(`${index + 1}. ${article.title || '(untitled)'}`);
      textLines.push(getArticleUrl(article));
      const description = summarizeText(getExcerptSource(article), 960);
      if (description) textLines.push(description);
      textLines.push('');
    });
  });

  return {
    html,
    text: textLines.join('\n').trim()
  };
}

async function loadAvailableArticles() {
  const section = encodeURIComponent(String(newsletterArticleSectionInput?.value || 'all'));
  const data = await apiRequest(`/api/admin-articles?section=${section}&limit=5000`);
  const weekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
  availableArticles = Array.isArray(data.articles) ? data.articles
    .filter((article) => String(article.status || '').toLowerCase() === 'published')
    .filter((article) => canUseInNewsletter(article))
    .filter((article) => {
      const ts = new Date(article.pubDate || article.updatedAt || article.createdAt).getTime();
      return Number.isFinite(ts) && ts >= weekAgo;
    })
    .sort((a, b) => {
      const at = new Date(a.pubDate || a.updatedAt || a.createdAt).getTime();
      const bt = new Date(b.pubDate || b.updatedAt || b.createdAt).getTime();
      return bt - at;
    })
    : [];
  articlePage = 1;
  renderAvailableArticles();
  setMessage(`Loaded ${availableArticles.length} published stories from the last 7 days.`);
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
  setCampaignContent('', '');
  clearStorySelection({ clearContent: true });
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
    setCampaignContent(html, campaign.contentText || '');
  } else {
    setCampaignContent(textToHtml(campaign.contentText || ''), campaign.contentText || '');
  }
  clearStorySelection();
}

function getComposerPayload() {
  const content = getCampaignContent();

  return {
    title: String(campaignTitleInput.value || '').trim(),
    subject: String(campaignSubjectInput.value || '').trim(),
    previewText: String(campaignPreviewTextInput.value || '').trim(),
    description: String(campaignDescriptionInput.value || '').trim(),
    segmentIds: fromCsv(campaignSegmentIdsInput.value),
    tagIds: fromCsv(campaignTagIdsInput.value),
    contentHtml: content.html,
    contentText: content.text
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
    if (getToken()) {
      await loadCampaigns();
      await loadAvailableArticles();
    }
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
  if (unlocked && getToken()) {
    loadAvailableArticles().catch((error) => setMessage(`Story load failed: ${error.message}`));
  }
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

newsletterArticleSearchInput?.addEventListener('input', () => {
  articlePage = 1;
  renderAvailableArticles();
});

newsletterArticleSectionInput?.addEventListener('change', async () => {
  try {
    await loadAvailableArticles();
  } catch (error) {
    setMessage(`Load articles failed: ${error.message}`);
  }
});

newsletterPagePrevBtn?.addEventListener('click', () => {
  articlePage -= 1;
  renderAvailableArticles();
});

newsletterPageNextBtn?.addEventListener('click', () => {
  articlePage += 1;
  renderAvailableArticles();
});

newsletterArticleResultsEl?.addEventListener('click', (event) => {
  const button = event.target.closest('button[data-article-action]');
  if (!button) return;
  const action = String(button.dataset.articleAction || '');
  const card = button.closest('[data-article-id]');
  const id = Number(card?.dataset?.articleId);
  const article = getArticleById(id);
  if (!article) return;

  if (action === 'set-lead') {
    if (selectedLeadArticle && Number(selectedLeadArticle.id) === Number(article.id)) {
      renderAvailableArticles();
      return;
    }
    selectedStoryArticles = selectedStoryArticles.filter((item) => Number(item.id) !== Number(article.id));
    if (selectedLeadArticle) {
      const previousLead = selectedLeadArticle;
      if (!selectedStoryArticles.some((item) => Number(item.id) === Number(previousLead.id))) {
        selectedStoryArticles.unshift(previousLead);
      }
    }
    selectedLeadArticle = article;
    refreshNewsletterFromSelection();
    renderAvailableArticles();
    return;
  }

  if (action === 'toggle-story') {
    if (selectedLeadArticle && Number(selectedLeadArticle.id) === Number(article.id)) {
      selectedLeadArticle = null;
    } else {
      const existingIndex = selectedStoryArticles.findIndex((item) => Number(item.id) === Number(article.id));
      if (existingIndex >= 0) {
        selectedStoryArticles.splice(existingIndex, 1);
      } else {
        selectedStoryArticles.push(article);
      }
    }
    refreshNewsletterFromSelection();
    renderAvailableArticles();
  }
});

tokenInput.value = localStorage.getItem('de_admin_token') || '';
setLockState(sessionStorage.getItem('de_admin_unlocked_newsletters') === '1');
setCampaignContent(
  campaignContentHtmlInput.value || textToHtml(campaignContentTextInput.value || ''),
  campaignContentTextInput.value || ''
);
renderAvailableArticles();
if (unlocked && getToken()) {
  loadCampaigns().catch((error) => setMessage(`Initial load failed: ${error.message}`));
  loadAvailableArticles().catch((error) => setMessage(`Story load failed: ${error.message}`));
}
