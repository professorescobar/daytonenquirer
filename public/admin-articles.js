const lockSection = document.getElementById('article-lock');
const appSection = document.getElementById('article-app');
const adminUiPasswordInput = document.getElementById('admin-ui-password');
const unlockAdminBtn = document.getElementById('unlock-admin-btn');

const tokenInput = document.getElementById('admin-token');
const sectionFilterInput = document.getElementById('article-section-filter');
const limitInput = document.getElementById('article-list-limit');
const saveTokenBtn = document.getElementById('save-token-btn');
const loadArticlesBtn = document.getElementById('load-articles-btn');
const showAllBtn = document.getElementById('show-all-btn');
const articleSearchInput = document.getElementById('article-search');
const articleTotalCountEl = document.getElementById('article-total-count');
const messageEl = document.getElementById('admin-message');
const listEl = document.getElementById('article-list');

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

let unlocked = false;
let loadedArticles = [];
let lastTotalCount = 0;
let totalAllArticles = 0;
const ET_TIME_ZONE = 'America/New_York';

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

    sessionStorage.setItem('de_admin_unlocked_articles', '1');
    setLockState(true);
    setMessage('Editor unlocked.');
    if (getToken()) {
      await loadArticles();
    }
  } catch (err) {
    setMessage(`Unlock failed: ${err.message}`);
  }
}

async function apiRequest(url, options = {}) {
  if (!unlocked) throw new Error('Editor is locked');
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
  if (!res.ok) throw new Error(data.error || `Request failed (${res.status})`);
  return data;
}

function saveToken() {
  localStorage.setItem('de_admin_token', getToken());
  setMessage('Token saved.');
}

function loadToken() {
  const token = localStorage.getItem('de_admin_token') || '';
  if (token) tokenInput.value = token;
}

function escapeHtml(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatDate(dateString) {
  if (!dateString) return '';
  const d = new Date(dateString);
  if (Number.isNaN(d.getTime())) return dateString;
  return d.toLocaleString();
}

function getEtPartsFromDate(date) {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone: ET_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  const parts = dtf.formatToParts(date);
  const out = {};
  for (const part of parts) {
    if (part.type !== 'literal') out[part.type] = part.value;
  }
  return out;
}

function formatUtcIsoToEtLocalValue(utcIso) {
  if (!utcIso) return '';
  const date = new Date(utcIso);
  if (Number.isNaN(date.getTime())) return '';
  const p = getEtPartsFromDate(date);
  return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}`;
}

function etLocalToUtcIso(localValue) {
  if (!localValue) return null;
  const match = String(localValue).match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/
  );
  if (!match) return null;

  const [, y, mo, d, h, mi] = match;
  const year = Number(y);
  const month = Number(mo);
  const day = Number(d);
  const hour = Number(h);
  const minute = Number(mi);

  // Brute-force a 24h window around a UTC guess to find exact ET wall time.
  const guessUtc = Date.UTC(year, month - 1, day, hour + 5, minute, 0);
  const windowStart = guessUtc - 12 * 60 * 60 * 1000;
  const windowEnd = guessUtc + 12 * 60 * 60 * 1000;

  for (let t = windowStart; t <= windowEnd; t += 60 * 1000) {
    const p = getEtPartsFromDate(new Date(t));
    if (
      Number(p.year) === year &&
      Number(p.month) === month &&
      Number(p.day) === day &&
      Number(p.hour) === hour &&
      Number(p.minute) === minute
    ) {
      return new Date(t).toISOString();
    }
  }

  return null;
}

function sectionSelectHtml(selected) {
  return SECTION_OPTIONS.map((value) => {
    const isSelected = value === selected ? 'selected' : '';
    return `<option value="${value}" ${isSelected}>${value}</option>`;
  }).join('');
}

function renderArticles(articles) {
  if (!articles.length) {
    listEl.innerHTML = '<p>No published articles found.</p>';
    return;
  }

  listEl.innerHTML = articles.map((article) => `
    <article class="draft-card" data-id="${article.id}">
      <button class="draft-header draft-toggle btn-reset" type="button">
        <strong>#${article.id} - ${escapeHtml(article.title || '')}</strong>
        <span class="draft-meta">
          section: ${escapeHtml(article.section || '')} |
          published: ${escapeHtml(formatDate(article.pubDate))} |
          slug: ${escapeHtml(article.slug || '')}
        </span>
      </button>

      <div class="draft-form article-editor is-collapsed" hidden>
        <label class="full">
          Title
          <input class="field-title" type="text" value="${escapeHtml(article.title || '')}" />
        </label>
        <label class="full">
          Description
          <textarea class="field-description">${escapeHtml(article.description || '')}</textarea>
        </label>
        <label class="full">
          Content
          <textarea class="field-content">${escapeHtml(article.content || '')}</textarea>
        </label>
        <label class="full">
          Image URL
          <input class="field-image" type="text" value="${escapeHtml(article.image || '')}" />
        </label>
        <label class="full">
          Image Description / Caption
          <textarea class="field-image-caption">${escapeHtml(article.imageCaption || '')}</textarea>
        </label>
        <label class="full">
          Image Source / Credit
          <input class="field-image-credit" type="text" value="${escapeHtml(article.imageCredit || '')}" />
        </label>
        <label>
          Section
          <select class="field-section">${sectionSelectHtml(article.section)}</select>
        </label>
        <label>
          Publish Date (ET)
          <input class="field-pubdate" type="datetime-local" value="${escapeHtml(formatUtcIsoToEtLocalValue(article.pubDate))}" />
        </label>
      </div>
      <div class="draft-actions article-editor is-collapsed" hidden>
        <button class="btn btn-primary btn-save-article">Save Article</button>
      </div>
    </article>
  `).join('');
}

function articleSearchHaystack(article) {
  const title = String(article.title || '').toLowerCase();
  const section = String(article.section || '').toLowerCase();
  const slug = String(article.slug || '').toLowerCase();
  const iso = String(article.pubDate || '').toLowerCase();
  const pretty = String(formatDate(article.pubDate) || '').toLowerCase();
  return `${title} ${section} ${slug} ${iso} ${pretty}`;
}

function applySearchFilter() {
  const q = String(articleSearchInput.value || '').trim().toLowerCase();
  if (!q) {
    renderArticles(loadedArticles);
    articleTotalCountEl.textContent = String(totalAllArticles || lastTotalCount || loadedArticles.length);
    return;
  }
  const filtered = loadedArticles.filter((a) => articleSearchHaystack(a).includes(q));
  renderArticles(filtered);
  articleTotalCountEl.textContent = String(totalAllArticles || lastTotalCount || loadedArticles.length);
}

async function loadArticles() {
  try {
    setMessage('Loading published articles...');
    const section = encodeURIComponent(sectionFilterInput.value || 'all');
    const limit = encodeURIComponent(limitInput.value || '50');
    const data = await apiRequest(`/api/admin-articles?section=${section}&limit=${limit}`);
    loadedArticles = data.articles || [];
    lastTotalCount = Number(data.totalCount || loadedArticles.length || 0);
    totalAllArticles = Number(data.totalAllCount || totalAllArticles || lastTotalCount);
    articleTotalCountEl.textContent = String(totalAllArticles);
    applySearchFilter();
    setMessage(`Loaded ${data.count || 0} article(s) from ${lastTotalCount} in section filter (${totalAllArticles} total published).`);
  } catch (err) {
    setMessage(`Load failed: ${err.message}`);
  }
}

async function showAllArticles() {
  try {
    setMessage('Loading all matching published articles...');
    const section = encodeURIComponent(sectionFilterInput.value || 'all');
    const total = Math.max(25, Math.min(5000, Number(lastTotalCount || 5000)));
    limitInput.value = String(total);
    await loadArticles();
  } catch (err) {
    setMessage(`Show all failed: ${err.message}`);
  }
}

async function saveArticle(card) {
  const id = Number(card.dataset.id);
  const pubDateRaw = card.querySelector('.field-pubdate').value;
  const pubDate = pubDateRaw ? etLocalToUtcIso(pubDateRaw) : null;
  if (pubDateRaw && !pubDate) {
    throw new Error('Invalid ET publish date format');
  }

  await apiRequest('/api/admin-update-article', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id,
      title: card.querySelector('.field-title').value,
      description: card.querySelector('.field-description').value,
      content: card.querySelector('.field-content').value,
      section: card.querySelector('.field-section').value,
      image: card.querySelector('.field-image').value,
      imageCaption: card.querySelector('.field-image-caption').value,
      imageCredit: card.querySelector('.field-image-credit').value,
      pubDate
    })
  });
}

function onListClick(event) {
  const card = event.target.closest('.draft-card');
  if (!card) return;
  const button = event.target.closest('button');
  if (!button) return;

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

  if (button.classList.contains('btn-save-article')) {
    saveArticle(card)
      .then(() => setMessage(`Article #${card.dataset.id} saved.`))
      .catch((err) => setMessage(`Save failed: ${err.message}`));
  }
}

saveTokenBtn.addEventListener('click', saveToken);
loadArticlesBtn.addEventListener('click', loadArticles);
unlockAdminBtn.addEventListener('click', unlock);
listEl.addEventListener('click', onListClick);
articleSearchInput.addEventListener('input', applySearchFilter);
showAllBtn.addEventListener('click', showAllArticles);
limitInput.addEventListener('input', () => {
  const raw = Number(limitInput.value || 50);
  const stepped = Math.max(25, Math.min(200, Math.round(raw / 25) * 25));
  if (stepped !== raw) limitInput.value = String(stepped);
});

loadToken();
setLockState(sessionStorage.getItem('de_admin_unlocked_articles') === '1');
if (unlocked && getToken()) {
  loadArticles().catch((err) => setMessage(`Load failed: ${err.message}`));
}
