// Get slug from URL
const params = new URLSearchParams(window.location.search);
const slug = params.get('slug');

console.log('Article.js loaded - slug:', slug);

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function getArticleSlug(article) {
  return article?.slug || article?.url || '';
}

function dedupeArticlesBySlug(articles) {
  const seen = new Set();
  const result = [];
  for (const article of articles || []) {
    const articleSlug = getArticleSlug(article);
    if (!articleSlug || seen.has(articleSlug)) continue;
    seen.add(articleSlug);
    result.push(article);
  }
  return result;
}

function sortArticlesNewestFirst(articles) {
  return [...articles].sort((a, b) => new Date(b.pubDate || 0) - new Date(a.pubDate || 0));
}

function syncCopyButtonLabels() {
  const isMobile = window.matchMedia('(max-width: 768px)').matches;
  const copyButtons = Array.from(document.querySelectorAll('[data-share-action="copy"]'));
  copyButtons.forEach((button) => {
    button.textContent = isMobile ? 'Copy' : 'Copy Link';
  });
}

function toAbsoluteUrl(input) {
  try {
    return new URL(String(input || ''), window.location.origin).toString();
  } catch (_) {
    return window.location.href;
  }
}

async function shareArticle({ title, url }, button) {
  const payload = { title: title || document.title, url: toAbsoluteUrl(url || window.location.href) };
  const nativeShareSupported = typeof navigator !== 'undefined' && typeof navigator.share === 'function';

  if (nativeShareSupported) {
    try {
      await navigator.share(payload);
      return;
    } catch (err) {
      if (err && err.name === 'AbortError') return;
    }
  }

  const copySupported = typeof navigator !== 'undefined' && navigator.clipboard && typeof navigator.clipboard.writeText === 'function';
  if (copySupported) {
    try {
      await navigator.clipboard.writeText(payload.url);
      if (button) {
        const original = button.textContent;
        button.textContent = 'Copied';
        setTimeout(() => {
          button.textContent = original || 'Share';
        }, 1300);
      }
      return;
    } catch (_) {
      // fall through to window prompt
    }
  }

  window.prompt('Copy this link:', payload.url);
}

async function copyArticleLink(url, button) {
  const absoluteUrl = toAbsoluteUrl(url || window.location.href);
  const copySupported = typeof navigator !== 'undefined' && navigator.clipboard && typeof navigator.clipboard.writeText === 'function';
  if (copySupported) {
    try {
      await navigator.clipboard.writeText(absoluteUrl);
      if (button) {
        const original = button.textContent;
        button.textContent = 'Copied';
        setTimeout(() => {
          button.textContent = original || 'Copy Link';
        }, 1300);
      }
      return;
    } catch (_) {
      // fall through to prompt
    }
  }
  window.prompt('Copy this link:', absoluteUrl);
}

function injectTopicEngineStyles() {
  const styleId = 'topic-engine-styles';
  if (document.getElementById(styleId)) return;
  const style = document.createElement('style');
  style.id = styleId;
  style.textContent = `
    .topic-engine-section {
      margin: 3rem auto;
      width: 100%;
      box-sizing: border-box;
    }
    .topic-engine-wrap {
      border: 1px solid var(--border-color, #e5e7eb);
      border-radius: 12px;
      background-color: var(--card-bg-color, #ffffff);
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }
    .topic-engine-header {
      display: flex;
      align-items: center;
      gap: 1rem;
      padding: 1rem 1.25rem;
      border-bottom: 1px solid var(--border-color, #e5e7eb);
      background-color: var(--card-bg-color, #ffffff);
    }
    .topic-engine-avatar {
      width: 50px;
      height: 50px;
      border-radius: 50%;
      overflow: hidden;
      flex-shrink: 0;
      background-color: var(--bg-color-secondary, #f3f4f6);
    }
    .topic-engine-avatar img {
      width: 100%;
      height: 100%;
      object-fit: cover;
    }
    .topic-engine-header h3 {
      margin: 0 0 0.25rem 0;
      font-size: 1.25rem;
      color: var(--text-color, #111827);
    }
    .topic-engine-disclosure {
      font-size: 0.875rem;
      color: var(--text-color-secondary, #6b7280);
      margin: 0;
    }
    .topic-engine-messages {
      padding: 1.25rem;
      overflow-y: auto;
      max-height: 400px;
      background-color: var(--bg-color, #ffffff);
      display: flex;
      flex-direction: column;
      gap: 1rem;
    }
    .topic-engine-message {
      max-width: 85%;
    }
    .topic-engine-message p {
      margin: 0;
      line-height: 1.5;
    }
    .topic-engine-message.is-user {
      align-self: flex-end;
      text-align: right;
    }
    .topic-engine-message.is-user p {
      display: inline-block;
      padding: 0.6rem 0.9rem;
      border-radius: 12px;
      box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
      background-color: var(--bg-color-secondary, #f3f4f6);
      color: var(--text-color, #111827);
      border-bottom-right-radius: 2px;
      text-align: left;
    }
    .topic-engine-message.is-bot {
      align-self: flex-start;
    }
    .topic-engine-message.is-bot p {
      display: inline-block;
      background-color: transparent;
      color: var(--text-color, #111827);
      padding: 0;
      border-radius: 0;
      box-shadow: none;
    }
    .topic-engine-message.is-thinking p {
      display: inline-block;
      color: var(--text-color-secondary, #6b7280);
      background-color: transparent;
      padding-left: 0;
      box-shadow: none;
    }
    .topic-engine-message.is-thinking .spinner {
      display: inline-block;
      width: 1em;
      height: 1em;
      border: 2px solid currentColor;
      border-right-color: transparent;
      border-radius: 50%;
      animation: spin 1s linear infinite;
      margin-right: 0.5em;
      vertical-align: -0.125em;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    .topic-engine-form {
      display: flex;
      padding: 1rem;
      border-top: 1px solid var(--border-color, #e5e7eb);
      gap: 0.5rem;
      background-color: var(--card-bg-color, #ffffff);
    }
    .topic-engine-form input {
      flex-grow: 1;
      border: 1px solid var(--border-color, #e5e7eb);
      background-color: var(--bg-color, #ffffff);
      color: var(--text-color, #111827);
      padding: 0.75rem;
      border-radius: 6px;
    }
    .topic-engine-form button {
      padding: 0.75rem 1.25rem;
      border: 0;
      border-radius: 6px;
      background-color: var(--primary-color, #2563eb);
      color: white;
      font-weight: bold;
      cursor: pointer;
    }
    .topic-engine-form button:disabled {
      background-color: var(--text-color-secondary, #6b7280);
      cursor: not-allowed;
    }

    /* Dark Mode Overrides */
    :root[data-theme="dark"] .topic-engine-wrap {
      border-color: #374151;
      background-color: #1f2937;
    }
    :root[data-theme="dark"] .topic-engine-header {
      border-bottom-color: #374151;
      background-color: #1f2937;
    }
    :root[data-theme="dark"] .topic-engine-avatar {
      background-color: #374151;
    }
    :root[data-theme="dark"] .topic-engine-header h3 {
      color: #f9fafb;
    }
    :root[data-theme="dark"] .topic-engine-disclosure {
      color: #9ca3af;
    }
    :root[data-theme="dark"] .topic-engine-messages {
      background-color: #111827;
    }
    :root[data-theme="dark"] .topic-engine-message.is-bot p {
      background-color: transparent;
      color: #f9fafb;
    }
    :root[data-theme="dark"] .topic-engine-message.is-thinking p {
      color: #9ca3af;
    }
    :root[data-theme="dark"] .topic-engine-form {
      border-top-color: #374151;
      background-color: #1f2937;
    }
    :root[data-theme="dark"] .topic-engine-form input {
      border-color: #4b5563;
      background-color: #374151;
      color: #f9fafb;
    }
    :root[data-theme="dark"] .topic-engine-form button:disabled {
      background-color: #4b5563;
    }
    :root[data-theme="dark"] .topic-engine-message.is-user p {
      background-color: #4b5563;
      color: #f9fafb;
    }
  `;
  document.head.appendChild(style);
}

async function fetchSectionArticles(apiUrl) {
  const res = await fetch(apiUrl);
  if (!res.ok) return [];
  const data = await res.json();
  return Array.isArray(data.articles) ? data.articles : [];
}

async function fetchAllSectionArticles(sectionConfig) {
  const urls = Object.values(sectionConfig);
  const settled = await Promise.allSettled(urls.map(fetchSectionArticles));
  const merged = settled
    .filter(item => item.status === 'fulfilled')
    .flatMap(item => item.value);
  return sortArticlesNewestFirst(dedupeArticlesBySlug(merged));
}

async function loadArticle() {
  const loadingEl = document.getElementById('article-loading');
  const contentEl = document.getElementById('article-content');
  
  injectTopicEngineStyles();
  try {
    if (!slug) throw new Error('No article slug provided');

    // Fetch article from database
    const res = await fetch(`/api/article?slug=${encodeURIComponent(slug)}`);
    if (!res.ok) {
      let message = `Article request failed (${res.status})`;
      try {
        const errData = await res.json();
        if (errData?.error) message = errData.error;
      } catch (_) {}
      throw new Error(message);
    }
    const data = await res.json();
    const article = data.article;

    console.log('Article loaded:', article);

    // Hide loading, show content
    if (loadingEl) loadingEl.setAttribute('hidden', '');
    if (contentEl) contentEl.removeAttribute('hidden');

    // Update page title
    document.title = `${article.title} | The Dayton Enquirer`;

    const inlineNewsletterForm = document.querySelector('.article-newsletter-form.newsletter-signup-form');
    if (inlineNewsletterForm && article.section) {
      inlineNewsletterForm.dataset.section = String(article.section);
    }
    
    // Render category badge
    const categoryEl = document.getElementById('article-category');
    if (categoryEl && article.section) {
      const sectionConfig = {
        local: "Local News",
        national: "National News",
        world: "World News",
        business: "Business",
        sports: "Sports",
        health: "Health",
        entertainment: "Entertainment",
        technology: "Technology"
      };
      const title = sectionConfig[article.section];
      if (title) {
        categoryEl.innerHTML = `<a href="/section.html?s=${article.section}">${title}</a>`;
      }
    }

    // Render headline
    const titleEl = document.getElementById('article-title');
    if (titleEl) titleEl.textContent = article.title;

    const shareButtons = Array.from(document.querySelectorAll('[data-share-action="share"]'));
    const copyButtons = Array.from(document.querySelectorAll('[data-share-action="copy"]'));
    syncCopyButtonLabels();
    window.addEventListener('resize', syncCopyButtonLabels);
    shareButtons.forEach((button) => {
      button.onclick = () => shareArticle({ title: article.title, url: window.location.href }, button);
    });
    copyButtons.forEach((button) => {
      button.onclick = () => copyArticleLink(window.location.href, button);
    });

    // Render byline with date and time
    const dateEl = document.getElementById('article-date');
    if (dateEl && article.pubDate) {
      const date = new Date(article.pubDate);
      const dateStr = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
      const timeStr = date.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true });
  
      const now = new Date();
      const minutes = Math.floor((now - date) / (1000 * 60));
      const hours = Math.floor((now - date) / (1000 * 60 * 60));
  
      let timeAgo = '';
      if (minutes < 1) timeAgo = 'Just now';
      else if (minutes < 60) timeAgo = `${minutes}m ago`;
      else if (hours < 24) timeAgo = `${hours}h ago`;
      else if (hours < 48) timeAgo = 'Yesterday';
  
      dateEl.textContent = timeAgo ? `${dateStr} • ${timeStr} • ${timeAgo}` : `${dateStr} • ${timeStr}`;
    }

    // Render image with caption/credit
    const imageContainer = document.getElementById('article-image-container');
    if (imageContainer && article.image) {
      const imageCredit = (article.imageCredit || article.sourceTitle || '').trim();
      let imageHTML = `<img src="${escapeHtml(article.image)}" alt="${escapeHtml(article.title)}" loading="lazy" />`;
  
      if (article.imageCaption || imageCredit) {
        imageHTML += `<div class="image-meta">`;
        if (article.imageCaption) {
          imageHTML += `<span class="image-caption">${escapeHtml(article.imageCaption)}</span>`;
        }
        if (imageCredit) {
          imageHTML += `<span class="image-credit">${escapeHtml(imageCredit)}</span>`;
        }
        imageHTML += `</div>`;
      }
      imageContainer.innerHTML = imageHTML;
    }

    // Render description/content
    const descriptionEl = document.getElementById('article-description');
    if (descriptionEl) {
      const content = String(article.content || article.description || '').trim();
      const hasBlockHtml = /<(p|h2|h3|ul|ol|li|blockquote|div)\b/i.test(content);
      if (!content) {
        descriptionEl.innerHTML = '';
      } else if (hasBlockHtml) {
        descriptionEl.innerHTML = content;
      } else {
        descriptionEl.innerHTML = `<p>${content.replace(/\n\n/g, '</p><p>')}</p>`;
      }
    }

    // Hide "Read Full Article" button (all articles are full custom articles now)
    const readFullBtn = document.getElementById('article-read-full');
    if (readFullBtn) readFullBtn.setAttribute('hidden', '');

    // Render Topic Engine Q&A
    renderTopicEngine(article);

    // Load related articles
    loadRelatedArticles(article.section);

    // Setup prev/next navigation (prefer deterministic backend neighbors)
    setupNavigationButtons(data.prevArticle, data.nextArticle);
    if (!data.prevArticle && !data.nextArticle) {
      console.log('API neighbors missing, falling back to section-based navigation');
      setupArticleNavigation(article.section);
    }

  } catch (err) {
    console.error('Article load error:', err);
    if (loadingEl) loadingEl.innerHTML = `<p>${escapeHtml(err?.message || 'Article not found.')}</p>`;
  }
}

function setupNavigationButtons(prevArticle, nextArticle) {
  const prevBtn = document.getElementById('prev-article');
  const nextBtn = document.getElementById('next-article');
  const navSection = document.getElementById('article-navigation');

  if (navSection) navSection.removeAttribute('hidden');
  if (!prevBtn || !nextBtn) return;

  const prevSlug = getArticleSlug(prevArticle);
  const nextSlug = getArticleSlug(nextArticle);

  prevBtn.type = 'button';
  nextBtn.type = 'button';

  prevBtn.disabled = !prevSlug;
  nextBtn.disabled = !nextSlug;

  prevBtn.onclick = prevSlug
    ? () => {
        console.log('Prev button clicked! Navigating to:', prevSlug);
        window.location.href = `article.html?slug=${encodeURIComponent(prevSlug)}`;
      }
    : null;

  nextBtn.onclick = nextSlug
    ? () => {
        console.log('Next button clicked! Navigating to:', nextSlug);
        window.location.href = `article.html?slug=${encodeURIComponent(nextSlug)}`;
      }
    : null;

  console.log('Navigation wired:', {
    prevEnabled: !!prevSlug,
    nextEnabled: !!nextSlug
  });
}

function renderTopicEngine(article) {
  let container = document.getElementById('topic-engine-container');
  if (!container) {
    const contentEl = document.getElementById('article-content');
    if (contentEl) {
      container = document.createElement('div');
      container.id = 'topic-engine-container';
      contentEl.appendChild(container);
    } else return;
  }

  // The backend should now provide an `author` object with the article.
  const authorName = article.author?.name || 'the Author';
  const avatarUrl = article.author?.avatarUrl || '/images/personas/default-avatar.svg';
  const disclosure = article.author?.disclosure || 'This article was generated by a topic engine. You can ask it questions based on the text above.';

  container.innerHTML = `
    <div class="topic-engine-section">
      <div class="topic-engine-wrap">
        <div class="topic-engine-header">
          <div class="topic-engine-avatar">
            <img src="${escapeHtml(avatarUrl)}" alt="Icon for ${escapeHtml(authorName)}" />
          </div>
          <div>
            <h3>Ask ${escapeHtml(authorName)}</h3>
            <p class="topic-engine-disclosure">
              ${escapeHtml(disclosure)}
            </p>
          </div>
        </div>
        <div class="topic-engine-messages" id="topic-engine-messages">
          <div class="topic-engine-message is-bot"><p>Hello! I am the AI that helped write this article. What would you like to know more about?</p></div>
        </div>
        <form class="topic-engine-form" id="topic-engine-form">
          <input type="text" name="query" placeholder="Ask a question about the article..." required autocomplete="off">
          <button type="submit">Send</button>
        </form>
      </div>
    </div>
  `;

  const form = document.getElementById('topic-engine-form');
  const input = form.querySelector('input[name="query"]');
  const button = form.querySelector('button');
  const messagesEl = document.getElementById('topic-engine-messages');

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const query = input.value.trim();
    if (!query) return;

    input.value = '';
    button.disabled = true;

    appendMessage(query, 'user');
    const thinkingEl = appendMessage('Thinking...', 'bot', true);

    try {
      const answer = await getTopicEngineResponse(query, article);
      thinkingEl.remove();
      appendMessage(answer, 'bot');
    } catch (err) {
      thinkingEl.remove();
      appendMessage('Sorry, I encountered an error. Please try again.', 'bot');
    } finally {
      button.disabled = false;
      input.focus();
    }
  });

  function appendMessage(text, sender, isThinking = false) {
    const messageEl = document.createElement('div');
    messageEl.className = `topic-engine-message is-${sender}`;
    if (isThinking) {
      messageEl.classList.add('is-thinking');
      messageEl.innerHTML = `<p><span class="spinner"></span>${escapeHtml(text)}</p>`;
    } else {
      messageEl.innerHTML = `<p>${escapeHtml(text)}</p>`;
    }
    messagesEl.appendChild(messageEl);
    messagesEl.scrollTop = messagesEl.scrollHeight;
    return messageEl;
  }
}

async function getTopicEngineResponse(query, article) {
  // This is a placeholder. In the future, this will make an API call.
  // The API will use the article content and persona to generate a response.
  //
  // const res = await fetch('/api/article-chat', {
  //   method: 'POST',
  //   headers: { 'Content-Type': 'application/json' },
  //   body: JSON.stringify({
  //     query,
  //     articleContent: article.content,
  //     persona: article.persona,
  //     slug: article.slug
  //   })
  // });
  // if (!res.ok) throw new Error('API request failed');
  // const data = await res.json();
  // return data.answer;

  // --- Placeholder Logic ---
  await new Promise(resolve => setTimeout(resolve, 1500));

  const lowerQuery = query.toLowerCase();
  if (lowerQuery.includes('next topic') || lowerQuery.includes('new article') || lowerQuery.includes('write about')) {
    // This simulates the "new topic" feedback loop
    return "That's an interesting question, but it seems to be outside the scope of this article. Would you like me to suggest this as a topic for a future article?";
  }

  if (article.title && article.title.toLowerCase().includes(lowerQuery)) {
    return `Yes, this article is about "${article.title}". It covers the main points and provides context on the subject.`;
  }

  return `This is a placeholder response. The full Q&A functionality is being built. Based on the article, I can confirm that the topic of "${query}" is mentioned. The system will soon be able to provide more detailed answers.`;
}

function formatDate(dateString) {
  if (!dateString) return '';
  const date = new Date(dateString);
  const now = new Date();
  const minutes = Math.floor((now - date) / (1000 * 60));
  const hours = Math.floor((now - date) / (1000 * 60 * 60));
  
  if (minutes < 1) return 'Just now';
  if (minutes < 60) return `${minutes}m ago`;
  if (hours < 24) return `${hours}h ago`;
  if (hours < 48) return 'Yesterday';
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

async function loadRelatedArticles(section) {
  try {
    const sectionConfig = {
      local: '/api/local-news',
      national: '/api/national-news',
      world: '/api/world-news',
      business: '/api/business-news',
      sports: '/api/sports-news',
      health: '/api/health-news',
      entertainment: '/api/entertainment-news',
      technology: '/api/technology-news'
    };

    // Update section title link
    const sectionTitle = document.querySelector('.bottom-articles-title');
    if (sectionTitle && section) {
      const sectionNames = {
        local: 'the Local News section...',
        national: 'the National News section...',
        world: 'the World News section...',
        business: 'the Business section...',
        sports: 'the Sports section...',
        health: 'the Health section...',
        entertainment: 'the Entertainment section...',
        technology: 'the Technology section...'
      };
      sectionTitle.innerHTML = `<a href="/section.html?s=${section}">More from ${sectionNames[section] || 'this section'}</a>`;
    }

    const apiUrl = sectionConfig[section];
    if (!apiUrl) return;

    const res = await fetch(apiUrl);
    if (!res.ok) return;
    const data = await res.json();
    
    // Filter articles with images, exclude current article
    let articles = data.articles
      .filter(a => a.image && getArticleSlug(a) !== slug)
      .slice(0, 6);

    const grid = document.getElementById('related-grid');
    const relatedSection = document.getElementById('related-section');
    
    if (!grid || !articles.length) return;
    if (relatedSection) relatedSection.removeAttribute('hidden');

    grid.innerHTML = '';
    articles.forEach(article => {
      const card = document.createElement('div');
      card.className = 'bottom-article-card';
      const articleSlug = getArticleSlug(article);
      if (!articleSlug) return;
      card.innerHTML = `
        <a href="article.html?slug=${encodeURIComponent(articleSlug)}">
          <img src="${article.image}" alt="${article.title}" loading="lazy">
          <h4>${article.title}</h4>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>
        </a>
      `;
      grid.appendChild(card);
    });
  } catch (err) {
    console.error('Related articles error:', err);
  }
}

async function setupArticleNavigation(currentSection) {
  console.log('=== NAVIGATION FUNCTION CALLED ===');
  console.log('setupArticleNavigation called with section:', currentSection);
  
  try {
    const sectionConfig = {
      local: '/api/local-news',
      national: '/api/national-news',
      world: '/api/world-news',
      business: '/api/business-news',
      sports: '/api/sports-news',
      health: '/api/health-news',
      entertainment: '/api/entertainment-news',
      technology: '/api/technology-news'
    };
    
    const apiUrl = sectionConfig[currentSection];
    console.log('API URL:', apiUrl);

    let articles = [];
    if (apiUrl) {
      articles = sortArticlesNewestFirst(dedupeArticlesBySlug(await fetchSectionArticles(apiUrl)));
    }
    if (!articles.length || currentSection === 'all') {
      console.log('Falling back to all-sections article list');
      articles = await fetchAllSectionArticles(sectionConfig);
    }
    
    console.log('Total articles:', articles.length);
    console.log('Current slug:', slug);
    
    // Find current article index
    let currentIndex = articles.findIndex(a => getArticleSlug(a) === slug);
    if (currentIndex === -1 && apiUrl) {
      console.log('Article not found in section list, retrying against all sections');
      articles = await fetchAllSectionArticles(sectionConfig);
      currentIndex = articles.findIndex(a => getArticleSlug(a) === slug);
    }
    console.log('Current index:', currentIndex);
    
    if (currentIndex === -1 || articles.length < 2) {
      console.log('Not enough articles or index not found');
      return;
    }
    
    // Prev = newer (lower index), Next = older (higher index)
    const prevArticle = currentIndex > 0 ? articles[currentIndex - 1] : null;
    const nextArticle = currentIndex < articles.length - 1 ? articles[currentIndex + 1] : null;
    
    console.log('Prev article:', prevArticle?.title);
    console.log('Next article:', nextArticle?.title);
    
    setupNavigationButtons(prevArticle, nextArticle);
  } catch (err) {
    console.error('Navigation setup error:', err);
  }
}

// Start loading
loadArticle();

// Market ticker
(function () {
  const container = document.querySelector(".tradingview-widget-container");
  if (!container) return;

  const script = document.createElement("script");
  script.src = "https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js";
  script.async = true;
  script.innerHTML = JSON.stringify({
    symbols: [
      { proName: "DJI", title: "Dow Jones" },
      { proName: "OANDA:SPX500USD", title: "S&P 500" },
      { proName: "OANDA:NAS100USD", title: "NASDAQ 100" },
      { proName: "NYSE:NYA", title: "NYSE Composite" },
      { proName: "OANDA:US2000USD", title: "Russell 2000" },
      { proName: "OANDA:EURUSD", title: "EUR/USD" },
      { proName: "OANDA:USDJPY", title: "USD/JPY" },
      { proName: "TVC:GOLD", title: "Gold" },
      { proName: "TVC:SILVER", title: "Silver" },
      { proName: "TVC:USOIL", title: "Crude Oil" }
    ],
    showSymbolLogo: false,
    showChange: true,
    showPercentageChange: true,
    colorTheme: document.documentElement.dataset.theme === "dark" ? "dark" : "light",
    isTransparent: false,
    displayMode: "regular",
    locale: "en"
  });
  container.appendChild(script);
})();
