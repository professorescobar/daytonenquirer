// Get slug from URL
const params = new URLSearchParams(window.location.search);
const slug = params.get('slug');

console.log('Article.js loaded - slug:', slug);

async function loadArticle() {
  const loadingEl = document.getElementById('article-loading');
  const contentEl = document.getElementById('article-content');
  
  try {
    if (!slug) throw new Error('No article slug provided');

    // Fetch article from database
    const res = await fetch(`/api/article?slug=${slug}`);
    if (!res.ok) throw new Error('Article not found');
    const data = await res.json();
    const article = data.article;

    console.log('Article loaded:', article);

    // Hide loading, show content
    if (loadingEl) loadingEl.setAttribute('hidden', '');
    if (contentEl) contentEl.removeAttribute('hidden');

    // Update page title
    document.title = `${article.title} | The Dayton Enquirer`;
    
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
      let imageHTML = `<img src="${article.image}" alt="${article.title}" loading="lazy" />`;
  
      if (article.imageCaption || article.imageCredit) {
        imageHTML += `<div class="image-meta">`;
        if (article.imageCredit) {
          imageHTML += `<span class="image-credit">${article.imageCredit}</span>`;
        }
        if (article.imageCaption) {
          imageHTML += `<span class="image-caption">${article.imageCaption}</span>`;
        }
        imageHTML += `</div>`;
      }
      imageContainer.innerHTML = imageHTML;
    }

    // Render description/content
    const descriptionEl = document.getElementById('article-description');
    if (descriptionEl) {
      const content = article.content || article.description || '';
      descriptionEl.innerHTML = `<p>${content.replace(/\n\n/g, '</p><p>')}</p>`;
    }

    // Hide "Read Full Article" button (all articles are full custom articles now)
    const readFullBtn = document.getElementById('article-read-full');
    if (readFullBtn) readFullBtn.setAttribute('hidden', '');

    // Load related articles
    loadRelatedArticles(article.section);

    // Setup prev/next navigation
    setupArticleNavigation(article.section);

  } catch (err) {
    console.error('Article load error:', err);
    if (loadingEl) loadingEl.innerHTML = '<p>Article not found.</p>';
  }
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
      .filter(a => a.image && a.slug !== slug)
      .slice(0, 6);

    const grid = document.getElementById('related-grid');
    const relatedSection = document.getElementById('related-section');
    
    if (!grid || !articles.length) return;
    if (relatedSection) relatedSection.removeAttribute('hidden');

    grid.innerHTML = '';
    articles.forEach(article => {
      const card = document.createElement('div');
      card.className = 'bottom-article-card';
      card.innerHTML = `
        <a href="article.html?slug=${article.slug}">
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
    if (!apiUrl) return;
    
    const res = await fetch(apiUrl);
    if (!res.ok) return;
    const data = await res.json();
    const articles = data.articles;
    
    // Find current article index
    const currentIndex = articles.findIndex(a => a.slug === slug);
    if (currentIndex === -1 || articles.length < 2) return;
    
    // Prev = newer (lower index), Next = older (higher index)
    const prevArticle = currentIndex > 0 ? articles[currentIndex - 1] : null;
    const nextArticle = currentIndex < articles.length - 1 ? articles[currentIndex + 1] : null;
    
    const prevBtn = document.getElementById('prev-article');
    const nextBtn = document.getElementById('next-article');
    
    if (prevBtn) {
      if (prevArticle) {
        prevBtn.href = `article.html?slug=${prevArticle.slug}`;
        prevBtn.disabled = false;
      } else {
        prevBtn.disabled = true;
      }
    }
    
    if (nextBtn) {
      if (nextArticle) {
        nextBtn.href = `article.html?slug=${nextArticle.slug}`;
        nextBtn.disabled = false;
      } else {
        nextBtn.disabled = true;
      }
    }
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
    colorTheme: "light",
    isTransparent: false,
    displayMode: "regular",
    locale: "en"
  });
  container.appendChild(script);
})();