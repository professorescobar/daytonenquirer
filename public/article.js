// Get parameters from URL
const params = new URLSearchParams(window.location.search);
const slug = params.get('slug');
const oldUrl = params.get('url');
const oldTitle = params.get('title');
const oldSource = params.get('source');
const oldDate = params.get('date');
const oldImage = params.get('image');
const oldDesc = params.get('desc');
const oldSection = params.get('section');
const isCustom = params.get('custom') === 'true';

// Determine if this is old format (RSS) or new format (custom article)
const isOldFormat = !!(oldUrl && oldTitle);

console.log('Article.js loaded');
console.log('slug:', slug);
console.log('isOldFormat:', isOldFormat);

async function loadArticle() {
  console.log('loadArticle called');
  
  const loadingEl = document.getElementById('article-loading');
  const contentEl = document.getElementById('article-content');
  
  try {
    let article;

    if (slug && !isOldFormat) {
      // NEW FORMAT: Fetch from API using slug
      console.log('Fetching custom article from API');
      const res = await fetch(`/api/article?slug=${slug}`);
      if (!res.ok) throw new Error('Article not found');
      const data = await res.json();
      article = data.article;
    } else if (isOldFormat) {
      // OLD FORMAT: Build article object from URL params
      console.log('Using old format from URL params');
      article = {
        url: decodeURIComponent(oldUrl),
        title: decodeURIComponent(oldTitle),
        source: decodeURIComponent(oldSource),
        pubDate: oldDate ? decodeURIComponent(oldDate) : null,
        image: oldImage ? decodeURIComponent(oldImage) : null,
        description: oldDesc ? decodeURIComponent(oldDesc) : '',
        section: oldSection,
        custom: isCustom
      };
    } else {
      throw new Error('Invalid article URL');
    }

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
        local: { title: "Local News" },
        national: { title: "National News" },
        world: { title: "World News" },
        business: { title: "Business" },
        sports: { title: "Sports" },
        health: { title: "Health" },
        entertainment: { title: "Entertainment" },
        technology: { title: "Technology" }
      };
      const config = sectionConfig[article.section];
      if (config) {
        categoryEl.innerHTML = `<a href="/section.html?s=${article.section}">${config.title}</a>`;
      }
    }

    // Render headline
    const titleEl = document.getElementById('article-title');
    if (titleEl) {
      titleEl.textContent = article.title;
    }

    // Render byline
    const sourceEl = document.getElementById('article-source');
    const dateEl = document.getElementById('article-date');
    
    if (dateEl && article.pubDate) {
      const date = new Date(article.pubDate);
      const dateStr = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
      const timeStr = date.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true });
  
      // Calculate time ago
      const now = new Date();
      const minutes = Math.floor((now - date) / (1000 * 60));
      const hours = Math.floor((now - date) / (1000 * 60 * 60));
  
      let timeAgo = '';
      if (minutes < 1) timeAgo = 'Just now';
      else if (minutes < 60) timeAgo = `${minutes}m ago`;  // Now shows exact minutes
      else if (hours < 24) timeAgo = `${hours}h ago`;
      else if (hours < 48) timeAgo = 'Yesterday';
      else timeAgo = '';
  
      dateEl.textContent = timeAgo ? `${dateStr} • ${timeStr} • ${timeAgo}` : `${dateStr} • ${timeStr}`;
    }

    // Render image
    const imageContainer = document.getElementById('article-image-container');
    if (imageContainer && article.image) {
     let imageHTML = `<img src="${article.image}" alt="${article.title}" loading="lazy" />`;
  
     // Add caption/credit if they exist
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

    // Render description
    const descriptionEl = document.getElementById('article-description');
    if (descriptionEl && article.description) {
      descriptionEl.innerHTML = `<p>${article.description.replace(/\n\n/g, '</p><p>')}</p>`;
    }

    // Handle "Read Full Article" button
    const readFullBtn = document.getElementById('article-read-full');
    const sourceNameEl = document.getElementById('article-source-name');
    
    if (readFullBtn && sourceNameEl) {
      if (article.custom) {
        readFullBtn.setAttribute('hidden', '');
      } else {
        readFullBtn.href = article.url;
        sourceNameEl.textContent = article.source;
      }
    }

    // Show related articles section
    const relatedSection = document.getElementById('related-section');
    if (relatedSection) {

      relatedSection.removeAttribute('hidden');
    }

    // Load related articles
    loadRelatedArticles(article.section);

     // Setup prev/next navigation
    if (article.custom) {
      setupArticleNavigation(article.section);
    }

  } catch (err) {
    console.error('Article load error:', err);
    if (loadingEl) loadingEl.innerHTML = '<p>Article not found.</p>';
  }
}

async function loadRelatedArticles(section) {
  // Helper function for formatting dates
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

    // Update section title to be a link
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
    
    // Filter to articles with images
    let articles = data.articles.filter(a => a.image);
    
    // Filter out current article
    if (slug) {
      articles = articles.filter(a => a.url !== slug);
    } else if (oldUrl) {
      articles = articles.filter(a => a.url !== decodeURIComponent(oldUrl));
    }
    
    articles = articles.slice(0, 6);

    const grid = document.getElementById('related-grid');
    const relatedSection = document.getElementById('related-section');
    
    if (!grid || !articles.length) return;

    // Show the section
    if (relatedSection) relatedSection.removeAttribute('hidden');

    grid.innerHTML = '';
    articles.forEach(article => {
      const card = document.createElement('div');
      card.className = 'bottom-article-card';
      card.innerHTML = `
        <a href="/api/article?slug=${article.url}&og=true">
          <img src="${article.image}" alt="${article.title}" class="related-card-image" loading="lazy">
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
    
    // Get all custom articles
    const customArticles = data.articles.filter(a => a.custom);
    
    // Find current article index
    let currentIndex = -1;
    if (slug) {
      currentIndex = customArticles.findIndex(a => a.url === slug);
    }
    
    if (currentIndex === -1 || customArticles.length < 2) return;

    // Show navigation
    const navSection = document.getElementById('article-navigation');
    if (navSection) navSection.removeAttribute('hidden');

    // Setup buttons
    const prevBtn = document.getElementById('prev-article');
    const nextBtn = document.getElementById('next-article');

    // Previous article (newer)
    if (currentIndex > 0) {
      const prevArticle = customArticles[currentIndex - 1];
      prevBtn.onclick = () => {
        window.location.href = `/api/article?slug=${prevArticle.url}&og=true`;
      };
    } else {
      prevBtn.disabled = true;
    }

    // Next article (older)
    if (currentIndex < customArticles.length - 1) {
      const nextArticle = customArticles[currentIndex + 1];
      nextBtn.onclick = () => {
        window.location.href = `/api/article?slug=${nextArticle.url}&og=true`;
      };
    } else {
      nextBtn.disabled = true;
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