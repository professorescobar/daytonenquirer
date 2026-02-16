// Get slug from URL
const params = new URLSearchParams(window.location.search);
const slug = params.get('slug');

if (!slug) {
  document.body.innerHTML = '<main class="container"><p>Article not found.</p></main>';
}

async function loadArticle() {
  try {
    // Fetch article data from API
    const res = await fetch(`/api/article?slug=${slug}`);
    if (!res.ok) throw new Error('Article not found');
    
    const data = await res.json();
    const article = data.article;

    // Update page title and meta tags
    document.title = `${article.title} | The Dayton Enquirer`;
    
    const metaDesc = document.querySelector('meta[name="description"]');
    if (metaDesc) {
      metaDesc.setAttribute('content', article.description.slice(0, 160));
    }

    // Update Open Graph tags
    document.querySelector('meta[property="og:title"]').setAttribute('content', article.title);
    document.querySelector('meta[property="og:description"]').setAttribute('content', article.description.slice(0, 160));
    document.querySelector('meta[property="og:image"]').setAttribute('content', article.image || '');
    document.querySelector('meta[property="og:url"]').setAttribute('content', window.location.href);

    // Update Twitter Card tags
    document.querySelector('meta[name="twitter:title"]').setAttribute('content', article.title);
    document.querySelector('meta[name="twitter:description"]').setAttribute('content', article.description.slice(0, 160));
    document.querySelector('meta[name="twitter:image"]').setAttribute('content', article.image || '');

    // Render category badge
    const categoryEl = document.querySelector('.article-category');
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
    const headlineEl = document.querySelector('.article-content h1');
    if (headlineEl) {
      headlineEl.textContent = article.title;
    }

    // Render byline
    const bylineEl = document.querySelector('.article-byline');
    if (bylineEl && article.source) {
      let bylineHTML = `<strong>${article.source}</strong>`;
      if (article.pubDate) {
        const date = new Date(article.pubDate);
        bylineHTML += ` | ${date.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}`;
      }
      bylineEl.innerHTML = bylineHTML;
    }

    // Render image
    const imageContainer = document.querySelector('.article-image-container');
    if (imageContainer && article.image) {
      imageContainer.innerHTML = `<img src="${article.image}" alt="${article.title}" loading="lazy" />`;
    } else if (imageContainer) {
      imageContainer.remove();
    }

    // Render description (convert \n\n to paragraphs)
    const descriptionEl = document.querySelector('.article-description');
    if (descriptionEl && article.description) {
      descriptionEl.innerHTML = `<p>${article.description.replace(/\n\n/g, '</p><p>')}</p>`;
    }

    // Hide "Read Full Article" button for custom articles
    const readFullBtn = document.getElementById('article-read-full');
    if (readFullBtn) {
      if (article.custom) {
        readFullBtn.setAttribute("hidden", "");
      } else {
        readFullBtn.href = article.url;
      }
    }

    // Load related articles
    loadRelatedArticles(article.section);

  } catch (err) {
    console.error('Article load error:', err);
    document.querySelector('.article-content').innerHTML = '<p>Article not found.</p>';
  }
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

    const apiUrl = sectionConfig[section];
    if (!apiUrl) return;

    const res = await fetch(apiUrl);
    if (!res.ok) return;

    const data = await res.json();
    const articles = data.articles.filter(a => a.url !== slug).slice(0, 6);

    const grid = document.querySelector('.related-grid');
    if (!grid || !articles.length) return;

    grid.innerHTML = '';
    articles.forEach(article => {
      const card = document.createElement('div');
      card.className = 'related-card';
      card.innerHTML = `
        <a href="/article/${article.url}">
          ${article.image ? `<img src="${article.image}" alt="${article.title}" class="related-card-image" loading="lazy">` : ''}
          <h4>${article.title}</h4>
          <p class="related-source">${article.source}</p>
        </a>
      `;
      grid.appendChild(card);
    });
  } catch (err) {
    console.error('Related articles error:', err);
  }
}

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