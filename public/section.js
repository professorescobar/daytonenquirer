// Section page script - updated

// ============================
// SECTION PAGE
// ============================

const sectionConfig = {
  local:         { title: "Local News",         api: "/api/local-news" },
  national:      { title: "National News",       api: "/api/national-news" },
  world:         { title: "World News",          api: "/api/world-news" },
  business:      { title: "Business",            api: "/api/business-news" },
  sports:        { title: "Sports",              api: "/api/sports-news" },
  health:        { title: "Health",              api: "/api/health-news" },
  entertainment: { title: "Entertainment",       api: "/api/entertainment-news" },
  technology:    { title: "Technology",          api: "/api/technology-news" }
};

// Get section from URL query parameter
const params = new URLSearchParams(window.location.search);
const sectionKey = (params.get('s') || '').toLowerCase();
const config = sectionConfig[sectionKey];

// Update page title
const sectionTitle = document.getElementById("section-title");
if (config) {
  sectionTitle.textContent = config.title;
  document.title = `${config.title} | The Dayton Enquirer`;
  const metaDesc = document.querySelector('meta[name="description"]');
  if (metaDesc) {
    metaDesc.setAttribute('content', `Latest ${config.title.toLowerCase()} from The Dayton Enquirer. Breaking news and updates.`);
  }
} else {
  sectionTitle.textContent = "Section Not Found";
}

function formatDate(dateString) {
  if (!dateString) return '';
  const date = new Date(dateString);
  const now = new Date();
  const hours = Math.floor((now - date) / (1000 * 60 * 60));
  if (hours < 1) return 'Just now';
  if (hours < 24) return `${hours}h ago`;
  if (hours < 48) return 'Yesterday';
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

function articleLink(article) {
  if (article.custom) {
    return `/api/article?slug=${article.url}&og=true`;
  }
  return `article.html?url=${encodeURIComponent(article.url)}&title=${encodeURIComponent(article.title)}&source=${encodeURIComponent(article.source)}&date=${encodeURIComponent(article.pubDate || '')}&image=${encodeURIComponent(article.image || '')}&desc=${encodeURIComponent(article.description || '')}&section=${sectionKey}`;
}

let allArticles = [];

function renderFeatured(article) {
  const container = document.getElementById("section-featured");
  if (!container || !article) return;

  container.innerHTML = `
    <div class="world-news-content">
      <div class="featured-story-container">
        <article class="featured-article">
          ${article.image
            ? `<img src="${article.image}" alt="${article.title}" loading="lazy">`
            : '<div class="placeholder-image"></div>'
          }
          <div class="featured-overlay">
            <h3>
              <a href="${articleLink(article)}">
                ${article.title}
              </a>
            </h3>
            <div class="article-meta">
              ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
            </div>
          </div>
        </article>
      </div>
      <div class="headlines-sidebar" id="section-sidebar-headlines">
        <!-- Top 5 headlines will be inserted here -->
      </div>
    </div>
  `;
}

let currentPage = 1;
const ARTICLES_PER_PAGE_DESKTOP = 18; // 3 columns × 6 rows
const ARTICLES_PER_PAGE_MOBILE_INITIAL = 5;
const ARTICLES_PER_PAGE_MOBILE_LOAD = 7;
let mobileArticlesShown = 0;

function renderArticles(articles) {
  const grid = document.getElementById("section-articles-grid");
  if (!grid) return;

  const isMobile = window.innerWidth <= 768;

  if (isMobile) {
    // Mobile: Show initial articles
    grid.innerHTML = '';
    mobileArticlesShown = ARTICLES_PER_PAGE_MOBILE_INITIAL;
    
    articles.slice(0, mobileArticlesShown).forEach(article => {
      const li = document.createElement("li");
      li.innerHTML = `
        <a href="${articleLink(article)}">
          ${article.title}
        </a>
        <div class="article-meta">
          ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
        </div>
      `;
      grid.appendChild(li);
    });

    // Show/hide Load More button
    const loadMoreBtn = document.getElementById("load-more-btn");
    const loadMoreContainer = document.getElementById("load-more-container");
    
    if (mobileArticlesShown < articles.length) {
      if (!loadMoreContainer) {
        const container = document.createElement("div");
        container.id = "load-more-container";
        container.style.textAlign = "center";
        container.style.margin = "2rem 0";
        container.innerHTML = `<button id="load-more-btn" class="load-more-btn">Load More</button>`;
        grid.parentElement.appendChild(container);
        
        document.getElementById("load-more-btn").addEventListener("click", () => {
          loadMoreArticles(articles);
        });
      } else {
        loadMoreContainer.removeAttribute("hidden");
      }
    } else if (loadMoreContainer) {
      loadMoreContainer.setAttribute("hidden", "");
    }

  } else {
    // Desktop: Show paginated articles
    const start = (currentPage - 1) * ARTICLES_PER_PAGE_DESKTOP;
    const end = start + ARTICLES_PER_PAGE_DESKTOP;
    const pageArticles = articles.slice(start, end);

    grid.innerHTML = '';
    pageArticles.forEach(article => {
      const li = document.createElement("li");
      li.innerHTML = `
        <a href="${articleLink(article)}">
          ${article.title}
        </a>
        <div class="article-meta">
          ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
        </div>
      `;
      grid.appendChild(li);
    });

    // Render pagination controls
    renderPagination(articles.length);
  }
}

function loadMoreArticles(articles) {
  const grid = document.getElementById("section-articles-grid");
  const loadMoreBtn = document.getElementById("load-more-btn");
  
  const nextBatch = articles.slice(
    mobileArticlesShown, 
    mobileArticlesShown + ARTICLES_PER_PAGE_MOBILE_LOAD
  );

  nextBatch.forEach(article => {
    const li = document.createElement("li");
    li.innerHTML = `
      <a href="${articleLink(article)}">
        ${article.title}
      </a>
      <div class="article-meta">
        ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
      </div>
    `;
    grid.appendChild(li);
  });

  mobileArticlesShown += nextBatch.length;

  // Hide button if no more articles
  if (mobileArticlesShown >= articles.length) {
    const loadMoreContainer = document.getElementById("load-more-container");
    if (loadMoreContainer) loadMoreContainer.setAttribute("hidden", "");
  }
}

function renderPagination(totalArticles) {
  const totalPages = Math.ceil(totalArticles / ARTICLES_PER_PAGE_DESKTOP);
  
  let paginationContainer = document.getElementById("pagination-container");
  
  if (!paginationContainer) {
    paginationContainer = document.createElement("div");
    paginationContainer.id = "pagination-container";
    paginationContainer.className = "pagination-container";
    document.getElementById("section-articles-grid").parentElement.appendChild(paginationContainer);
  }

  if (totalPages <= 1) {
    paginationContainer.innerHTML = '';
    return;
  }

  paginationContainer.innerHTML = `
    <button id="prev-page" class="pagination-btn" ${currentPage === 1 ? 'disabled' : ''}>
      ← Previous
    </button>
    <span class="page-info">Page ${currentPage} of ${totalPages}</span>
    <button id="next-page" class="pagination-btn" ${currentPage === totalPages ? 'disabled' : ''}>
      Next →
    </button>
  `;

  const prevBtn = document.getElementById("prev-page");
  const nextBtn = document.getElementById("next-page");

  if (prevBtn) {
    prevBtn.addEventListener("click", () => {
      if (currentPage > 1) {
        currentPage--;
        renderArticles(allArticles.slice(6));
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", () => {
      if (currentPage < totalPages) {
        currentPage++;
        renderArticles(allArticles.slice(6));
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
    });
  }
}

loadSection();

// ============================
// MARKET TICKER (TradingView)
// ============================
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