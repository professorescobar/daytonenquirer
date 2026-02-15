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
  // Update meta description
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

let allArticles = [];
let displayed = 0;
const PAGE_SIZE = 18;

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
              <a href="article.html?url=${encodeURIComponent(article.url)}&title=${encodeURIComponent(article.title)}&source=${encodeURIComponent(article.source)}&date=${encodeURIComponent(article.pubDate || '')}&image=${encodeURIComponent(article.image || '')}&desc=${encodeURIComponent(article.description || '')}&section=${sectionKey}${article.custom ? '&custom=true' : ''}">
                ${article.title}
              </a>
            </h3>
            <div class="article-meta">
              <span class="source">${article.source}</span>
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

function renderArticles(articles) {
  const grid = document.getElementById("section-articles-grid");
  if (!grid) return;

  articles.forEach(article => {
    const card = document.createElement("div");
    card.className = "section-article-card";
    card.innerHTML = `
      ${article.image ? `<img src="${article.image}" alt="${article.title}" class="card-image" loading="lazy">` : ''}
      <div class="card-body">
        <h4><a href="article.html?url=${encodeURIComponent(article.url)}&title=${encodeURIComponent(article.title)}&source=${encodeURIComponent(article.source)}&date=${encodeURIComponent(article.pubDate || '')}&image=${encodeURIComponent(article.image || '')}&desc=${encodeURIComponent(article.description || '')}&section=${sectionKey}${article.custom ? '&custom=true' : ''}"></h4>
        <p>${article.description ? article.description.slice(0, 120) + '...' : ''}</p>
        <div class="article-meta">
          <span class="source">${article.source}</span>
          ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
        </div>
      </div>
    `;
    grid.appendChild(card);
  });

  displayed += articles.length;

  // Show/hide load more button
  const loadMoreBtn = document.getElementById("load-more-btn");
  if (loadMoreBtn) {
    if (displayed < allArticles.length) {
      loadMoreBtn.removeAttribute("hidden");
    } else {
      loadMoreBtn.setAttribute("hidden", "");
    }
  }
}

async function loadSection() {
  if (!config) return;

  try {
    const res = await fetch(config.api);
    if (!res.ok) throw new Error("Failed to fetch section");

    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) {
      document.getElementById("section-featured").innerHTML = "<p>No articles found.</p>";
      return;
    }

    allArticles = data.articles;

    // Render featured article
renderFeatured(allArticles[0]);

// Render sidebar headlines (next 5 articles)
const sidebarContainer = document.getElementById("section-sidebar-headlines");
if (sidebarContainer) {
  const sidebarList = document.createElement("ul");
  allArticles.slice(1, 6).forEach(article => {
    const li = document.createElement("li");
    li.innerHTML = `
      <a href="article.html?url=${encodeURIComponent(article.url)}&title=${encodeURIComponent(article.title)}&source=${encodeURIComponent(article.source)}&date=${encodeURIComponent(article.pubDate || '')}&image=${encodeURIComponent(article.image || '')}&desc=${encodeURIComponent(article.description || '')}&section=${sectionKey}${article.custom ? '&custom=true' : ''}">
        ${article.title}
      </a>
      <div class="article-meta">
        <span class="source">${article.source}</span>
        ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
      </div>
    `;
    sidebarList.appendChild(li);
  });
  sidebarContainer.appendChild(sidebarList);
}

// Render first page of grid articles (start from article 6 now)
renderArticles(allArticles.slice(6, PAGE_SIZE + 6));
displayed = PAGE_SIZE;

  } catch (err) {
    console.error("Section load error:", err);
  }
}

// Load more button
document.getElementById("load-more-btn").addEventListener("click", () => {
  renderArticles(allArticles.slice(displayed + 1, displayed + PAGE_SIZE + 1));
});

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