// ============================
// TOP STORIES CAROUSEL
// ============================
const slides = document.querySelectorAll(".slide");
let currentSlide = 0;

const nextBtn = document.getElementById("next");
const prevBtn = document.getElementById("prev");

if (slides.length && nextBtn && prevBtn) {
  nextBtn.addEventListener("click", () => {
    slides[currentSlide].classList.remove("active");
    currentSlide = (currentSlide + 1) % slides.length;
    slides[currentSlide].classList.add("active");
  });

  prevBtn.addEventListener("click", () => {
    slides[currentSlide].classList.remove("active");
    currentSlide =
      (currentSlide - 1 + slides.length) % slides.length;
    slides[currentSlide].classList.add("active");
  });
}

// ============================
// MARKET TICKER (TradingView)
// ============================
(function () {
  const container = document.querySelector(".tradingview-widget-container");
  if (!container) return;

  const script = document.createElement("script");
  script.src =
    "https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js";
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

// ============================
// WORLD NEWS (RSS via API)
// ============================
async function loadWorldNews() {
  try {
    const res = await fetch("/api/world-news");
    if (!res.ok) throw new Error("World news fetch failed");

    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;

    const articles = data.articles;

    // Helper function to format date
    function formatDate(dateString) {
      if (!dateString) return '';
      const date = new Date(dateString);
      const now = new Date();
      const diff = now - date;
      const hours = Math.floor(diff / (1000 * 60 * 60));
      
      if (hours < 1) return 'Just now';
      if (hours < 24) return `${hours}h ago`;
      if (hours < 48) return 'Yesterday';
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    }

// ----------------
// FEATURED STORY (first article with image overlay)
// ----------------
const featured = articles[0];
const featuredContainer = document.getElementById("featured-story");
if (featuredContainer) {
  featuredContainer.innerHTML = `
    <article class="featured-article">
      ${featured.image 
        ? `<img src="${featured.image}" alt="${featured.title}">`
        : '<div class="placeholder-image"></div>'
      }
      <div class="featured-overlay">
        <h3>
          <a href="#" data-article-url="${featured.url}" data-article-title="${featured.title}" data-article-source="${featured.source}">
            ${featured.title}
          </a>
        </h3>
        <div class="article-meta">
          <span class="source">${featured.source}</span>
          ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
        </div>
      </div>
    </article>
  `;
}

    // ----------------
    // VISIBLE HEADLINES (next 5 articles)
    // ----------------
    const headlinesList = document.getElementById("headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="#" data-article-url="${article.url}" data-article-title="${article.title}" data-article-source="${article.source}">
            ${article.title}
          </a>
          <div class="article-meta">
            <span class="source">${article.source}</span>
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>
        `;
        headlinesList.appendChild(li);
      });
    }

    // ----------------
    // MORE HEADLINES (remaining articles, hidden by default)
    // ----------------
    const moreList = document.getElementById("more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      articles.slice(6, 24).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="#" data-article-url="${article.url}" data-article-title="${article.title}" data-article-source="${article.source}">
            ${article.title}
          </a>
          <div class="article-meta">
            <span class="source">${article.source}</span>
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>
        `;
        moreList.appendChild(li);
      });
    }

  } catch (err) {
    console.error("World news error:", err);
  }
}

// ============================
// TOGGLE "MORE" HEADLINES
// ============================
const toggleMoreBtn = document.getElementById("toggle-more");
const moreHeadlinesContainer = document.getElementById("more-headlines-container");

if (toggleMoreBtn && moreHeadlinesContainer) {
  toggleMoreBtn.addEventListener("click", () => {
    const isHidden = moreHeadlinesContainer.hasAttribute("hidden");

    if (isHidden) {
      moreHeadlinesContainer.removeAttribute("hidden");
      toggleMoreBtn.setAttribute("aria-expanded", "true");
    } else {
      moreHeadlinesContainer.setAttribute("hidden", "");
      toggleMoreBtn.setAttribute("aria-expanded", "false");
    }
  });
}

// ============================
// INIT
// ============================
loadWorldNews();

// Add click handlers for article links
document.addEventListener('click', (e) => {
  const link = e.target.closest('a[data-article-url]');
  if (link) {
    e.preventDefault();
    const url = link.dataset.articleUrl;
    const title = link.dataset.articleTitle;
    const source = link.dataset.articleSource;
    showArticleSummary(title, url, source);
  }
});

// ============================
// ARTICLE SUMMARY MODAL
// ============================
const modal = document.getElementById("article-modal");
const closeModalBtn = document.getElementById("close-modal");
const modalBody = document.getElementById("modal-body");

async function showArticleSummary(title, url, source) {
  // Show modal with loading state
  modal.removeAttribute("hidden");
  modalBody.innerHTML = '<div class="loading">Loading summary...</div>';

  try {
    const response = await fetch("/api/summarize-article", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title, url, source })
    });

    if (!response.ok) throw new Error("Failed to load summary");

    const data = await response.json();

    modalBody.innerHTML = `
      <h2>${data.title}</h2>
      <div class="source-info">Source: ${data.source}</div>
      <div class="summary">${data.summary}</div>
      <a href="${data.originalUrl}" target="_blank" rel="noopener noreferrer" class="read-full">
        Read Full Article →
      </a>
    `;
  } catch (err) {
    console.error("Summary error:", err);
    modalBody.innerHTML = `
      <h2>Unable to load summary</h2>
      <p>Sorry, we couldn't generate a summary for this article.</p>
      <a href="${url}" target="_blank" rel="noopener noreferrer" class="read-full">
        Read Original Article →
      </a>
    `;
  }
}

// Close modal - make sure this runs after page loads
document.addEventListener('DOMContentLoaded', () => {
  const modal = document.getElementById("article-modal");
  const closeModalBtn = document.getElementById("close-modal");
  
  if (closeModalBtn && modal) {
    closeModalBtn.addEventListener("click", () => {
      modal.setAttribute("hidden", "");
    });

    // Close on background click
    modal.addEventListener("click", (e) => {
      if (e.target === modal) {
        modal.setAttribute("hidden", "");
      }
    });
  }
});

  // Close on background click
  modal.addEventListener("click", (e) => {
    if (e.target === modal) {
      modal.setAttribute("hidden", "");
    }
  });

