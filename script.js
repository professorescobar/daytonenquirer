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
              <a href="${featured.url}" target="_blank" rel="noopener noreferrer">
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
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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

// ============================
// NATIONAL NEWS (RSS via API)
// ============================
async function loadNationalNews() {
  try {
    const res = await fetch("/api/national-news");
    if (!res.ok) throw new Error("National news fetch failed");

    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;

    const articles = data.articles;

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

    const featured = articles[0];
    const featuredContainer = document.getElementById("national-featured-story");
    if (featuredContainer) {
      featuredContainer.innerHTML = `
        <article class="featured-article">
          ${featured.image 
            ? `<img src="${featured.image}" alt="${featured.title}">`
            : '<div class="placeholder-image"></div>'
          }
          <div class="featured-overlay">
            <h3>
              <a href="${featured.url}" target="_blank" rel="noopener noreferrer">
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

    const headlinesList = document.getElementById("national-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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

    const moreList = document.getElementById("national-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      articles.slice(6, 24).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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
    console.error("National news error:", err);
  }
}

// ============================
// BUSINESS NEWS (RSS via API)
// ============================
async function loadBusinessNews() {
  try {
    const res = await fetch("/api/business-news");
    if (!res.ok) throw new Error("Business news fetch failed");

    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;

    const articles = data.articles;

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

    const featured = articles[0];
    const featuredContainer = document.getElementById("business-featured-story");
    if (featuredContainer) {
      featuredContainer.innerHTML = `
        <article class="featured-article">
          ${featured.image 
            ? `<img src="${featured.image}" alt="${featured.title}">`
            : '<div class="placeholder-image"></div>'
          }
          <div class="featured-overlay">
            <h3>
              <a href="${featured.url}" target="_blank" rel="noopener noreferrer">
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

    const headlinesList = document.getElementById("business-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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

    const moreList = document.getElementById("business-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      articles.slice(6, 24).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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
    console.error("Business news error:", err);
  }
}

// ============================
// SPORTS NEWS (RSS via API)
// ============================
async function loadSportsNews() {
  try {
    const res = await fetch("/api/sports-news");
    if (!res.ok) throw new Error("Sports news fetch failed");

    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;

    const articles = data.articles;

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

    const featured = articles[0];
    const featuredContainer = document.getElementById("sports-featured-story");
    if (featuredContainer) {
      featuredContainer.innerHTML = `
        <article class="featured-article">
          ${featured.image 
            ? `<img src="${featured.image}" alt="${featured.title}">`
            : '<div class="placeholder-image"></div>'
          }
          <div class="featured-overlay">
            <h3>
              <a href="${featured.url}" target="_blank" rel="noopener noreferrer">
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

    const headlinesList = document.getElementById("sports-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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

    const moreList = document.getElementById("sports-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      articles.slice(6, 24).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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
    console.error("Sports news error:", err);
  }
}

// ============================
// TOGGLE BUTTONS FOR ALL SECTIONS
// ============================
const nationalToggleBtn = document.getElementById("national-toggle-more");
const nationalMoreContainer = document.getElementById("national-more-headlines-container");

if (nationalToggleBtn && nationalMoreContainer) {
  nationalToggleBtn.addEventListener("click", () => {
    const isHidden = nationalMoreContainer.hasAttribute("hidden");
    if (isHidden) {
      nationalMoreContainer.removeAttribute("hidden");
      nationalToggleBtn.setAttribute("aria-expanded", "true");
    } else {
      nationalMoreContainer.setAttribute("hidden", "");
      nationalToggleBtn.setAttribute("aria-expanded", "false");
    }
  });
}

const businessToggleBtn = document.getElementById("business-toggle-more");
const businessMoreContainer = document.getElementById("business-more-headlines-container");

if (businessToggleBtn && businessMoreContainer) {
  businessToggleBtn.addEventListener("click", () => {
    const isHidden = businessMoreContainer.hasAttribute("hidden");
    if (isHidden) {
      businessMoreContainer.removeAttribute("hidden");
      businessToggleBtn.setAttribute("aria-expanded", "true");
    } else {
      businessMoreContainer.setAttribute("hidden", "");
      businessToggleBtn.setAttribute("aria-expanded", "false");
    }
  });
}

const sportsToggleBtn = document.getElementById("sports-toggle-more");
const sportsMoreContainer = document.getElementById("sports-more-headlines-container");

if (sportsToggleBtn && sportsMoreContainer) {
  sportsToggleBtn.addEventListener("click", () => {
    const isHidden = sportsMoreContainer.hasAttribute("hidden");
    if (isHidden) {
      sportsMoreContainer.removeAttribute("hidden");
      sportsToggleBtn.setAttribute("aria-expanded", "true");
    } else {
      sportsMoreContainer.setAttribute("hidden", "");
      sportsToggleBtn.setAttribute("aria-expanded", "false");
    }
  });
}

// ============================
// LOCAL NEWS (RSS via API)
// ============================
async function loadLocalNews() {
  try {
    const res = await fetch("/api/local-news");
    if (!res.ok) throw new Error("Local news fetch failed");

    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;

    const articles = data.articles;

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

    const featured = articles[0];
    const featuredContainer = document.getElementById("local-featured-story");
    if (featuredContainer && featured) {
      featuredContainer.innerHTML = `
        <article class="featured-article">
          ${featured.image 
            ? `<img src="${featured.image}" alt="${featured.title}">`
            : '<div class="placeholder-image"></div>'
          }
          <div class="featured-overlay">
            <h3>
              <a href="${featured.url}" target="_blank" rel="noopener noreferrer">
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

    const headlinesList = document.getElementById("local-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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

    const moreList = document.getElementById("local-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      articles.slice(6, 24).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
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
    console.error("Local news error:", err);
  }
}

// Toggle button for local news
const localToggleBtn = document.getElementById("local-toggle-more");
const localMoreContainer = document.getElementById("local-more-headlines-container");

if (localToggleBtn && localMoreContainer) {
  localToggleBtn.addEventListener("click", () => {
    const isHidden = localMoreContainer.hasAttribute("hidden");
    if (isHidden) {
      localMoreContainer.removeAttribute("hidden");
      localToggleBtn.setAttribute("aria-expanded", "true");
    } else {
      localMoreContainer.setAttribute("hidden", "");
      localToggleBtn.setAttribute("aria-expanded", "false");
    }
  });
}
// ============================
// INIT
// ============================
loadLocalNews();
loadNationalNews();
loadBusinessNews();
loadSportsNews();
loadWorldNews();