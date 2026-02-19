// ============================
// TOP STORIES CAROUSEL
// ============================
async function loadCarousel() {
  try {
    const res = await fetch("/api/carousel-stories");
    if (!res.ok) throw new Error("Carousel fetch failed");

    const data = await res.json();
    if (!Array.isArray(data.stories) || !data.stories.length) return;

    const carouselContainer = document.querySelector(".carousel");
    if (!carouselContainer) return;

    // Format date helper function
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

    // Clear existing slides
    carouselContainer.innerHTML = '';

    // Create slides
    data.stories.forEach((story, index) => {
      const slide = document.createElement('div');
      slide.className = index === 0 ? 'slide active' : 'slide';
      const sectionKey = story.category.toLowerCase().replace(' ', '-');
      const slideLink = articleLink(story, sectionKey === 'world-news' ? 'world' : sectionKey);

      slide.innerHTML = `
        <img src="${story.image}" alt="${story.title}" />
        <div class="slide-text">
          <h2><a href="${slideLink}">${story.title}</a></h2>
          <p>${story.description ? story.description.slice(0, 150) + '...' : ''}</p>
          <span class="slide-category">${story.category} | ${formatDate(story.pubDate)}</span>
        </div>
      `;
      carouselContainer.appendChild(slide);
    });

    // Add navigation buttons
    const prevBtn = document.createElement('button');
    prevBtn.id = 'prev';
    prevBtn.setAttribute('aria-label', 'Previous story');
    prevBtn.textContent = '❮';
    
    const nextBtn = document.createElement('button');
    nextBtn.id = 'next';
    nextBtn.setAttribute('aria-label', 'Next story');
    nextBtn.textContent = '❯';
    
    carouselContainer.appendChild(prevBtn);
    carouselContainer.appendChild(nextBtn);

    // Setup carousel navigation
    const slides = document.querySelectorAll(".slide");
    let currentSlide = 0;
    let autoTimer;

    function goToSlide(index) {
      slides[currentSlide].classList.remove("active");
      currentSlide = (index + slides.length) % slides.length;
      slides[currentSlide].classList.add("active");
    }

    function startAutoTimer() {
      autoTimer = setInterval(() => {
        goToSlide(currentSlide + 1);
      }, 15000); // 15 seconds
    }

    function resetAutoTimer() {
      clearInterval(autoTimer);
      startAutoTimer();
    }

    nextBtn.addEventListener("click", () => {
      goToSlide(currentSlide + 1);
      resetAutoTimer();
    });

    prevBtn.addEventListener("click", () => {
      goToSlide(currentSlide - 1);
      resetAutoTimer();
    });

    // Start auto-cycling
    startAutoTimer();

  } catch (err) {
    console.error("Carousel error:", err);
  }
}

// ============================
// ARTICLE LINK HELPER
// ============================
function articleLink(article, section) {
  if (article.custom) {
    return `/api/article?slug=${article.url}&og=true`;
  }
  return `article.html?url=${encodeURIComponent(article.url)}&title=${encodeURIComponent(article.title)}&source=${encodeURIComponent(article.source)}&date=${encodeURIComponent(article.pubDate || '')}&image=${encodeURIComponent(article.image || '')}&desc=${encodeURIComponent(article.description || '')}&section=${section}`;
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

    const featured = articles[0];
    const featuredContainer = document.getElementById("featured-story");
    if (featuredContainer) {
      featuredContainer.innerHTML = `
        <article class="featured-article">
          ${featured.image ? `<img src="${featured.image}" alt="${featured.title}" loading="lazy">` : '<div class="placeholder-image"></div>'}
          <div class="featured-overlay">
           <h3><a href="${articleLink(featured, 'world')}">${featured.title}</a></h3>
           ${featured.description ? `<p class="featured-preview">${featured.description.slice(0, 120)}...</p>` : ''}
           <div class="article-meta">
            ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
           </div>
         </div>
       </article>`;
    }

    const headlinesList = document.getElementById("headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'world')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        headlinesList.appendChild(li);
      });
    }

    const moreList = document.getElementById("more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      const isMobile = window.innerWidth <= 768;
      const maxArticles = isMobile ? 12 : 24;
      
      articles.slice(6, maxArticles).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'world')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
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
      const hours = Math.floor((now - date) / (1000 * 60 * 60));
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
        ${featured.image ? `<img src="${featured.image}" alt="${featured.title}" loading="lazy">` : '<div class="placeholder-image"></div>'}
        <div class="featured-overlay">
         <h3><a href="${articleLink(featured, 'national')}">${featured.title}</a></h3>
         ${featured.description ? `<p class="featured-preview">${featured.description.slice(0, 120)}...</p>` : ''}
         <div class="article-meta">
          ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
         </div>
       </div>
      </article>`;
    }

    const headlinesList = document.getElementById("national-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'national')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        headlinesList.appendChild(li);
      });
    }

    const moreList = document.getElementById("national-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      const isMobile = window.innerWidth <= 768;
      const maxArticles = isMobile ? 12 : 24;
      
      articles.slice(6, maxArticles).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'national')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
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
      const hours = Math.floor((now - date) / (1000 * 60 * 60));
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
        ${featured.image ? `<img src="${featured.image}" alt="${featured.title}" loading="lazy">` : '<div class="placeholder-image"></div>'}
        <div class="featured-overlay">
         <h3><a href="${articleLink(featured, 'business')}">${featured.title}</a></h3>
         ${featured.description ? `<p class="featured-preview">${featured.description.slice(0, 120)}...</p>` : ''}
         <div class="article-meta">
          ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
         </div>
        </div>
       </article>`;
    }

    const headlinesList = document.getElementById("business-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'business')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        headlinesList.appendChild(li);
      });
    }

    const moreList = document.getElementById("business-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      const isMobile = window.innerWidth <= 768;
      const maxArticles = isMobile ? 12 : 24;
      
      articles.slice(6, maxArticles).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'business')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
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
      const hours = Math.floor((now - date) / (1000 * 60 * 60));
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
          ${featured.image ? `<img src="${featured.image}" alt="${featured.title}" loading="lazy">` : '<div class="placeholder-image"></div>'}
          <div class="featured-overlay">
            <h3><a href="${articleLink(featured, 'sports')}">${featured.title}</a></h3>
            ${featured.description ? `<p class="featured-preview">${featured.description.slice(0, 120)}...</p>` : ''}
            <div class="article-meta">
             ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
            </div>
          </div>
        </article>`;
    }

    const headlinesList = document.getElementById("sports-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'sports')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        headlinesList.appendChild(li);
      });
    }

    const moreList = document.getElementById("sports-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      const isMobile = window.innerWidth <= 768;
      const maxArticles = isMobile ? 12 : 24;
      
      articles.slice(6, maxArticles).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'sports')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        moreList.appendChild(li);
      });
    }
  } catch (err) {
    console.error("Sports news error:", err);
  }
}

// ============================
// HEALTH NEWS (RSS via API)
// ============================
async function loadHealthNews() {
  try {
    const res = await fetch("/api/health-news");
    if (!res.ok) throw new Error("Health news fetch failed");
    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;
    const articles = data.articles;

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

    const featured = articles[0];
    const featuredContainer = document.getElementById("health-featured-story");
    if (featuredContainer && featured) {
      featuredContainer.innerHTML = `
        <article class="featured-article">
          ${featured.image ? `<img src="${featured.image}" alt="${featured.title}" loading="lazy">` : '<div class="placeholder-image"></div>'}
          <div class="featured-overlay">
            <h3><a href="${articleLink(featured, 'health')}">${featured.title}</a></h3>
            ${featured.description ? `<p class="featured-preview">${featured.description.slice(0, 120)}...</p>` : ''}
            <div class="article-meta">
              ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
            </div>
          </div>
        </article>`;
    }

    const headlinesList = document.getElementById("health-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'health')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        headlinesList.appendChild(li);
      });
    }

    const moreList = document.getElementById("health-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      const isMobile = window.innerWidth <= 768;
      const maxArticles = isMobile ? 12 : 24;
      
      articles.slice(6, maxArticles).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'health')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        moreList.appendChild(li);
      });
    }
  } catch (err) {
    console.error("Health news error:", err);
  }
}

// ============================
// ENTERTAINMENT NEWS (RSS via API)
// ============================
async function loadEntertainmentNews() {
  try {
    const res = await fetch("/api/entertainment-news");
    if (!res.ok) throw new Error("Entertainment news fetch failed");
    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;
    const articles = data.articles;

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

    const featured = articles[0];
    const featuredContainer = document.getElementById("entertainment-featured-story");
    if (featuredContainer && featured) {
      featuredContainer.innerHTML = `
        <article class="featured-article">
          ${featured.image ? `<img src="${featured.image}" alt="${featured.title}" loading="lazy">` : '<div class="placeholder-image"></div>'}
          <div class="featured-overlay">
            <h3><a href="${articleLink(featured, 'entertainment')}">${featured.title}</a></h3>
            ${featured.description ? `<p class="featured-preview">${featured.description.slice(0, 120)}...</p>` : ''}
            <div class="article-meta">
              ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
            </div>
          </div>
        </article>`;
    }

    const headlinesList = document.getElementById("entertainment-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'entertainment')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        headlinesList.appendChild(li);
      });
    }

    const moreList = document.getElementById("entertainment-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      const isMobile = window.innerWidth <= 768;
      const maxArticles = isMobile ? 12 : 24;
      
      articles.slice(6, maxArticles).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'entertainment')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        moreList.appendChild(li);
      });
    }
  } catch (err) {
    console.error("Entertainment news error:", err);
  }
}

// ============================
// TECHNOLOGY NEWS (RSS via API)
// ============================
async function loadTechnologyNews() {
  try {
    const res = await fetch("/api/technology-news");
    if (!res.ok) throw new Error("Technology news fetch failed");
    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;
    const articles = data.articles;

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

    const featured = articles[0];
    const featuredContainer = document.getElementById("technology-featured-story");
    if (featuredContainer && featured) {
      featuredContainer.innerHTML = `
        <article class="featured-article">
          ${featured.image ? `<img src="${featured.image}" alt="${featured.title}" loading="lazy">` : '<div class="placeholder-image"></div>'}
          <div class="featured-overlay">
            <h3><a href="${articleLink(featured, 'technology')}">${featured.title}</a></h3>
            ${featured.description ? `<p class="featured-preview">${featured.description.slice(0, 120)}...</p>` : ''}
            <div class="article-meta">
              ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
            </div>
          </div>
        </article>`;
    }

    const headlinesList = document.getElementById("technology-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'technology')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        headlinesList.appendChild(li);
      });
    }

    const moreList = document.getElementById("technology-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      const isMobile = window.innerWidth <= 768;
      const maxArticles = isMobile ? 12 : 24;
      
      articles.slice(6, maxArticles).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'technology')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        moreList.appendChild(li);
      });
    }
  } catch (err) {
    console.error("Technology news error:", err);
  }
}

// Toggle buttons for new sections
const healthToggleBtn = document.getElementById("health-toggle-more");
const healthMoreContainer = document.getElementById("health-more-headlines-container");
if (healthToggleBtn && healthMoreContainer) {
  healthToggleBtn.addEventListener("click", () => {
    const isHidden = healthMoreContainer.hasAttribute("hidden");
    if (isHidden) {
      healthMoreContainer.removeAttribute("hidden");
      healthToggleBtn.setAttribute("aria-expanded", "true");
    } else {
      healthMoreContainer.setAttribute("hidden", "");
      healthToggleBtn.setAttribute("aria-expanded", "false");
    }
  });
}

const entertainmentToggleBtn = document.getElementById("entertainment-toggle-more");
const entertainmentMoreContainer = document.getElementById("entertainment-more-headlines-container");
if (entertainmentToggleBtn && entertainmentMoreContainer) {
  entertainmentToggleBtn.addEventListener("click", () => {
    const isHidden = entertainmentMoreContainer.hasAttribute("hidden");
    if (isHidden) {
      entertainmentMoreContainer.removeAttribute("hidden");
      entertainmentToggleBtn.setAttribute("aria-expanded", "true");
    } else {
      entertainmentMoreContainer.setAttribute("hidden", "");
      entertainmentToggleBtn.setAttribute("aria-expanded", "false");
    }
  });
}

const technologyToggleBtn = document.getElementById("technology-toggle-more");
const technologyMoreContainer = document.getElementById("technology-more-headlines-container");
if (technologyToggleBtn && technologyMoreContainer) {
  technologyToggleBtn.addEventListener("click", () => {
    const isHidden = technologyMoreContainer.hasAttribute("hidden");
    if (isHidden) {
      technologyMoreContainer.removeAttribute("hidden");
      technologyToggleBtn.setAttribute("aria-expanded", "true");
    } else {
      technologyMoreContainer.setAttribute("hidden", "");
      technologyToggleBtn.setAttribute("aria-expanded", "false");
    }
  });
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
      const hours = Math.floor((now - date) / (1000 * 60 * 60));
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
          ${featured.image ? `<img src="${featured.image}" alt="${featured.title}" loading="lazy">` : '<div class="placeholder-image"></div>'}
          <div class="featured-overlay">
            <h3><a href="${articleLink(featured, 'local')}">${featured.title}</a></h3>
            ${featured.description ? `<p class="featured-preview">${featured.description.slice(0, 120)}...</p>` : ''}
            <div class="article-meta">
              ${featured.pubDate ? `<span class="time">${formatDate(featured.pubDate)}</span>` : ''}
            </div>
          </div>
        </article>`;
    }

    const headlinesList = document.getElementById("local-headlines-list");
    if (headlinesList) {
      headlinesList.innerHTML = "";
      articles.slice(1, 6).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'local')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
        headlinesList.appendChild(li);
      });
    }

    const moreList = document.getElementById("local-more-headlines-list");
    if (moreList && articles.length > 6) {
      moreList.innerHTML = "";
      const isMobile = window.innerWidth <= 768;
      const maxArticles = isMobile ? 12 : 24;
      
      articles.slice(6, maxArticles).forEach(article => {
        const li = document.createElement("li");
        li.innerHTML = `
          <a href="${articleLink(article, 'local')}">${article.title}</a>
          <div class="article-meta">
            ${article.pubDate ? `<span class="time">${formatDate(article.pubDate)}</span>` : ''}
          </div>`;
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
loadCarousel();
loadLocalNews();
loadNationalNews();
loadWorldNews();
loadBusinessNews();
loadSportsNews();
loadHealthNews();
loadEntertainmentNews();
loadTechnologyNews();

// Auto-refresh all news sections every 5 minutes
setInterval(() => {
  loadLocalNews();
  loadNationalNews();
  loadWorldNews();
  loadBusinessNews();
  loadSportsNews();
  loadHealthNews();
  loadEntertainmentNews();
  loadTechnologyNews();
}, 5 * 60 * 1000);

// Auto-refresh carousel every 30 minutes
setInterval(() => {
  loadCarousel();
}, 30 * 60 * 1000); // 30 minutes