// ====================
// TOP STORIES CAROUSEL
// ====================
const slides = document.querySelectorAll(".slide");
let currentSlide = 0;

if (slides.length > 0) {
  document.getElementById("next")?.addEventListener("click", () => {
    slides[currentSlide].classList.remove("active");
    currentSlide = (currentSlide + 1) % slides.length;
    slides[currentSlide].classList.add("active");
  });

  document.getElementById("prev")?.addEventListener("click", () => {
    slides[currentSlide].classList.remove("active");
    currentSlide = (currentSlide - 1 + slides.length) % slides.length;
    slides[currentSlide].classList.add("active");
  });
}

// ====================
// MARKET TICKER
// ====================
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

// ====================
// NEWS DATA (FETCH ONCE)
// ====================
let cachedArticles = [];

async function fetchNews() {
  try {
    const res = await fetch("/api/news");
    if (!res.ok) throw new Error("News API request failed");

    const data = await res.json();

    if (!data.articles || !Array.isArray(data.articles)) {
      throw new Error("Invalid news format");
    }

    cachedArticles = data.articles;

    renderLatestNews();
    renderGNews();

  } catch (err) {
    console.error("Failed to load news:", err);
  }
}

// ====================
// LATEST NEWS LIST
// ====================
function renderLatestNews() {
  const container = document.getElementById("news-container");
  if (!container || cachedArticles.length === 0) return;

  container.innerHTML = "";

  cachedArticles.slice(0, 10).forEach(article => {
    const item = document.createElement("div");
    item.className = "news-item";

    item.innerHTML = `
      <h3>
        <a href="${article.url}" target="_blank" rel="noopener noreferrer">
          ${article.title}
        </a>
      </h3>
      <p>${article.description || ""}</p>
      <span class="source">${article.source?.name || ""}</span>
    `;

    container.appendChild(item);
  });
}

// ====================
// GNEWS LEAD + LINKS
// ====================
function renderGNews() {
  if (cachedArticles.length === 0) return;

  const leadContainer = document.getElementById("gnews-lead");
  const listContainer = document.getElementById("gnews-list");

  if (!leadContainer || !listContainer) return;

  const lead = cachedArticles[0];

  leadContainer.innerHTML = `
    <article class="lead-story">
      ${lead.image ? `<img src="${lead.image}" alt="${lead.title}">` : ""}
      <div class="lead-text">
        <h2>
          <a href="${lead.url}" target="_blank" rel="noopener">
            ${lead.title}
          </a>
        </h2>
        <p>${lead.description || ""}</p>
        <span class="source">${lead.source?.name || ""}</span>
      </div>
    </article>
  `;

  listContainer.innerHTML = "";

  cachedArticles.slice(1, 8).forEach(article => {
    const li = document.createElement("li");
    li.innerHTML = `
      <a href="${article.url}" target="_blank" rel="noopener">
        ${article.title}
      </a>
    `;
    listContainer.appendChild(li);
  });
}

// ====================
// INIT
// ====================
fetchNews();
