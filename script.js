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
    currentSlide = (currentSlide - 1 + slides.length) % slides.length;
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
// GNEWS FEED (Lead + Toggle)
// ============================
async function loadGNews() {
  try {
    const res = await fetch("/api/news");
    if (!res.ok) throw new Error("Failed to fetch news");

    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;

    const articles = data.articles;

    // ----------------
    // LEAD STORY
    // ----------------
    const lead = articles[0];
    const leadContainer = document.getElementById("gnews-lead");

    leadContainer.innerHTML = `
      <article class="lead-story">
        <img src="${lead.image || ""}" alt="${lead.title}">
        <div class="lead-text">
          <h2>
            <a href="${lead.url}" target="_blank" rel="noopener noreferrer">
              ${lead.title}
            </a>
          </h2>
          <p>${lead.description || ""}</p>
          <span class="source">${lead.source?.name || ""}</span>
        </div>
      </article>
    `;

    // ----------------
    // HEADLINES LIST
    // ----------------
    const list = document.getElementById("gnews-list");
    list.innerHTML = "";

    articles.slice(1, 10).forEach(article => {
      const li = document.createElement("li");
      li.innerHTML = `
        <a href="${article.url}" target="_blank" rel="noopener noreferrer">
          ${article.title}
        </a>
      `;
      list.appendChild(li);
    });

  } catch (err) {
    console.error("GNews error:", err);
  }
}

// ============================
// TOGGLE HEADLINES
// ============================
const toggleBtn = document.getElementById("toggle-headlines");
const headlinesList = document.getElementById("gnews-list");

if (toggleBtn && headlinesList) {
  toggleBtn.addEventListener("click", () => {
    const isHidden = headlinesList.hasAttribute("hidden");

    if (isHidden) {
      headlinesList.removeAttribute("hidden");
      toggleBtn.textContent = "hide headlines";
      toggleBtn.setAttribute("aria-expanded", "true");
    } else {
      headlinesList.setAttribute("hidden", "");
      toggleBtn.textContent = "more headlines…";
      toggleBtn.setAttribute("aria-expanded", "false");
    }
  });
}

// ============================
// OPTIONAL SECONDARY FEED
// ============================
async function loadSecondaryNews() {
  try {
    const res = await fetch("/api/news");
    if (!res.ok) return;

    const data = await res.json();
    const container = document.getElementById("news-container");
    if (!container) return;

    container.innerHTML = "";

    data.articles.slice(10, 16).forEach(article => {
      const item = document.createElement("div");
      item.className = "news-item";
      item.innerHTML = `
        <h3>
          <a href="${article.url}" target="_blank" rel="noopener noreferrer">
            ${article.title}
          </a>
        </h3>
      `;
      container.appendChild(item);
    });
  } catch (err) {
    console.error("Secondary feed error:", err);
  }
}

// ============================
// INIT
// ============================
loadGNews();
loadSecondaryNews();

