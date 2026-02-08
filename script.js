/* ======================================================
   TOP STORIES CAROUSEL
====================================================== */
const slides = document.querySelectorAll(".slide");
let currentSlide = 0;

document.getElementById("next")?.addEventListener("click", () => {
  slides[currentSlide].classList.remove("active");
  currentSlide = (currentSlide + 1) % slides.length;
  slides[currentSlide].classList.add("active");
});

document.getElementById("prev")?.addEventListener("click", () => {
  slides[currentSlide].classList.remove("active");
  currentSlide =
    (currentSlide - 1 + slides.length) % slides.length;
  slides[currentSlide].classList.add("active");
});

/* ======================================================
   MARKET TICKER (TradingView)
====================================================== */
(function () {
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

  document
    .querySelector(".tradingview-widget-container")
    ?.appendChild(script);
})();

/* ======================================================
   WORLD NEWS (RSS via /api/world-news)
====================================================== */
async function loadWorldNews() {
  try {
    const res = await fetch("/api/world-news");
    if (!res.ok) throw new Error("World news fetch failed");

    const data = await res.json();
    if (!Array.isArray(data.articles) || data.articles.length === 0) {
      throw new Error("No world news articles returned");
    }

    const articles = data.articles;

    /* ---------- Lead Story ---------- */
    const lead = articles[0];
    const leadContainer = document.getElementById("gnews-lead");

    leadContainer.innerHTML = `
      <article class="lead-story">
        ${
          lead.image
            ? `<img src="${lead.image}" alt="${lead.title}">`
            : ""
        }
        <div class="lead-text">
          <h2>
            <a href="${lead.url}" target="_blank" rel="noopener">
              ${lead.title}
            </a>
          </h2>
          <p>${lead.description || ""}</p>
          <span class="source">${lead.source}</span>
        </div>
      </article>
    `;

    /* ---------- Expandable Headlines ---------- */
    const list = document.getElementById("gnews-list");
    list.innerHTML = "";

    articles.slice(1, 12).forEach(article => {
      const li = document.createElement("li");
      li.innerHTML = `
        <a href="${article.url}" target="_blank" rel="noopener">
          ${article.title}
        </a>
      `;
      list.appendChild(li);
    });

  } catch (err) {
    console.error("World news error:", err);
    document.getElementById("gnews-lead").innerHTML =
      "<p>World news is temporarily unavailable.</p>";
  }
}

loadWorldNews();

/* ======================================================
   HEADLINES TOGGLE (Expandable Section)
====================================================== */
const toggleBtn = document.getElementById("toggle-headlines");
const headlinesList = document.getElementById("gnews-list");

toggleBtn?.addEventListener("click", () => {
  const isExpanded = toggleBtn.getAttribute("aria-expanded") === "true";

  toggleBtn.setAttribute("aria-expanded", String(!isExpanded));
  headlinesList.hidden = isExpanded;

  toggleBtn.textContent = isExpanded
    ? "more headlines…"
    : "hide headlines";
});
