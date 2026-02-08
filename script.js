// --------------------
// TOP STORIES CAROUSEL
// --------------------
const slides = document.querySelectorAll(".slide");
let currentSlide = 0;

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

// --------------------
// MARKET TICKER
// --------------------
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

// --------------------
// INTERNAL NEWS API (/api/news)
// --------------------
async function loadNews() {
  try {
    const res = await fetch("/api/news");
    if (!res.ok) throw new Error("API response failed");

    const data = await res.json();
    const container = document.getElementById("news-container");
    container.innerHTML = "";

    data.articles.slice(0, 10).forEach(article => {
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
  } catch (err) {
    console.error("News load failed:", err);
  }
}

loadNews();

// --------------------
// GNEWS FEED (LEAD + LINKS)
// --------------------
fetch(
  "https://gnews.io/api/v4/top-headlines?country=us&max=6&token=YOUR_GNEWS_KEY"
)
  .then(res => res.json())
  .then(data => {
    if (!data.articles?.length) return;

    const lead = data.articles[0];
    const leadContainer = document.getElementById("gnews-lead");
    const listContainer = document.getElementById("gnews-list");

    // Lead story
    leadContainer.innerHTML = `
      <article class="gnews-lead">
        ${
          lead.image
            ? `<img src="${lead.image}" alt="${lead.title}">`
            : ""
        }
        <h2>
          <a href="${lead.url}" target="_blank" rel="noopener noreferrer">
            ${lead.title}
          </a>
        </h2>
        <p>${lead.description || ""}</p>
      </article>
    `;

    // Remaining links
    data.articles.slice(1).forEach(article => {
      const li = document.createElement("li");
      li.innerHTML = `
        <a href="${article.url}" target="_blank" rel="noopener noreferrer">
          ${article.title}
        </a>
      `;
      listContainer.appendChild(li);
    });
  })
  .catch(err => console.error("GNews failed:", err));

