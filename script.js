// ==============================
// PRICE TICKER CODE
// ==============================

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

(function () {
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
      { proName: "OANDA:EURUSD", title: "USD/EUR" },
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


// ==============================
// NEWS CODE (Vercel API)
// ==============================

async function loadNews() {
  try {
    const res = await fetch("/api/news");
    const data = await res.json();

    const newsContainer = document.getElementById("news-container");
    if (!newsContainer) return;

    newsContainer.innerHTML = "";

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
        <span class="source">${article.source.name}</span>
      `;

      newsContainer.appendChild(item);
    });
  } catch (err) {
    console.error("News load failed", err);
  }
}

loadNews();