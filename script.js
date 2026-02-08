const slides = document.querySelectorAll(".slide");
let currentSlide = 0;

document.getElementById("next").addEventListener("click", () => {
  slides[currentSlide].classList.remove("active");
  currentSlide = (currentSlide + 1) % slides.length;
  slides[currentSlide].classList.add("active");
});

document.getElementById("prev").addEventListener("click", () => {
  slides[currentSlide].classList.remove("active");
  currentSlide =
    (currentSlide - 1 + slides.length) % slides.length;
  slides[currentSlide].classList.add("active");
});
(function () {
  const script = document.createElement("script");
  script.src = "https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js";
  script.async = true;
  script.innerHTML = JSON.stringify({
    symbols: [
   { proName: "DJI", title: "Dow Jones" },
  { proName: "OANDA:SPX500USD", title: "S&P 500" }  ,
  { proName: "OANDA:NAS100USD", title: "NASDAQ 100" },
  { proName: "NYSE:NYA", title: "NYSE Composite" },
  { proName: "OANDA:US2000USD", title: "Russell 2000" },

  { proName: "OANDA:EURUSD", title: "USD/EURO" },
  { proName: "OANDA:USDJPY", title: "USD/JPY" },

  { proName: "TVC:GOLD", title: "Gold" },
  { proName: "TVC:SILVER", title: "Silver" },
  { proName: "TVC:USOIL", title: "Crude Oil" },
    ],
  showSymbolLogo: false,
  showChange: true,
  showPercentageChange: true,
  colorTheme: "light",      // or "dark"
  isTransparent: false,
  displayMode: "regular",
  locale: "en"
  });
  document.querySelector(".tradingview-widget-container").appendChild(script);
})();

const container = document.getElementById("top-stories");

fetch("https://gnews.io/api/v4/top-headlines?country=us&token=f64ccb8988099959940262b663ed0c24")
  .then(res => res.json())
  .then(data => {
    const container = document.getElementById("top-stories");

    data.articles.forEach(article => {
      const el = document.createElement("article");
      el.className = "story-card";
      el.innerHTML = `
        <h3>${article.title}</h3>
        <p>${article.description || ""}</p>
      `;
      container.appendChild(el);
    });
  });