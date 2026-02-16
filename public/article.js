// Get parameters from URL
const params = new URLSearchParams(window.location.search);
const slug = params.get('slug');
const oldUrl = params.get('url');
const oldTitle = params.get('title');
const oldSource = params.get('source');
const oldDate = params.get('date');
const oldImage = params.get('image');
const oldDesc = params.get('desc');
const oldSection = params.get('section');
const isCustom = params.get('custom') === 'true';

// Determine if this is old format (RSS) or new format (custom article)
const isOldFormat = oldUrl && oldTitle;

async function loadArticle() {
  try {
    let article;

    if (slug && !isOldFormat) {
      // NEW FORMAT: Fetch from API using slug
      const res = await fetch(`/api/article?slug=${slug}`);
      if (!res.ok) throw new Error('Article not found');
      const data = await res.json();
      article = data.article;
    } else if (isOldFormat) {
      // OLD FORMAT: Build article object from URL params
      article = {
        url: decodeURIComponent(oldUrl),
        title: decodeURIComponent(oldTitle),
        source: decodeURIComponent(oldSource),
        pubDate: oldDate ? decodeURIComponent(oldDate) : null,
        image: oldImage ? decodeURIComponent(oldImage) : null,
        description: oldDesc ? decodeURIComponent(oldDesc) : '',
        section: oldSection,
        custom: isCustom
      };
    } else {
      throw new Error('Invalid article URL');
    }

    // Update page title and meta tags
    document.title = `${article.title} | The Dayton Enquirer`;
    
    const metaDesc = document.querySelector('meta[name="description"]');
    if (metaDesc && article.description) {
      metaDesc.setAttribute('content', article.description.slice(0, 160));
    }

    // Update Open Graph tags
    const ogTitle = document.querySelector('meta[property="og:title"]');
    const ogDesc = document.querySelector('meta[property="og:description"]');
    const ogImage = document.querySelector('meta[property="og:image"]');
    const ogUrl = document.querySelector('meta[property="og:url"]');
    
    if (ogTitle) ogTitle.setAttribute('content', article.title);
    if (ogDesc && article.description) ogDesc.setAttribute('content', article.description.slice(0, 160));
    if (ogImage) ogImage.setAttribute('content', article.image || '');
    if (ogUrl) ogUrl.setAttribute('content', window.location.href);

    // Update Twitter Card tags
    const twTitle = document.querySelector('meta[name="twitter:title"]');
    const twDesc = document.querySelector('meta[name="twitter:description"]');
    const twImage = document.querySelector('meta[name="twitter:image"]');
    
    if (twTitle) twTitle.setAttribute('content', article.title);
    if (twDesc && article.description) twDesc.setAttribute('content', article.description.slice(0, 160));
    if (twImage) twImage.setAttribute('content', article.image || '');

    // Render category badge
    const categoryEl = document.querySelector('.article-category');
    if (categoryEl && article.section) {
      const sectionConfig = {
        local: { title: "Local News" },
        national: { title: "National News" },
        world: { title: "World News" },
        business: { title: "Business" },
        sports: { title: "Sports" },
        health: { title: "Health" },
        entertainment: { title: "Entertainment" },
        technology: { title: "Technology" }
      };
      const config = sectionConfig[article.section];
      if (config) {
        categoryEl.innerHTML = `<a href="/section.html?s=${article.section}">${config.title}</a>`;
      }
    }

    // Render headline
    const headlineEl = document.querySelector('.article-content h1');
    if (headlineEl) {
      headlineEl.textContent = article.title;
    }

    // Render byline
    const bylineEl = document.querySelector('.article-byline');
    if (bylineEl && article.source) {
      let bylineHTML = `<strong>${article.source}</strong>`;
      if (article.pubDate) {
        const date = new Date(article.pubDate);
        bylineHTML += ` | ${date.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}`;
      }
      bylineEl.innerHTML = bylineHTML;
    }

    // Render image
    const imageContainer = document.querySelector('.article-image-container');
    if (imageContainer && article.image) {
      imageContainer.innerHTML = `<img src="${article.image}" alt="${article.title}" loading="lazy" />`;
    } else if (imageContainer) {
      imageContainer.remove();
    }

    // Render description (convert \n\n to paragraphs)
    const descriptionEl = document.querySelector('.article-description');
    if (descriptionEl && article.description) {
      descriptionEl.innerHTML = `<p>${article.description.replace(/\n\n/g, '</p><p>')}</p>`;
    }

    // Hide "Read Full Article" button for custom articles
    const readFullBtn = document.getElementById('article-read-full');
    if (readFullBtn) {
      if (article.custom) {
        readFullBtn.setAttribute("hidden", "");
      } else {
        readFullBtn.href = article.url;
      }
    }

    // Load related articles
    loadRelatedArticles(article.section);

  } catch (err) {
    console.error('Article load error:', err);
    const contentEl = document.querySelector('.article-content');
    if (contentEl) {
      contentEl.innerHTML = '<p>Article not found.</p>';
    }
  }
}

// Market ticker
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