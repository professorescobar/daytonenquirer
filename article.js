// ============================
// ARTICLE PAGE
// ============================

const sectionConfig = {
  local:         { title: "Local News",      api: "/api/local-news" },
  national:      { title: "National News",   api: "/api/national-news" },
  world:         { title: "World News",      api: "/api/world-news" },
  business:      { title: "Business",        api: "/api/business-news" },
  sports:        { title: "Sports",          api: "/api/sports-news" },
  health:        { title: "Health",          api: "/api/health-news" },
  entertainment: { title: "Entertainment",   api: "/api/entertainment-news" },
  technology:    { title: "Technology",      api: "/api/technology-news" }
};

// Get params from URL
const params = new URLSearchParams(window.location.search);
const articleUrl = params.get('url');
const articleTitle = params.get('title');
const articleSource = params.get('source');
const articleDate = params.get('date');
const articleImage = params.get('image');
const articleDescription = params.get('desc');
const articleSection = params.get('section');

function formatDate(dateString) {
  if (!dateString) return '';
  const date = new Date(dateString);
  const now = new Date();
  const hours = Math.floor((now - date) / (1000 * 60 * 60));
  if (hours < 1) return 'Just now';
  if (hours < 24) return `${hours}h ago`;
  if (hours < 48) return 'Yesterday';
  return date.toLocaleDateString('en-US', {
    weekday: 'long',
    month: 'long',
    day: 'numeric',
    year: 'numeric'
  });
}

async function loadArticle() {
  const loading = document.getElementById("article-loading");
  const content = document.getElementById("article-content");

  // Set page title
  if (articleTitle) {
    document.title = `${decodeURIComponent(articleTitle)} | The Dayton Enquirer`;
  }

  // Fill in article details from URL params
  const titleEl = document.getElementById("article-title");
  const sourceEl = document.getElementById("article-source");
  const sourceNameEl = document.getElementById("article-source-name");
  const dateEl = document.getElementById("article-date");
  const imageContainer = document.getElementById("article-image-container");
  const descriptionEl = document.getElementById("article-description");
  const readFullBtn = document.getElementById("article-read-full");
  const categoryEl = document.getElementById("article-category");

  if (titleEl && articleTitle) {
    titleEl.textContent = decodeURIComponent(articleTitle);
  }

  if (sourceEl && articleSource) {
    sourceEl.textContent = decodeURIComponent(articleSource);
  }

  if (sourceNameEl && articleSource) {
    sourceNameEl.textContent = decodeURIComponent(articleSource);
  }

  if (dateEl && articleDate) {
    dateEl.textContent = formatDate(decodeURIComponent(articleDate));
  }

  if (categoryEl && articleSection) {
    const config = sectionConfig[articleSection];
    if (config) {
      categoryEl.innerHTML = `<a href="/section.html?s=${articleSection}">${config.title}</a>`;
    }
  }

  if (imageContainer && articleImage) {
    const decoded = decodeURIComponent(articleImage);
    if (decoded) {
      imageContainer.innerHTML = `<img src="${decoded}" alt="${decodeURIComponent(articleTitle || '')}" />`;
    }
  }

  if (descriptionEl && articleDescription) {
    const decoded = decodeURIComponent(articleDescription);
    if (decoded) {
      descriptionEl.innerHTML = `<p>${decoded.replace(/\n\n/g, '</p><p>')}</p>`;
    }
  }

 // Handle custom articles (no external URL)
const decodedUrl = articleUrl ? decodeURIComponent(articleUrl) : '';
const isCustom = decodedUrl.startsWith('custom-');

if (readFullBtn) {
  if (isCustom) {
    readFullBtn.setAttribute("hidden", "");
  } else {
    readFullBtn.href = decodedUrl;
  }
}

  // Show content, hide loading
  if (loading) loading.setAttribute("hidden", "");
  if (content) content.removeAttribute("hidden");

  // Try to get extended summary from summarize API
  if (articleUrl && articleTitle && articleSource && !isCustom) {
    try {
      const summaryRes = await fetch("/api/summarize-article", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url: decodeURIComponent(articleUrl),
          title: decodeURIComponent(articleTitle),
          source: decodeURIComponent(articleSource),
          description: decodeURIComponent(articleDescription || '')
        })
      });

      if (summaryRes.ok) {
        const summaryData = await summaryRes.json();
        if (summaryData.summary && summaryData.summary.length > 50) {
          const summarySection = document.getElementById("article-summary");
          const summaryText = document.getElementById("article-summary-text");
          if (summarySection && summaryText) {
            summaryText.innerHTML = `<p>${summaryData.summary}</p>`;
            summarySection.removeAttribute("hidden");
          }
        }
      }
    } catch (err) {
      console.error("Summary fetch error:", err);
      // Fail silently - description is already showing
    }
  }

  // Load related articles
  loadRelated();
}

async function loadRelated() {
  if (!articleSection || !sectionConfig[articleSection]) return;

  try {
    const config = sectionConfig[articleSection];
    const res = await fetch(config.api);
    if (!res.ok) return;

    const data = await res.json();
    if (!Array.isArray(data.articles) || !data.articles.length) return;

    // Filter out current article
    const related = data.articles
      .filter(a => a.url !== decodeURIComponent(articleUrl || ''))
      .slice(0, 6);

    if (!related.length) return;

    const relatedSection = document.getElementById("related-section");
    const relatedGrid = document.getElementById("related-grid");

    if (!relatedSection || !relatedGrid) return;

    related.forEach(article => {
      const card = document.createElement("div");
      card.className = "related-card";
      card.innerHTML = `
        ${article.image ? `<img src="${article.image}" alt="${article.title}" class="related-card-image">` : ''}
        <div class="related-card-body">
          <h4>
            <a href="/article.html?url=${encodeURIComponent(article.url)}&title=${encodeURIComponent(article.title)}&source=${encodeURIComponent(article.source)}&date=${encodeURIComponent(article.pubDate || '')}&image=${encodeURIComponent(article.image || '')}&desc=${encodeURIComponent(article.description || '')}&section=${articleSection}">
              ${article.title}
            </a>
          </h4>
          <div class="article-meta">
            <span class="source">${article.source}</span>
          </div>
        </div>
      `;
      relatedGrid.appendChild(card);
    });

    relatedSection.removeAttribute("hidden");

  } catch (err) {
    console.error("Related articles error:", err);
  }
}

loadArticle();