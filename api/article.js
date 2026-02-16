const getCustomArticles = require('./custom-articles');

module.exports = async (req, res) => {
  const { slug, og } = req.query;
  
  if (!slug) {
    return res.status(400).json({ error: 'Slug required' });
  }

  // Get all custom articles
  const allCustoms = [
    ...getCustomArticles('local'),
    ...getCustomArticles('national'),
    ...getCustomArticles('world'),
    ...getCustomArticles('business'),
    ...getCustomArticles('sports'),
    ...getCustomArticles('health'),
    ...getCustomArticles('entertainment'),
    ...getCustomArticles('technology'),
    ...getCustomArticles('all')
  ];

  // Find article by slug
  const article = allCustoms.find(a => a.url === slug);

  if (!article) {
    return res.status(404).json({ error: 'Article not found' });
  }

  // If og=true, return HTML with Open Graph tags for social sharing
  if (og === 'true') {
    const html = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${article.title} | The Dayton Enquirer</title>
  <meta name="description" content="${article.description.slice(0, 160).replace(/"/g, '&quot;')}">
  
  <!-- Open Graph for Facebook/LinkedIn -->
  <meta property="og:type" content="article">
  <meta property="og:site_name" content="The Dayton Enquirer">
  <meta property="og:title" content="${article.title.replace(/"/g, '&quot;')}">
  <meta property="og:description" content="${article.description.slice(0, 160).replace(/"/g, '&quot;')}">
  <meta property="og:image" content="${article.image || ''}">
  <meta property="og:url" content="https://thedaytonenquirer.com/article.html?slug=${slug}">
  
  <!-- Twitter Card -->
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${article.title.replace(/"/g, '&quot;')}">
  <meta name="twitter:description" content="${article.description.slice(0, 160).replace(/"/g, '&quot;')}">
  <meta name="twitter:image" content="${article.image || ''}">
  
  <!-- Redirect to actual article page -->
  <meta http-equiv="refresh" content="0; url=/article.html?slug=${slug}">
  <script>window.location.href = '/article.html?slug=${slug}';</script>
  
  <link rel="stylesheet" href="/styles.css" />
</head>
<body>
  <p>Redirecting to article...</p>
</body>
</html>
    `;

    res.setHeader('Content-Type', 'text/html');
    return res.status(200).send(html);
  }

  // Otherwise return JSON
  res.status(200).json({ article });
};