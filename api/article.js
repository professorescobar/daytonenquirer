const { neon } = require('@neondatabase/serverless');

module.exports = async (req, res) => {
  const { slug, og } = req.query;
  
  if (!slug) {
    return res.status(400).json({ error: 'Slug required' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    
    // Find article by slug
    const results = await sql`
      SELECT 
        id,
        slug,
        title,
        description,
        content,
        section,
        image,
        image_caption as "imageCaption",
        image_credit as "imageCredit",
        pub_date as "pubDate"
      FROM articles
      WHERE slug = ${slug} AND status = 'published'
      LIMIT 1
    `;
    
    if (!results || results.length === 0) {
      return res.status(404).json({ error: 'Article not found' });
    }
    
    const article = results[0];
    
    // Add url field for compatibility with frontend
    article.url = article.slug;
    
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
  
  <link rel="stylesheet" href="/styles.css" />
  
<!-- Immediate redirect -->
  <script>
    window.location.href = '/article.html?slug=${slug}';
  </script>
</head>
<body>
  <main class="container">
    <p>Loading article...</p>
  </main>
</body>
</html>
      `;
      res.setHeader('Content-Type', 'text/html');
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
      return res.status(200).send(html);
    }
    
    // Otherwise return JSON
    res.status(200).json({ article });
    
  } catch (error) {
    console.error('Article fetch error:', error);
    return res.status(500).json({ error: 'Failed to fetch article' });
  }
};