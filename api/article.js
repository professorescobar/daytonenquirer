const { neon } = require('@neondatabase/serverless');

module.exports = async (req, res) => {
  const { slug, og } = req.query;
  
  if (!slug) {
    return res.status(400).json({ error: 'Slug required' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);

    const rows = await sql`
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

    const article = rows[0];
    if (!article) {
      return res.status(404).json({ error: 'Article not found' });
    }

    // Prev = newer in same section, Next = older in same section
    const prevRows = await sql`
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
      WHERE section = ${article.section}
        AND status = 'published'
        AND (
          pub_date > ${article.pubDate}
          OR (pub_date = ${article.pubDate} AND id > ${article.id})
        )
      ORDER BY pub_date ASC, id ASC
      LIMIT 1
    `;

    const nextRows = await sql`
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
      WHERE section = ${article.section}
        AND status = 'published'
        AND (
          pub_date < ${article.pubDate}
          OR (pub_date = ${article.pubDate} AND id < ${article.id})
        )
      ORDER BY pub_date DESC, id DESC
      LIMIT 1
    `;

    const prevArticle = prevRows[0] || null;
    const nextArticle = nextRows[0] || null;

    // If og=true, return HTML with Open Graph tags for social sharing
    if (og === 'true') {
      const safeDescription = (article.description || '').slice(0, 160).replace(/"/g, '&quot;');
      const safeTitle = (article.title || '').replace(/"/g, '&quot;');
      const html = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${article.title} | The Dayton Enquirer</title>
  <meta name="description" content="${safeDescription}">
  
  <!-- Open Graph for Facebook/LinkedIn -->
  <meta property="og:type" content="article">
  <meta property="og:site_name" content="The Dayton Enquirer">
  <meta property="og:title" content="${safeTitle}">
  <meta property="og:description" content="${safeDescription}">
  <meta property="og:image" content="${article.image || ''}">
  <meta property="og:url" content="https://thedaytonenquirer.com/article.html?slug=${slug}">
  
  <!-- Twitter Card -->
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${safeTitle}">
  <meta name="twitter:description" content="${safeDescription}">
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
    return res.status(200).json({ article, prevArticle, nextArticle });
  } catch (error) {
    console.error('Article API database error:', error);
    return res.status(500).json({ error: 'Failed to fetch article' });
  }
};
