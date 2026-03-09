const { neon } = require('@neondatabase/serverless');
const { getPersonaLabel } = require('../lib/personas');

function isMissingColumn(error, columnName) {
  return new RegExp(`column\\s+"?${columnName}"?\\s+does\\s+not\\s+exist`, 'i').test(String(error?.message || ''));
}

module.exports = async (req, res) => {
  const { slug, og } = req.query;
  
  if (!slug) {
    return res.status(400).json({ error: 'Slug required' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    let hasStatusColumn = true;
    let hasPersonaColumn = true;

    async function queryBySlug() {
      if (hasPersonaColumn && hasStatusColumn) {
        return sql`
          SELECT
            id,
            slug,
            title,
            description,
            content,
            section,
            persona,
            image,
            image_caption as "imageCaption",
            image_credit as "imageCredit",
            pub_date as "pubDate"
          FROM articles
          WHERE lower(trim(slug)) = lower(trim(${slug}))
            AND COALESCE(status, 'published') = 'published'
          LIMIT 1
        `;
      }
      if (hasPersonaColumn && !hasStatusColumn) {
        return sql`
          SELECT
            id,
            slug,
            title,
            description,
            content,
            section,
            persona,
            image,
            image_caption as "imageCaption",
            image_credit as "imageCredit",
            pub_date as "pubDate"
          FROM articles
          WHERE lower(trim(slug)) = lower(trim(${slug}))
          LIMIT 1
        `;
      }
      if (!hasPersonaColumn && hasStatusColumn) {
        return sql`
          SELECT
            id,
            slug,
            title,
            description,
            content,
            section,
            NULL::text as persona,
            image,
            image_caption as "imageCaption",
            image_credit as "imageCredit",
            pub_date as "pubDate"
          FROM articles
          WHERE lower(trim(slug)) = lower(trim(${slug}))
            AND COALESCE(status, 'published') = 'published'
          LIMIT 1
        `;
      }
      return sql`
        SELECT
          id,
          slug,
          title,
          description,
          content,
          section,
          NULL::text as persona,
          image,
          image_caption as "imageCaption",
          image_credit as "imageCredit",
          pub_date as "pubDate"
        FROM articles
        WHERE lower(trim(slug)) = lower(trim(${slug}))
        LIMIT 1
      `;
    }

    async function queryNeighbor(section, pubDate, id, direction) {
      const isPrev = direction === 'prev';
      if (hasPersonaColumn && hasStatusColumn) {
        return isPrev
          ? sql`
              SELECT
                id,
                slug,
                title,
                description,
                content,
                section,
                persona,
                image,
                image_caption as "imageCaption",
                image_credit as "imageCredit",
                pub_date as "pubDate"
              FROM articles
              WHERE section = ${section}
                AND COALESCE(status, 'published') = 'published'
                AND (
                  pub_date > ${pubDate}
                  OR (pub_date = ${pubDate} AND id > ${id})
                )
              ORDER BY pub_date ASC, id ASC
              LIMIT 1
            `
          : sql`
              SELECT
                id,
                slug,
                title,
                description,
                content,
                section,
                persona,
                image,
                image_caption as "imageCaption",
                image_credit as "imageCredit",
                pub_date as "pubDate"
              FROM articles
              WHERE section = ${section}
                AND COALESCE(status, 'published') = 'published'
                AND (
                  pub_date < ${pubDate}
                  OR (pub_date = ${pubDate} AND id < ${id})
                )
              ORDER BY pub_date DESC, id DESC
              LIMIT 1
            `;
      }
      if (hasPersonaColumn && !hasStatusColumn) {
        return isPrev
          ? sql`
              SELECT
                id,
                slug,
                title,
                description,
                content,
                section,
                persona,
                image,
                image_caption as "imageCaption",
                image_credit as "imageCredit",
                pub_date as "pubDate"
              FROM articles
              WHERE section = ${section}
                AND (
                  pub_date > ${pubDate}
                  OR (pub_date = ${pubDate} AND id > ${id})
                )
              ORDER BY pub_date ASC, id ASC
              LIMIT 1
            `
          : sql`
              SELECT
                id,
                slug,
                title,
                description,
                content,
                section,
                persona,
                image,
                image_caption as "imageCaption",
                image_credit as "imageCredit",
                pub_date as "pubDate"
              FROM articles
              WHERE section = ${section}
                AND (
                  pub_date < ${pubDate}
                  OR (pub_date = ${pubDate} AND id < ${id})
                )
              ORDER BY pub_date DESC, id DESC
              LIMIT 1
            `;
      }
      if (!hasPersonaColumn && hasStatusColumn) {
        return isPrev
          ? sql`
              SELECT
                id,
                slug,
                title,
                description,
                content,
                section,
                NULL::text as persona,
                image,
                image_caption as "imageCaption",
                image_credit as "imageCredit",
                pub_date as "pubDate"
              FROM articles
              WHERE section = ${section}
                AND COALESCE(status, 'published') = 'published'
                AND (
                  pub_date > ${pubDate}
                  OR (pub_date = ${pubDate} AND id > ${id})
                )
              ORDER BY pub_date ASC, id ASC
              LIMIT 1
            `
          : sql`
              SELECT
                id,
                slug,
                title,
                description,
                content,
                section,
                NULL::text as persona,
                image,
                image_caption as "imageCaption",
                image_credit as "imageCredit",
                pub_date as "pubDate"
              FROM articles
              WHERE section = ${section}
                AND COALESCE(status, 'published') = 'published'
                AND (
                  pub_date < ${pubDate}
                  OR (pub_date = ${pubDate} AND id < ${id})
                )
              ORDER BY pub_date DESC, id DESC
              LIMIT 1
            `;
      }
      return isPrev
        ? sql`
            SELECT
              id,
              slug,
              title,
              description,
              content,
              section,
              NULL::text as persona,
              image,
              image_caption as "imageCaption",
              image_credit as "imageCredit",
              pub_date as "pubDate"
            FROM articles
            WHERE section = ${section}
              AND (
                pub_date > ${pubDate}
                OR (pub_date = ${pubDate} AND id > ${id})
              )
            ORDER BY pub_date ASC, id ASC
            LIMIT 1
          `
        : sql`
            SELECT
              id,
              slug,
              title,
              description,
              content,
              section,
              NULL::text as persona,
              image,
              image_caption as "imageCaption",
              image_credit as "imageCredit",
              pub_date as "pubDate"
            FROM articles
            WHERE section = ${section}
              AND (
                pub_date < ${pubDate}
                OR (pub_date = ${pubDate} AND id < ${id})
              )
            ORDER BY pub_date DESC, id DESC
            LIMIT 1
          `;
    }
    let rows;
    try {
      rows = await queryBySlug();
    } catch (error) {
      const missingStatus = isMissingColumn(error, 'status');
      const missingPersona = isMissingColumn(error, 'persona');
      if (!missingStatus && !missingPersona) throw error;
      if (missingStatus) hasStatusColumn = false;
      if (missingPersona) hasPersonaColumn = false;
      rows = await queryBySlug();
    }

    const article = rows[0];
    if (!article) {
      return res.status(404).json({ error: 'Article not found' });
    }

    // Fetch persona data for the main article
    let author = {
      name: getPersonaLabel(article.persona),
      avatarUrl: '/images/personas/default-avatar.svg',
      disclosure: 'This article was generated by a topic engine. You can ask it questions based on the text above.'
    };

    if (article.persona) {
      try {
        const personaRows = await sql`
          SELECT display_name, avatar_url, disclosure FROM personas WHERE id = ${article.persona}
        `;
        const personaData = personaRows[0];
        if (personaData) {
          if (personaData.display_name) author.name = String(personaData.display_name);
          author.avatarUrl = personaData.avatar_url || author.avatarUrl;
          author.disclosure = personaData.disclosure || author.disclosure;
        }
      } catch (personaError) {
        console.error(`Persona data fetch failed for '${article.persona}':`, personaError.message);
      }
    }
    article.author = author;

    // Prev = newer in same section, Next = older in same section
    const prevRows = await queryNeighbor(article.section, article.pubDate, article.id, 'prev');
    const nextRows = await queryNeighbor(article.section, article.pubDate, article.id, 'next');

    const prevArticle = prevRows[0] || null;
    const nextArticle = nextRows[0] || null;

    // If og=true, return HTML with Open Graph tags for social sharing
    if (og === 'true') {
      const escapeHtml = (value) => String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
      const safeDescription = escapeHtml((article.description || '').slice(0, 160));
      const safeTitle = escapeHtml(article.title || '');
      const safeImage = escapeHtml(article.image || '');
      const encodedSlug = encodeURIComponent(String(slug || ''));
      const safeOgUrl = escapeHtml(`https://thedaytonenquirer.com/article.html?slug=${encodedSlug}`);
      const safeRedirectJs = JSON.stringify(`/article.html?slug=${encodedSlug}`)
        .replace(/</g, '\\u003c')
        .replace(/>/g, '\\u003e')
        .replace(/&/g, '\\u0026');
      const html = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${safeTitle} | The Dayton Enquirer</title>
  <meta name="description" content="${safeDescription}">
  
  <!-- Open Graph for Facebook/LinkedIn -->
  <meta property="og:type" content="article">
  <meta property="og:site_name" content="The Dayton Enquirer">
  <meta property="og:title" content="${safeTitle}">
  <meta property="og:description" content="${safeDescription}">
  <meta property="og:image" content="${safeImage}">
  <meta property="og:url" content="${safeOgUrl}">
  
  <!-- Twitter Card -->
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${safeTitle}">
  <meta name="twitter:description" content="${safeDescription}">
  <meta name="twitter:image" content="${safeImage}">
  
  <link rel="stylesheet" href="/styles.css" />
  
<!-- Immediate redirect -->
  <script>
    window.location.href = ${safeRedirectJs};
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
