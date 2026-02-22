const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { cleanText, normalizeSection, generateSlug } = require('./_draft-utils');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const {
      id,
      title,
      description,
      content,
      section,
      image,
      imageCaption,
      imageCredit,
      pubDate
    } = req.body || {};

    const articleId = Number(id || 0);
    if (!articleId) {
      return res.status(400).json({ error: 'Missing article id' });
    }

    const existingRows = await sql`
      SELECT id, slug, title
      FROM articles
      WHERE id = ${articleId}
      LIMIT 1
    `;

    const existing = existingRows[0];
    if (!existing) {
      return res.status(404).json({ error: 'Article not found' });
    }

    const nextTitle = cleanText(title) || existing.title;
    let nextSlug = generateSlug(nextTitle) || existing.slug;

    if (nextSlug !== existing.slug) {
      const duplicate = await sql`
        SELECT id
        FROM articles
        WHERE slug = ${nextSlug} AND id != ${articleId}
        LIMIT 1
      `;
      if (duplicate.length) {
        nextSlug = `${nextSlug}-${Date.now().toString().slice(-6)}`;
      }
    }

    await sql`
      UPDATE articles
      SET
        slug = ${nextSlug},
        title = ${nextTitle},
        description = ${cleanText(description) || ''},
        content = ${cleanText(content) || ''},
        section = ${normalizeSection(section || 'local')},
        image = ${cleanText(image) || ''},
        image_caption = ${cleanText(imageCaption) || ''},
        image_credit = ${cleanText(imageCredit) || ''},
        pub_date = ${pubDate || new Date().toISOString()},
        updated_at = NOW()
      WHERE id = ${articleId}
    `;

    return res.status(200).json({ ok: true, id: articleId, slug: nextSlug });
  } catch (error) {
    console.error('Admin update article error:', error);
    return res.status(500).json({ error: 'Failed to update article' });
  }
};
