const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { generateSlug } = require('./_draft-utils');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const { id } = req.body || {};

    if (!id) {
      return res.status(400).json({ error: 'Missing draft id' });
    }

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
        pub_date as "pubDate",
        status
      FROM article_drafts
      WHERE id = ${id}
      LIMIT 1
    `;
    const draft = rows[0];

    if (!draft) return res.status(404).json({ error: 'Draft not found' });
    if (draft.status === 'published') {
      return res.status(400).json({ error: 'Draft already published' });
    }

    let slug = draft.slug || generateSlug(draft.title);
    const exists = await sql`SELECT id FROM articles WHERE slug = ${slug} LIMIT 1`;
    if (exists.length > 0) {
      slug = `${slug}-${Date.now().toString().slice(-6)}`;
    }

    const inserted = await sql`
      INSERT INTO articles (
        slug,
        title,
        description,
        content,
        section,
        image,
        image_caption,
        image_credit,
        pub_date,
        status
      )
      VALUES (
        ${slug},
        ${draft.title},
        ${draft.description || ''},
        ${draft.content || ''},
        ${draft.section},
        ${draft.image || ''},
        ${draft.imageCaption || ''},
        ${draft.imageCredit || ''},
        ${draft.pubDate || new Date().toISOString()},
        'published'
      )
      RETURNING id
    `;

    const articleId = inserted[0].id;

    await sql`
      UPDATE article_drafts
      SET
        slug = ${slug},
        status = 'published',
        published_article_id = ${articleId},
        updated_at = NOW()
      WHERE id = ${id}
    `;

    return res.status(200).json({ ok: true, articleId, slug });
  } catch (error) {
    console.error('Publish draft error:', error);
    return res.status(500).json({ error: 'Failed to publish draft' });
  }
};
