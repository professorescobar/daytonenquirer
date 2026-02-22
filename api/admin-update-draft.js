const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { cleanText, generateSlug, normalizeSection } = require('./_draft-utils');

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
      status
    } = req.body || {};

    if (!id) {
      return res.status(400).json({ error: 'Missing draft id' });
    }

    const row = await sql`
      SELECT id, slug, status
      FROM article_drafts
      WHERE id = ${id}
      LIMIT 1
    `;

    const draft = row[0];
    if (!draft) {
      return res.status(404).json({ error: 'Draft not found' });
    }

    const nextTitle = cleanText(title);
    const nextSlug = nextTitle ? generateSlug(nextTitle) : draft.slug;
    const nextStatus = status || draft.status || 'pending_review';

    await sql`
      UPDATE article_drafts
      SET
        slug = ${nextSlug},
        title = COALESCE(${nextTitle || null}, title),
        description = COALESCE(${cleanText(description) || null}, description),
        content = COALESCE(${cleanText(content) || null}, content),
        section = COALESCE(${section ? normalizeSection(section) : null}, section),
        image = COALESCE(${cleanText(image) || null}, image),
        image_caption = COALESCE(${cleanText(imageCaption) || null}, image_caption),
        image_credit = COALESCE(${cleanText(imageCredit) || null}, image_credit),
        status = ${nextStatus},
        updated_at = NOW()
      WHERE id = ${id}
    `;

    return res.status(200).json({ ok: true, id, slug: nextSlug, status: nextStatus });
  } catch (error) {
    console.error('Update draft error:', error);
    return res.status(500).json({ error: 'Failed to update draft' });
  }
};
