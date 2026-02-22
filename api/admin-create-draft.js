const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { generateSlug, normalizeSection, cleanText } = require('./_draft-utils');

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const section = normalizeSection(req.body?.section || 'local');
    const rawTitle = cleanText(req.body?.title || '');
    const title = rawTitle || `Draft: ${new Date().toLocaleString('en-US')}`;

    let slug = generateSlug(title);
    if (!slug) slug = `draft-${Date.now()}`;

    const exists = await sql`SELECT id FROM article_drafts WHERE slug = ${slug} LIMIT 1`;
    if (exists.length > 0) slug = `${slug}-${Date.now().toString().slice(-6)}`;

    const inserted = await sql`
      INSERT INTO article_drafts (
        slug,
        title,
        description,
        content,
        section,
        status,
        created_via,
        pub_date
      )
      VALUES (
        ${slug},
        ${title},
        '',
        '',
        ${section},
        'pending_review',
        'manual',
        ${new Date().toISOString()}
      )
      RETURNING id, slug, title, section, status
    `;

    return res.status(200).json({ ok: true, draft: inserted[0] });
  } catch (error) {
    console.error('Create draft error:', error);
    return res.status(500).json({ error: 'Failed to create manual draft' });
  }
};
