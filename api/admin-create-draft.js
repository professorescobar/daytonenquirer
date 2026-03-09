const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { generateSlug, normalizeSection, cleanText } = require('./_draft-utils');

function isUniqueViolation(error) {
  const code = String(error?.code || '').trim();
  const message = String(error?.message || '').toLowerCase();
  return code === '23505' || message.includes('duplicate key value violates unique constraint');
}

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

    const baseSlug = generateSlug(title) || `draft-${Date.now()}`;
    const slugCandidates = [
      baseSlug,
      `${baseSlug}-${Date.now().toString().slice(-6)}`,
      `${baseSlug}-${Math.random().toString(36).slice(2, 8)}`
    ];

    let insertedDraft = null;
    let lastError = null;
    for (const slug of slugCandidates) {
      try {
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
        insertedDraft = inserted?.[0] || null;
        if (insertedDraft) break;
      } catch (error) {
        if (!isUniqueViolation(error)) throw error;
        lastError = error;
      }
    }

    if (!insertedDraft) {
      if (lastError) {
        return res.status(409).json({ error: 'Draft slug already exists' });
      }
      return res.status(500).json({ error: 'Failed to create manual draft' });
    }

    return res.status(200).json({ ok: true, draft: insertedDraft });
  } catch (error) {
    console.error('Create draft error:', error);
    return res.status(500).json({ error: 'Failed to create manual draft' });
  }
};
