const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { cleanText, generateSlug, normalizeSection } = require('./_draft-utils');

function isUniqueViolation(error) {
  const code = String(error?.code || '').trim();
  const message = String(error?.message || '').toLowerCase();
  return code === '23505' || message.includes('duplicate key value violates unique constraint');
}

function normalizeComparableTitle(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function normalizeDraftStatus(value, fallback) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return fallback;
  if (raw === 'pending_review' || raw === 'approved' || raw === 'rejected' || raw === 'draft') return raw;
  return null;
}

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
      beat,
      persona,
      image,
      imageCaption,
      imageCredit,
      status
    } = req.body || {};

    if (!id) {
      return res.status(400).json({ error: 'Missing draft id' });
    }

    const row = await sql`
      SELECT id, slug, title, status
      FROM article_drafts
      WHERE id = ${id}
      LIMIT 1
    `;

    const draft = row[0];
    if (!draft) {
      return res.status(404).json({ error: 'Draft not found' });
    }
    if (String(draft.status || '').trim().toLowerCase() === 'published') {
      return res.status(409).json({ error: 'Published drafts cannot be edited' });
    }

    const nextTitle = cleanText(title);
    const hasTitleUpdate = Boolean(nextTitle);
    const titleChanged = hasTitleUpdate
      ? normalizeComparableTitle(nextTitle) !== normalizeComparableTitle(draft.title)
      : false;
    const generatedNextSlug = titleChanged ? generateSlug(nextTitle) : '';
    const nextSlug = generatedNextSlug || draft.slug || `draft-${Number(id) || Date.now()}`;
    const nextStatus = normalizeDraftStatus(status, draft.status || 'pending_review');
    if (!nextStatus) {
      return res.status(400).json({ error: 'Invalid draft status' });
    }

    const slugCandidates = [];
    if (nextSlug) slugCandidates.push(nextSlug);
    const stableFallbackSlug = `draft-${Number(id) || Date.now()}`;
    if (!slugCandidates.includes(stableFallbackSlug)) slugCandidates.push(stableFallbackSlug);

    let lastError = null;
    for (const slugCandidate of slugCandidates) {
      try {
        const updatedRows = await sql`
          UPDATE article_drafts
          SET
            slug = ${slugCandidate},
            title = COALESCE(${nextTitle || null}, title),
            description = COALESCE(${cleanText(description) || null}, description),
            content = COALESCE(${cleanText(content) || null}, content),
            section = COALESCE(${section ? normalizeSection(section) : null}, section),
            beat = COALESCE(${cleanText(beat) || null}, beat),
            persona = COALESCE(${cleanText(persona) || null}, persona),
            image = COALESCE(${cleanText(image) || null}, image),
            image_caption = COALESCE(${cleanText(imageCaption) || null}, image_caption),
            image_credit = COALESCE(${cleanText(imageCredit) || null}, image_credit),
            status = ${nextStatus},
            updated_at = NOW()
          WHERE id = ${id}
          RETURNING id
        `;
        if (!updatedRows?.length) {
          return res.status(404).json({ error: 'Draft not found' });
        }
        return res.status(200).json({ ok: true, id, slug: slugCandidate, status: nextStatus });
      } catch (error) {
        if (!isUniqueViolation(error)) throw error;
        lastError = error;
      }
    }

    if (lastError) {
      return res.status(409).json({ error: 'Draft slug already exists' });
    }
    return res.status(500).json({ error: 'Failed to update draft' });
  } catch (error) {
    console.error('Update draft error:', error);
    return res.status(500).json({ error: 'Failed to update draft' });
  }
};
