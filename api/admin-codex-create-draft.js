const { neon } = require('@neondatabase/serverless');
const { generateSlug, normalizeSection, cleanText, truncate } = require('./_draft-utils');

function getCodexToken(req) {
  const authHeader = String(req.headers.authorization || '');
  const bearer = authHeader.startsWith('Bearer ')
    ? authHeader.slice('Bearer '.length).trim()
    : '';
  return (
    bearer ||
    req.headers['x-codex-token'] ||
    req.query.token ||
    ''
  );
}

function requireCodexAutomation(req, res) {
  if (String(process.env.CODEX_AUTOMATION_ENABLED || '').toLowerCase() !== 'true') {
    res.status(403).json({ error: 'Codex automation is disabled' });
    return false;
  }

  const expected = cleanText(process.env.CODEX_AUTOMATION_TOKEN || '');
  if (!expected) {
    res.status(500).json({ error: 'Missing CODEX_AUTOMATION_TOKEN env var' });
    return false;
  }

  const token = cleanText(getCodexToken(req));
  if (!token || token !== expected) {
    res.status(401).json({ error: 'Unauthorized' });
    return false;
  }

  return true;
}

function normalizeDate(value) {
  const raw = cleanText(value || '');
  if (!raw) return null;
  const timestamp = Date.parse(raw);
  if (Number.isNaN(timestamp)) return null;
  return new Date(timestamp).toISOString();
}

function sanitizeKey(value) {
  return cleanText(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9:_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 120);
}

module.exports = async (req, res) => {
  if (!requireCodexAutomation(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    if (!process.env.DATABASE_URL) {
      return res.status(500).json({ error: 'Missing DATABASE_URL env var' });
    }

    const sql = neon(process.env.DATABASE_URL);

    const draftId = Number(req.body?.id || req.body?.draftId || 0);
    const section = normalizeSection(req.body?.section || 'local');
    const title = truncate(cleanText(req.body?.title || ''), 240);
    const description = truncate(cleanText(req.body?.description || ''), 1200);
    const content = truncate(cleanText(req.body?.content || ''), 60000);
    const image = truncate(cleanText(req.body?.image || ''), 2000);
    const imageCaption = truncate(cleanText(req.body?.imageCaption || ''), 400);
    const imageCredit = truncate(cleanText(req.body?.imageCredit || ''), 240);
    const sourceTitle = truncate(cleanText(req.body?.sourceTitle || ''), 500);
    const sourceUrlInput = truncate(cleanText(req.body?.sourceUrl || ''), 2000);
    const model = truncate(cleanText(req.body?.model || 'codex'), 160);
    const idempotencyKey = sanitizeKey(req.body?.idempotencyKey || '');
    const updateOnDuplicate = req.body?.updateOnDuplicate !== false;
    const sourcePublishedAt = normalizeDate(req.body?.sourcePublishedAt || '');

    if (!title) return res.status(400).json({ error: 'Title is required' });
    if (!content) return res.status(400).json({ error: 'Content is required' });

    async function resolveUniqueSlug(nextTitle, currentId) {
      let nextSlug = generateSlug(nextTitle);
      if (!nextSlug) nextSlug = `codex-draft-${Date.now()}`;

      const existingSlug = await sql`
        SELECT id
        FROM article_drafts
        WHERE slug = ${nextSlug}
          AND id <> ${currentId}
        LIMIT 1
      `;
      if (existingSlug.length > 0) {
        nextSlug = `${nextSlug}-${Date.now().toString().slice(-6)}`;
      }
      return nextSlug;
    }

    async function updateDraftById(targetId, options = {}) {
      const currentRows = await sql`
        SELECT id, slug, title, section, status
        FROM article_drafts
        WHERE id = ${targetId}
          AND created_via = 'codex_automation'
        LIMIT 1
      `;
      const current = currentRows[0];
      if (!current) {
        return null;
      }

      const nextSlug = title
        ? await resolveUniqueSlug(title, current.id)
        : current.slug;

      await sql`
        UPDATE article_drafts
        SET
          slug = ${nextSlug},
          title = ${title},
          description = ${description},
          content = ${content},
          section = ${section},
          image = ${image || null},
          image_caption = ${imageCaption || null},
          image_credit = ${imageCredit || null},
          source_url = ${options.nextSourceUrl || null},
          source_title = ${sourceTitle || null},
          source_published_at = ${sourcePublishedAt},
          model = ${model},
          status = 'pending_review',
          updated_at = NOW()
        WHERE id = ${current.id}
      `;

      const updatedRows = await sql`
        SELECT id, slug, title, section, status
        FROM article_drafts
        WHERE id = ${current.id}
        LIMIT 1
      `;
      return updatedRows[0];
    }

    if (Number.isInteger(draftId) && draftId > 0) {
      const updatedDraft = await updateDraftById(draftId, {
        nextSourceUrl: sourceUrlInput || null
      });
      if (!updatedDraft) {
        return res.status(404).json({ error: 'Codex draft not found' });
      }
      return res.status(200).json({ ok: true, updated: true, deduped: false, draft: updatedDraft });
    }

    const syntheticSourceUrl = idempotencyKey ? `codex://automation/${idempotencyKey}` : '';
    const sourceUrl = sourceUrlInput || syntheticSourceUrl;

    if (syntheticSourceUrl) {
      const duplicateByKey = await sql`
        SELECT id, slug, title, section, status
        FROM article_drafts
        WHERE created_via = 'codex_automation'
          AND source_url = ${syntheticSourceUrl}
        ORDER BY created_at DESC
        LIMIT 1
      `;
      if (duplicateByKey[0]) {
        if (!updateOnDuplicate) {
          return res.status(200).json({ ok: true, deduped: true, draft: duplicateByKey[0] });
        }

        const updatedDraft = await updateDraftById(duplicateByKey[0].id, {
          nextSourceUrl: sourceUrlInput || syntheticSourceUrl
        });
        if (!updatedDraft) {
          return res.status(404).json({ error: 'Codex draft not found' });
        }
        return res.status(200).json({ ok: true, deduped: true, updated: true, draft: updatedDraft });
      }
    }

    const duplicateByTitle = await sql`
      SELECT id, slug, title, section, status
      FROM article_drafts
      WHERE created_via = 'codex_automation'
        AND lower(title) = lower(${title})
        AND section = ${section}
        AND created_at > NOW() - INTERVAL '12 hours'
      ORDER BY created_at DESC
      LIMIT 1
    `;
    if (duplicateByTitle[0]) {
      return res.status(200).json({ ok: true, deduped: true, draft: duplicateByTitle[0] });
    }

    const slug = await resolveUniqueSlug(title, 0);

    const inserted = await sql`
      INSERT INTO article_drafts (
        slug,
        title,
        description,
        content,
        section,
        image,
        image_caption,
        image_credit,
        source_url,
        source_title,
        source_published_at,
        pub_date,
        model,
        created_via,
        status
      )
      VALUES (
        ${slug},
        ${title},
        ${description},
        ${content},
        ${section},
        ${image || null},
        ${imageCaption || null},
        ${imageCredit || null},
        ${sourceUrl || null},
        ${sourceTitle || null},
        ${sourcePublishedAt},
        ${new Date().toISOString()},
        ${model},
        'codex_automation',
        'pending_review'
      )
      RETURNING id, slug, title, section, status
    `;

    return res.status(200).json({ ok: true, deduped: false, draft: inserted[0] });
  } catch (error) {
    console.error('Codex create draft error:', error);
    return res.status(500).json({ error: 'Failed to create codex draft', details: error.message });
  }
};
