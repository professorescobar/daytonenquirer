const { neon } = require('@neondatabase/serverless');
const { normalizeSection, cleanText, truncate } = require('./_draft-utils');

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

    const id = Number(req.body?.id || 0);
    const idempotencyKey = sanitizeKey(req.body?.idempotencyKey || '');

    if ((!Number.isInteger(id) || id <= 0) && !idempotencyKey) {
      return res.status(400).json({ error: 'Provide a valid draft id or idempotencyKey' });
    }

    const title = truncate(cleanText(req.body?.title || ''), 240);
    const description = truncate(cleanText(req.body?.description || ''), 1200);
    const content = truncate(cleanText(req.body?.content || ''), 60000);
    const sectionRaw = cleanText(req.body?.section || '');
    const image = truncate(cleanText(req.body?.image || ''), 2000);
    const imageCaption = truncate(cleanText(req.body?.imageCaption || ''), 400);
    const imageCredit = truncate(cleanText(req.body?.imageCredit || ''), 240);
    const sourceTitle = truncate(cleanText(req.body?.sourceTitle || ''), 500);
    const sourceUrl = truncate(cleanText(req.body?.sourceUrl || ''), 2000);
    const model = truncate(cleanText(req.body?.model || ''), 160);
    const sourcePublishedAt = normalizeDate(req.body?.sourcePublishedAt || '');

    const hasAnyUpdateField = Boolean(
      title ||
      description ||
      content ||
      sectionRaw ||
      image ||
      imageCaption ||
      imageCredit ||
      sourceTitle ||
      sourceUrl ||
      model ||
      sourcePublishedAt
    );

    if (!hasAnyUpdateField) {
      return res.status(400).json({ error: 'No update fields provided' });
    }

    let targetRows;
    if (Number.isInteger(id) && id > 0) {
      targetRows = await sql`
        SELECT id, slug, title, section, status
        FROM article_drafts
        WHERE id = ${id}
          AND created_via = 'codex_automation'
        LIMIT 1
      `;
    } else {
      const syntheticSourceUrl = `codex://automation/${idempotencyKey}`;
      targetRows = await sql`
        SELECT id, slug, title, section, status
        FROM article_drafts
        WHERE created_via = 'codex_automation'
          AND source_url = ${syntheticSourceUrl}
        ORDER BY created_at DESC
        LIMIT 1
      `;
    }

    const target = targetRows?.[0];
    if (!target) {
      return res.status(404).json({ error: 'Codex draft not found' });
    }

    const nextSection = sectionRaw ? normalizeSection(sectionRaw) : null;

    await sql`
      UPDATE article_drafts
      SET
        title = COALESCE(${title || null}, title),
        description = COALESCE(${description || null}, description),
        content = COALESCE(${content || null}, content),
        section = COALESCE(${nextSection}, section),
        image = COALESCE(${image || null}, image),
        image_caption = COALESCE(${imageCaption || null}, image_caption),
        image_credit = COALESCE(${imageCredit || null}, image_credit),
        source_title = COALESCE(${sourceTitle || null}, source_title),
        source_url = COALESCE(${sourceUrl || null}, source_url),
        source_published_at = COALESCE(${sourcePublishedAt}, source_published_at),
        model = COALESCE(${model || null}, model),
        updated_at = NOW()
      WHERE id = ${target.id}
    `;

    const updatedRows = await sql`
      SELECT id, slug, title, section, status
      FROM article_drafts
      WHERE id = ${target.id}
      LIMIT 1
    `;

    return res.status(200).json({ ok: true, draft: updatedRows[0] });
  } catch (error) {
    console.error('Codex update draft error:', error);
    return res.status(500).json({ error: 'Failed to update codex draft', details: error.message });
  }
};
