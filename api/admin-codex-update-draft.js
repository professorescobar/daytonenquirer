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

function isUniqueViolation(error) {
  const code = String(error?.code || '').trim();
  const message = String(error?.message || '').toLowerCase();
  return code === '23505' || message.includes('duplicate key value violates unique constraint');
}

function getUniqueViolationMessage(error) {
  const constraint = cleanText(error?.constraint || '', 160).toLowerCase();
  const detail = cleanText(error?.detail || '', 500).toLowerCase();
  const message = cleanText(error?.message || '', 500).toLowerCase();
  const haystack = `${constraint} ${detail} ${message}`;

  if (haystack.includes('codex_idempotency_key')) {
    return 'Codex idempotency key already exists';
  }
  if (haystack.includes('source_url')) {
    return 'Draft source URL already exists';
  }
  if (haystack.includes('slug')) {
    return 'Draft slug already exists';
  }
  return 'Draft already exists';
}

async function ensureCodexIdempotencySchema(sql) {
  const columnRows = await sql`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'article_drafts'
        AND column_name = 'codex_idempotency_key'
    ) AS present
  `;
  const hasColumn = Boolean(columnRows?.[0]?.present);
  const indexRows = await sql`
    SELECT to_regclass('public.uq_article_drafts_codex_idempotency_key_norm') AS name
  `;
  const hasNormalizedUniqueIndex = Boolean(indexRows?.[0]?.name);
  if (!hasColumn || !hasNormalizedUniqueIndex) {
    const error = new Error('Codex idempotency schema not ready. Apply migrations 20260309_23 and 20260309_24.');
    error.statusCode = 503;
    throw error;
  }
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
    await ensureCodexIdempotencySchema(sql);

    const id = Number(req.body?.id || 0);
    const rawIdempotencyKey = cleanText(req.body?.idempotencyKey || '', 240);
    const idempotencyKey = sanitizeKey(rawIdempotencyKey);
    if (rawIdempotencyKey && !idempotencyKey) {
      return res.status(400).json({ error: 'idempotencyKey is invalid after normalization' });
    }

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
    const isIdLookup = Number.isInteger(id) && id > 0;
    if (Number.isInteger(id) && id > 0) {
      targetRows = await sql`
        SELECT id, slug, title, section, status, source_url as "sourceUrl", codex_idempotency_key as "codexIdempotencyKey"
        FROM article_drafts
        WHERE id = ${id}
          AND created_via = 'codex_automation'
        LIMIT 1
      `;
    } else {
      targetRows = await sql`
        SELECT id, slug, title, section, status, source_url as "sourceUrl", codex_idempotency_key as "codexIdempotencyKey"
        FROM article_drafts
        WHERE created_via = 'codex_automation'
          AND lower(trim(codex_idempotency_key)) = ${idempotencyKey}
        ORDER BY created_at DESC
        LIMIT 1
      `;
    }

    const target = targetRows?.[0];
    if (!target) {
      return res.status(404).json({ error: 'Codex draft not found' });
    }
    const existingIdempotencyKey = cleanText(target.codexIdempotencyKey || '').toLowerCase();
    if (isIdLookup && idempotencyKey && existingIdempotencyKey && existingIdempotencyKey !== idempotencyKey) {
      return res.status(409).json({ error: 'Codex idempotency key mismatch for this draft' });
    }

    const nextSection = sectionRaw ? normalizeSection(sectionRaw) : null;
    const nextSourceUrl = sourceUrl || null;
    const nextIdempotencyKey = idempotencyKey || cleanText(target.codexIdempotencyKey || '') || null;

    const updateRows = await sql`
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
        source_url = COALESCE(${nextSourceUrl}, source_url),
        source_published_at = COALESCE(${sourcePublishedAt}, source_published_at),
        codex_idempotency_key = CASE
          WHEN codex_idempotency_key IS NULL OR btrim(codex_idempotency_key) = ''
            THEN COALESCE(${nextIdempotencyKey}, codex_idempotency_key)
          ELSE codex_idempotency_key
        END,
        model = COALESCE(${model || null}, model),
        updated_at = NOW()
      WHERE id = ${target.id}
      RETURNING id
    `;
    if (!updateRows?.length) {
      return res.status(404).json({ error: 'Codex draft not found' });
    }

    const updatedRows = await sql`
      SELECT id, slug, title, section, status
      FROM article_drafts
      WHERE id = ${target.id}
      LIMIT 1
    `;
    if (!updatedRows?.[0]) {
      return res.status(404).json({ error: 'Codex draft not found' });
    }
    return res.status(200).json({ ok: true, draft: updatedRows[0] });
  } catch (error) {
    console.error('Codex update draft error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    if (isUniqueViolation(error)) {
      return res.status(409).json({ error: getUniqueViolationMessage(error) });
    }
    return res.status(500).json({ error: 'Failed to update codex draft', details: error.message });
  }
};
