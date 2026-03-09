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

function normalizeComparableTitle(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
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

function buildSlugCandidates(baseSlug) {
  const base = String(baseSlug || '').trim() || `codex-draft-${Date.now()}`;
  return [
    base,
    `${base}-${Date.now().toString().slice(-6)}`,
    `${base}-${Math.random().toString(36).slice(2, 8)}`
  ];
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
    const rawIdempotencyKey = cleanText(req.body?.idempotencyKey || '', 240);
    const idempotencyKey = sanitizeKey(rawIdempotencyKey);
    const updateOnDuplicate = req.body?.updateOnDuplicate !== false;
    const sourcePublishedAt = normalizeDate(req.body?.sourcePublishedAt || '');
    if (rawIdempotencyKey && !idempotencyKey) {
      return res.status(400).json({ error: 'idempotencyKey is invalid after normalization' });
    }

    if (!title) return res.status(400).json({ error: 'Title is required' });
    if (!content) return res.status(400).json({ error: 'Content is required' });

    async function updateDraftById(targetId, options = {}) {
      const currentRows = await sql`
        SELECT id, slug, title, section, status, codex_idempotency_key as "codexIdempotencyKey"
        FROM article_drafts
        WHERE id = ${targetId}
          AND created_via = 'codex_automation'
        LIMIT 1
      `;
      const current = currentRows[0];
      if (!current) {
        return null;
      }
      const existingIdempotencyKey = cleanText(current.codexIdempotencyKey || '').toLowerCase();
      if (options.idempotencyKey && existingIdempotencyKey && existingIdempotencyKey !== options.idempotencyKey) {
        const mismatchError = new Error('Codex idempotency key mismatch for this draft');
        mismatchError.statusCode = 409;
        throw mismatchError;
      }

      const titleChanged = normalizeComparableTitle(title) !== normalizeComparableTitle(current.title);
      const nextBaseSlug = titleChanged
        ? (generateSlug(title) || current.slug || `codex-draft-${current.id}`)
        : (current.slug || generateSlug(current.title) || `codex-draft-${current.id}`);
      const slugCandidates = buildSlugCandidates(nextBaseSlug);

      let lastError = null;
      for (const slugCandidate of slugCandidates) {
        try {
          await sql`
            UPDATE article_drafts
            SET
              slug = ${slugCandidate},
              title = ${title},
              description = ${description},
              content = ${content},
              section = ${section},
              image = ${image || null},
              image_caption = ${imageCaption || null},
              image_credit = ${imageCredit || null},
              source_url = COALESCE(${options.nextSourceUrl ?? null}, source_url),
              source_title = COALESCE(${sourceTitle || null}, source_title),
              source_published_at = COALESCE(${sourcePublishedAt}, source_published_at),
              codex_idempotency_key = CASE
                WHEN codex_idempotency_key IS NULL OR btrim(codex_idempotency_key) = ''
                  THEN COALESCE(${options.idempotencyKey ?? null}, codex_idempotency_key)
                ELSE codex_idempotency_key
              END,
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
        } catch (error) {
          if (!isUniqueViolation(error)) throw error;
          lastError = error;
        }
      }
      if (lastError) {
        const conflictError = new Error(getUniqueViolationMessage(lastError));
        conflictError.statusCode = 409;
        throw conflictError;
      }
      return null;
    }

    async function upsertByIdempotencyKey(slugCandidate) {
      const rows = await sql`
        WITH advisory_lock AS (
          SELECT pg_advisory_xact_lock(hashtext(${`codex_automation:${idempotencyKey}`}))
        ),
        existing AS (
          SELECT id
          FROM advisory_lock l, article_drafts
          WHERE created_via = 'codex_automation'
            AND lower(trim(codex_idempotency_key)) = ${idempotencyKey}
          ORDER BY created_at DESC
          LIMIT 1
        ),
        updated AS (
          UPDATE article_drafts d
          SET
            title = ${title},
            description = ${description},
            content = ${content},
            section = ${section},
            image = ${image || null},
            image_caption = ${imageCaption || null},
            image_credit = ${imageCredit || null},
            source_url = COALESCE(${sourceUrlInput || null}, d.source_url),
            source_title = COALESCE(${sourceTitle || null}, d.source_title),
            source_published_at = COALESCE(${sourcePublishedAt}, d.source_published_at),
            codex_idempotency_key = ${idempotencyKey},
            model = ${model},
            status = 'pending_review',
            updated_at = NOW()
          FROM existing e
          WHERE ${updateOnDuplicate}::boolean = true
            AND d.id = e.id
          RETURNING d.id, d.slug, d.title, d.section, d.status, 'updated'::text AS _action
        ),
        inserted AS (
          INSERT INTO article_drafts (
            slug,
            title,
            description,
            content,
            section,
            image,
            image_caption,
            image_credit,
            codex_idempotency_key,
            source_url,
            source_title,
            source_published_at,
            pub_date,
            model,
            created_via,
            status
          )
          SELECT
            ${slugCandidate},
            ${title},
            ${description},
            ${content},
            ${section},
            ${image || null},
            ${imageCaption || null},
            ${imageCredit || null},
            ${idempotencyKey},
            ${sourceUrlInput || null},
            ${sourceTitle || null},
            ${sourcePublishedAt},
            ${new Date().toISOString()},
            ${model},
            'codex_automation',
            'pending_review'
          WHERE NOT EXISTS (SELECT 1 FROM existing)
          RETURNING id, slug, title, section, status, 'inserted'::text AS _action
        ),
        existing_row AS (
          SELECT d.id, d.slug, d.title, d.section, d.status, 'existing'::text AS _action
          FROM article_drafts d
          JOIN existing e ON e.id = d.id
          WHERE NOT EXISTS (SELECT 1 FROM updated)
            AND NOT EXISTS (SELECT 1 FROM inserted)
        ),
        resolved AS (
          SELECT * FROM updated
          UNION ALL
          SELECT * FROM inserted
          UNION ALL
          SELECT * FROM existing_row
        )
        SELECT * FROM resolved LIMIT 1
      `;
      return rows[0] || null;
    }

    if (Number.isInteger(draftId) && draftId > 0) {
      const updatedDraft = await updateDraftById(draftId, {
        nextSourceUrl: sourceUrlInput || null,
        idempotencyKey: idempotencyKey || null
      });
      if (!updatedDraft) {
        return res.status(404).json({ error: 'Codex draft not found' });
      }
      return res.status(200).json({ ok: true, updated: true, deduped: false, draft: updatedDraft });
    }

    let insertedDraft = null;
    let lastError = null;

    if (idempotencyKey) {
      const slugCandidates = buildSlugCandidates(generateSlug(title));
      for (const slugCandidate of slugCandidates) {
        try {
          const resolvedDraft = await upsertByIdempotencyKey(slugCandidate);
          if (resolvedDraft) {
            return res.status(200).json({
              ok: true,
              deduped: resolvedDraft._action !== 'inserted',
              updated: resolvedDraft._action === 'updated',
              draft: {
                id: resolvedDraft.id,
                slug: resolvedDraft.slug,
                title: resolvedDraft.title,
                section: resolvedDraft.section,
                status: resolvedDraft.status
              }
            });
          }
        } catch (error) {
          if (!isUniqueViolation(error)) throw error;
          lastError = error;
        }
      }
    } else {
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

      const slugCandidates = buildSlugCandidates(generateSlug(title));
      for (const slugCandidate of slugCandidates) {
        try {
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
              codex_idempotency_key,
              source_url,
              source_title,
              source_published_at,
              pub_date,
              model,
              created_via,
              status
            )
            VALUES (
              ${slugCandidate},
              ${title},
              ${description},
              ${content},
              ${section},
              ${image || null},
              ${imageCaption || null},
              ${imageCredit || null},
              ${idempotencyKey ? idempotencyKey : null},
              ${sourceUrlInput || null},
              ${sourceTitle || null},
              ${sourcePublishedAt},
              ${new Date().toISOString()},
              ${model},
              'codex_automation',
              'pending_review'
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
    }

    if (!insertedDraft) {
      if (lastError) {
        return res.status(409).json({ error: getUniqueViolationMessage(lastError) });
      }
      return res.status(500).json({ error: 'Failed to create codex draft' });
    }

    return res.status(200).json({ ok: true, deduped: false, draft: insertedDraft });
  } catch (error) {
    console.error('Codex create draft error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    if (Number(error?.statusCode || 0) === 409) {
      return res.status(409).json({ error: error.message || getUniqueViolationMessage(error) });
    }
    return res.status(500).json({ error: 'Failed to create codex draft', details: error.message });
  }
};
