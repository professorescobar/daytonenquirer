const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { cleanText, normalizeSection, truncate } = require('./_draft-utils');

const SECTION_VALUES = new Set([
  'local',
  'national',
  'world',
  'business',
  'sports',
  'health',
  'entertainment',
  'technology'
]);

function toArray(input, maxItems = 30, maxLen = 80) {
  if (Array.isArray(input)) {
    return input
      .map((item) => truncate(cleanText(item), maxLen))
      .filter(Boolean)
      .slice(0, maxItems);
  }
  const text = cleanText(input || '');
  if (!text) return [];
  return text
    .split(',')
    .map((item) => truncate(cleanText(item), maxLen))
    .filter(Boolean)
    .slice(0, maxItems);
}

function toNullableText(input, maxLen) {
  const value = truncate(cleanText(input || ''), maxLen);
  return value || null;
}

function parseBool(input, fallback = null) {
  if (typeof input === 'boolean') return input;
  const value = String(input || '').trim().toLowerCase();
  if (!value) return fallback;
  if (['1', 'true', 'yes'].includes(value)) return true;
  if (['0', 'false', 'no'].includes(value)) return false;
  return fallback;
}

async function ensureMediaLibraryTable(sql) {
  const tableRows = await sql`SELECT to_regclass('public.media_library') AS name`;
  if (!tableRows[0]?.name) {
    const error = new Error('Schema not ready: missing media_library. Apply migration 20260309_26.');
    error.statusCode = 503;
    throw error;
  }
}

async function listImages(req, res, sql) {
  const section = cleanText(req.query.section || '');
  const persona = cleanText(req.query.persona || '');
  const beat = cleanText(req.query.beat || '');
  const approved = parseBool(req.query.approved, null);
  const q = cleanText(req.query.q || '').toLowerCase();
  const limit = Math.max(1, Math.min(parseInt(req.query.limit || '80', 10) || 80, 200));

  const rows = await sql`
    SELECT
      id,
      section,
      beat,
      persona,
      title,
      description,
      tags,
      entities,
      tone,
      image_url as "imageUrl",
      image_public_id as "imagePublicId",
      credit,
      license_type as "licenseType",
      license_source_url as "licenseSourceUrl",
      approved,
      created_at as "createdAt",
      updated_at as "updatedAt"
    FROM media_library
    WHERE (${section || null} IS NULL OR section = ${section})
      AND (${persona || null} IS NULL OR persona = ${persona})
      AND (${beat || null} IS NULL OR beat = ${beat})
      AND (${approved}::boolean IS NULL OR approved = ${approved})
      AND (
        ${q || null} IS NULL
        OR lower(coalesce(title, '')) LIKE '%' || ${q} || '%'
        OR lower(coalesce(description, '')) LIKE '%' || ${q} || '%'
        OR lower(coalesce(persona, '')) LIKE '%' || ${q} || '%'
        OR lower(coalesce(beat, '')) LIKE '%' || ${q} || '%'
      )
    ORDER BY created_at DESC
    LIMIT ${limit}
  `;

  return res.status(200).json({ ok: true, images: rows, count: rows.length });
}

async function createImage(req, res, sql) {
  const imageUrl = toNullableText(req.body?.imageUrl, 3000);
  if (!imageUrl) return res.status(400).json({ error: 'imageUrl is required' });

  const sectionInput = cleanText(req.body?.section || 'entertainment');
  const section = SECTION_VALUES.has(sectionInput) ? sectionInput : normalizeSection(sectionInput);
  const beat = toNullableText(req.body?.beat, 120);
  const persona = toNullableText(req.body?.persona, 120);
  const title = toNullableText(req.body?.title, 240);
  const description = toNullableText(req.body?.description, 2000);
  const tags = toArray(req.body?.tags, 30, 80);
  const entities = toArray(req.body?.entities, 30, 80);
  const tone = toNullableText(req.body?.tone, 80);
  const imagePublicId = toNullableText(req.body?.imagePublicId, 300);
  const credit = toNullableText(req.body?.credit, 240);
  const licenseType = toNullableText(req.body?.licenseType, 120);
  const licenseSourceUrl = toNullableText(req.body?.licenseSourceUrl, 2000);
  const approved = parseBool(req.body?.approved, false);

  const inserted = await sql`
    INSERT INTO media_library (
      section,
      beat,
      persona,
      title,
      description,
      tags,
      entities,
      tone,
      image_url,
      image_public_id,
      credit,
      license_type,
      license_source_url,
      approved
    )
    VALUES (
      ${section},
      ${beat},
      ${persona},
      ${title},
      ${description},
      ${JSON.stringify(tags)}::jsonb,
      ${JSON.stringify(entities)}::jsonb,
      ${tone},
      ${imageUrl},
      ${imagePublicId},
      ${credit},
      ${licenseType},
      ${licenseSourceUrl},
      ${approved}
    )
    RETURNING
      id,
      section,
      beat,
      persona,
      title,
      description,
      tags,
      entities,
      tone,
      image_url as "imageUrl",
      image_public_id as "imagePublicId",
      credit,
      license_type as "licenseType",
      license_source_url as "licenseSourceUrl",
      approved,
      created_at as "createdAt",
      updated_at as "updatedAt"
  `;

  return res.status(200).json({ ok: true, image: inserted[0] });
}

async function updateImage(req, res, sql) {
  const id = Number(req.body?.id || 0);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).json({ error: 'Valid id is required' });
  }

  const currentRows = await sql`
    SELECT id
    FROM media_library
    WHERE id = ${id}
    LIMIT 1
  `;
  if (!currentRows[0]) return res.status(404).json({ error: 'Image not found' });

  const sectionInput = cleanText(req.body?.section || '');
  const section = sectionInput ? (SECTION_VALUES.has(sectionInput) ? sectionInput : normalizeSection(sectionInput)) : null;

  await sql`
    UPDATE media_library
    SET
      section = COALESCE(${section}, section),
      beat = COALESCE(${toNullableText(req.body?.beat, 120)}, beat),
      persona = COALESCE(${toNullableText(req.body?.persona, 120)}, persona),
      title = COALESCE(${toNullableText(req.body?.title, 240)}, title),
      description = COALESCE(${toNullableText(req.body?.description, 2000)}, description),
      tags = COALESCE(${req.body?.tags === undefined ? null : JSON.stringify(toArray(req.body?.tags, 30, 80))}::jsonb, tags),
      entities = COALESCE(${req.body?.entities === undefined ? null : JSON.stringify(toArray(req.body?.entities, 30, 80))}::jsonb, entities),
      tone = COALESCE(${toNullableText(req.body?.tone, 80)}, tone),
      image_url = COALESCE(${toNullableText(req.body?.imageUrl, 3000)}, image_url),
      image_public_id = COALESCE(${toNullableText(req.body?.imagePublicId, 300)}, image_public_id),
      credit = COALESCE(${toNullableText(req.body?.credit, 240)}, credit),
      license_type = COALESCE(${toNullableText(req.body?.licenseType, 120)}, license_type),
      license_source_url = COALESCE(${toNullableText(req.body?.licenseSourceUrl, 2000)}, license_source_url),
      approved = COALESCE(${parseBool(req.body?.approved, null)}, approved),
      updated_at = NOW()
    WHERE id = ${id}
  `;

  const updatedRows = await sql`
    SELECT
      id,
      section,
      beat,
      persona,
      title,
      description,
      tags,
      entities,
      tone,
      image_url as "imageUrl",
      image_public_id as "imagePublicId",
      credit,
      license_type as "licenseType",
      license_source_url as "licenseSourceUrl",
      approved,
      created_at as "createdAt",
      updated_at as "updatedAt"
    FROM media_library
    WHERE id = ${id}
    LIMIT 1
  `;

  return res.status(200).json({ ok: true, image: updatedRows[0] });
}

async function deleteImage(req, res, sql) {
  const id = Number(req.query.id || req.body?.id || 0);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).json({ error: 'Valid id is required' });
  }

  const deleted = await sql`
    DELETE FROM media_library
    WHERE id = ${id}
    RETURNING id
  `;
  if (!deleted[0]) return res.status(404).json({ error: 'Image not found' });

  return res.status(200).json({ ok: true, id });
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureMediaLibraryTable(sql);

    if (req.method === 'GET') return listImages(req, res, sql);
    if (req.method === 'POST') return createImage(req, res, sql);
    if (req.method === 'PUT') return updateImage(req, res, sql);
    if (req.method === 'DELETE') return deleteImage(req, res, sql);
    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    console.error('Admin images error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Failed to manage media library', details: error.message });
  }
};
