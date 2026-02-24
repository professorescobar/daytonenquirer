const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const ALLOWED_DUPLICATE_TYPES = new Set(['internal', 'external']);

async function ensureDuplicateReportsTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS duplicate_reports (
      id SERIAL PRIMARY KEY,
      draft_id INTEGER,
      draft_slug TEXT,
      draft_title TEXT NOT NULL,
      section TEXT,
      source_url TEXT,
      source_title TEXT,
      model TEXT,
      duplicate_type TEXT DEFAULT 'internal',
      input_tokens INTEGER,
      output_tokens INTEGER,
      total_tokens INTEGER,
      report_reason TEXT DEFAULT 'manual_duplicate',
      notes TEXT,
      reported_by TEXT DEFAULT 'admin_ui',
      reported_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_duplicate_reports_draft_id_unique
    ON duplicate_reports(draft_id)
    WHERE draft_id IS NOT NULL
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_duplicate_reports_reported_at
    ON duplicate_reports(reported_at DESC)
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_duplicate_reports_source_url
    ON duplicate_reports(source_url)
    WHERE source_url IS NOT NULL
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS input_tokens INTEGER
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS output_tokens INTEGER
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS total_tokens INTEGER
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS duplicate_type TEXT DEFAULT 'internal'
  `;

  await sql`
    ALTER TABLE duplicate_reports
    ADD COLUMN IF NOT EXISTS model TEXT
  `;
}

async function ensureModelTrackingReset(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS editorial_rejections (
      id SERIAL PRIMARY KEY,
      draft_id INTEGER,
      draft_slug TEXT,
      draft_title TEXT NOT NULL,
      section TEXT,
      source_url TEXT,
      source_title TEXT,
      model TEXT,
      input_tokens INTEGER,
      output_tokens INTEGER,
      total_tokens INTEGER,
      reject_reason TEXT NOT NULL,
      notes TEXT,
      rejected_by TEXT DEFAULT 'admin_ui',
      rejected_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS admin_runtime_flags (
      key TEXT PRIMARY KEY,
      value TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `;

  const rows = await sql`
    SELECT key
    FROM admin_runtime_flags
    WHERE key = 'model_memory_reset_v1'
    LIMIT 1
  `;
  if (rows.length) return;

  await sql`DELETE FROM duplicate_reports`;
  await sql`DELETE FROM editorial_rejections`;
  await sql`
    INSERT INTO admin_runtime_flags (key, value, created_at, updated_at)
    VALUES ('model_memory_reset_v1', 'done', NOW(), NOW())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value,
        updated_at = NOW()
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureDuplicateReportsTable(sql);
    await ensureModelTrackingReset(sql);

    const id = Number(req.body?.id || 0);
    const notes = String(req.body?.notes || '').trim();
    const reason = String(req.body?.reason || 'manual_duplicate').trim() || 'manual_duplicate';
    const duplicateType = String(req.body?.duplicateType || 'internal').trim().toLowerCase();

    if (!id) {
      return res.status(400).json({ error: 'Missing draft id' });
    }
    if (!ALLOWED_DUPLICATE_TYPES.has(duplicateType)) {
      return res.status(400).json({ error: 'Invalid duplicate type' });
    }

    const rows = await sql`
      SELECT
        id,
        slug,
        title,
        section,
        source_url as "sourceUrl",
        source_title as "sourceTitle",
        model,
        input_tokens as "inputTokens",
        output_tokens as "outputTokens",
        total_tokens as "totalTokens"
      FROM article_drafts
      WHERE id = ${id}
      LIMIT 1
    `;
    const draft = rows[0];
    if (!draft) {
      return res.status(404).json({ error: 'Draft not found' });
    }

    await sql`DELETE FROM duplicate_reports WHERE draft_id = ${draft.id}`;

    const saved = await sql`
      INSERT INTO duplicate_reports (
        draft_id,
        draft_slug,
        draft_title,
        section,
        source_url,
        source_title,
        model,
        duplicate_type,
        input_tokens,
        output_tokens,
        total_tokens,
        report_reason,
        notes,
        reported_by,
        reported_at
      )
      VALUES (
        ${draft.id},
        ${draft.slug || ''},
        ${draft.title || ''},
        ${draft.section || ''},
        ${draft.sourceUrl || null},
        ${draft.sourceTitle || null},
        ${draft.model || 'unknown'},
        ${duplicateType},
        ${Number(draft.inputTokens || 0)},
        ${Number(draft.outputTokens || 0)},
        ${Number(draft.totalTokens || 0)},
        ${reason},
        ${notes || null},
        'admin_ui',
        NOW()
      )
      RETURNING id, draft_id as "draftId", draft_title as "draftTitle", source_url as "sourceUrl", model, duplicate_type as "duplicateType", reported_at as "reportedAt"
    `;

    await sql`
      DELETE FROM article_drafts
      WHERE id = ${id}
    `;

    return res.status(200).json({
      ok: true,
      report: saved[0],
      deletedDraftId: id
    });
  } catch (error) {
    console.error('Report duplicate error:', error);
    return res.status(500).json({ error: 'Failed to report duplicate draft' });
  }
};
