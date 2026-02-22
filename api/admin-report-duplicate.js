const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

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
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureDuplicateReportsTable(sql);

    const id = Number(req.body?.id || 0);
    const notes = String(req.body?.notes || '').trim();
    const reason = String(req.body?.reason || 'manual_duplicate').trim() || 'manual_duplicate';

    if (!id) {
      return res.status(400).json({ error: 'Missing draft id' });
    }

    const rows = await sql`
      SELECT id, slug, title, section, source_url as "sourceUrl", source_title as "sourceTitle"
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
        ${reason},
        ${notes || null},
        'admin_ui',
        NOW()
      )
      RETURNING id, draft_id as "draftId", draft_title as "draftTitle", source_url as "sourceUrl", reported_at as "reportedAt"
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
