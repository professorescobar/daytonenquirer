const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const ALLOWED_REASONS = new Set([
  'stale_or_not_time_relevant',
  'low_newsworthiness_or_thin',
  'style_mismatch',
  'user_error'
]);
const ALLOWED_DUPLICATE_TYPES = new Set(['internal', 'external']);

async function ensureTables(sql) {
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

  await sql`
    ALTER TABLE editorial_rejections
    ADD COLUMN IF NOT EXISTS input_tokens INTEGER
  `;

  await sql`
    ALTER TABLE editorial_rejections
    ADD COLUMN IF NOT EXISTS output_tokens INTEGER
  `;

  await sql`
    ALTER TABLE editorial_rejections
    ADD COLUMN IF NOT EXISTS total_tokens INTEGER
  `;

  await sql`
    ALTER TABLE editorial_rejections
    ADD COLUMN IF NOT EXISTS model TEXT
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    await ensureTables(sql);

    const id = Number(req.body?.id || 0);
    const action = String(req.body?.action || '').trim(); // duplicate | reject
    const reason = String(req.body?.reason || '').trim();
    const duplicateType = String(req.body?.duplicateType || 'internal').trim().toLowerCase();
    const notes = String(req.body?.notes || '').trim();

    if (!id) return res.status(400).json({ error: 'Missing article id' });
    if (!['duplicate', 'reject'].includes(action)) {
      return res.status(400).json({ error: 'Invalid removal action' });
    }
    if (action === 'reject' && !ALLOWED_REASONS.has(reason)) {
      return res.status(400).json({ error: 'Invalid rejection reason' });
    }
    if (action === 'duplicate' && !ALLOWED_DUPLICATE_TYPES.has(duplicateType)) {
      return res.status(400).json({ error: 'Invalid duplicate type' });
    }

    const articleRows = await sql`
      SELECT id, slug, title, section, pub_date as "pubDate"
      FROM articles
      WHERE id = ${id}
      LIMIT 1
    `;
    const article = articleRows[0];
    if (!article) return res.status(404).json({ error: 'Article not found' });

    const linkedDraftRows = await sql`
      SELECT
        id,
        source_url as "sourceUrl",
        source_title as "sourceTitle",
        model,
        input_tokens as "inputTokens",
        output_tokens as "outputTokens",
        total_tokens as "totalTokens"
      FROM article_drafts
      WHERE published_article_id = ${id}
      ORDER BY updated_at DESC, id DESC
      LIMIT 1
    `;
    const linked = linkedDraftRows[0] || {};

    if (action === 'duplicate') {
      await sql`
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
          ${null},
          ${article.slug || ''},
          ${article.title || ''},
          ${article.section || ''},
          ${linked.sourceUrl || null},
          ${linked.sourceTitle || null},
          ${linked.model || 'unknown'},
          ${duplicateType},
          ${Number(linked.inputTokens || 0)},
          ${Number(linked.outputTokens || 0)},
          ${Number(linked.totalTokens || 0)},
          'published_duplicate',
          ${notes || null},
          'admin_published_editor',
          NOW()
        )
      `;
    } else {
      await sql`
        INSERT INTO editorial_rejections (
          draft_id,
          draft_slug,
          draft_title,
          section,
          source_url,
          source_title,
          model,
          input_tokens,
          output_tokens,
          total_tokens,
          reject_reason,
          notes,
          rejected_by,
          rejected_at
        )
        VALUES (
          ${null},
          ${article.slug || ''},
          ${article.title || ''},
          ${article.section || ''},
          ${linked.sourceUrl || null},
          ${linked.sourceTitle || null},
          ${linked.model || 'unknown'},
          ${Number(linked.inputTokens || 0)},
          ${Number(linked.outputTokens || 0)},
          ${Number(linked.totalTokens || 0)},
          ${reason},
          ${notes || null},
          'admin_published_editor',
          NOW()
        )
      `;
    }

    await sql`
      UPDATE article_drafts
      SET
        published_article_id = NULL,
        updated_at = NOW()
      WHERE published_article_id = ${id}
    `;

    await sql`DELETE FROM articles WHERE id = ${id}`;

    return res.status(200).json({
      ok: true,
      action,
      deletedArticleId: id,
      title: article.title
    });
  } catch (error) {
    console.error('Remove published article error:', error);
    return res.status(500).json({ error: 'Failed to remove published article', details: error.message });
  }
};
