const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('../../_admin-auth');

const ALLOWED_SORT_FIELDS = new Set(['created_at', 'processed_at', 'updated_at', 'is_newsworthy', 'action', 'review_decision']);

function cleanText(value, max = 255) {
  return String(value || '').trim().slice(0, max);
}

function parsePositiveInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value || ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function normalizeSortField(value) {
  const raw = String(value || 'created_at').trim().toLowerCase();
  return ALLOWED_SORT_FIELDS.has(raw) ? raw : 'created_at';
}

function normalizeSortDir(value) {
  return String(value || '').trim().toLowerCase() === 'asc' ? 'asc' : 'desc';
}

async function fetchSignals(sql, {
  personaId,
  action,
  reviewDecision,
  page,
  pageSize,
  sortBy,
  sortDir
}) {
  const offset = (page - 1) * pageSize;
  const personaFilter = personaId || null;
  const actionFilter = action || null;
  const reviewFilter = reviewDecision || null;

  const totalRows = await sql`
    SELECT COUNT(*)::int as count
    FROM topic_signals
    WHERE (${personaFilter}::text IS NULL OR persona_id = ${personaFilter})
      AND (${actionFilter}::text IS NULL OR action = ${actionFilter})
      AND (${reviewFilter}::text IS NULL OR review_decision = ${reviewFilter})
  `;
  const total = Number(totalRows[0]?.count || 0);

  const orderByCreated = sortBy === 'created_at';
  const orderByProcessed = sortBy === 'processed_at';
  const orderByUpdated = sortBy === 'updated_at';
  const orderByNewsworthy = sortBy === 'is_newsworthy';
  const orderByAction = sortBy === 'action';
  const orderByReview = sortBy === 'review_decision';
  const isAsc = sortDir === 'asc';

  const rows = isAsc
    ? await sql`
        SELECT
          id,
          persona_id as "personaId",
          source_type as "sourceType",
          source_name as "sourceName",
          source_url as "sourceUrl",
          external_id as "externalId",
          title,
          snippet,
          section_hint as "sectionHint",
          metadata,
          dedupe_key as "dedupeKey",
          is_newsworthy as "isNewsworthy",
          is_local as "isLocal",
          confidence,
          category,
          event_key as "eventKey",
          relation_to_archive as "relationToArchive",
          action,
          next_step as "nextStep",
          policy_flags as "policyFlags",
          reasoning,
          review_decision as "reviewDecision",
          review_notes as "reviewNotes",
          processed_at as "processedAt",
          created_at as "createdAt",
          updated_at as "updatedAt"
        FROM topic_signals
        WHERE (${personaFilter}::text IS NULL OR persona_id = ${personaFilter})
          AND (${actionFilter}::text IS NULL OR action = ${actionFilter})
          AND (${reviewFilter}::text IS NULL OR review_decision = ${reviewFilter})
        ORDER BY
          CASE WHEN ${orderByNewsworthy} THEN is_newsworthy END ASC NULLS LAST,
          CASE WHEN ${orderByProcessed} THEN processed_at END ASC NULLS LAST,
          CASE WHEN ${orderByUpdated} THEN updated_at END ASC NULLS LAST,
          CASE WHEN ${orderByCreated} THEN created_at END ASC NULLS LAST,
          CASE WHEN ${orderByAction} THEN action END ASC NULLS LAST,
          CASE WHEN ${orderByReview} THEN review_decision END ASC NULLS LAST,
          id ASC
        LIMIT ${pageSize} OFFSET ${offset}
      `
    : await sql`
        SELECT
          id,
          persona_id as "personaId",
          source_type as "sourceType",
          source_name as "sourceName",
          source_url as "sourceUrl",
          external_id as "externalId",
          title,
          snippet,
          section_hint as "sectionHint",
          metadata,
          dedupe_key as "dedupeKey",
          is_newsworthy as "isNewsworthy",
          is_local as "isLocal",
          confidence,
          category,
          event_key as "eventKey",
          relation_to_archive as "relationToArchive",
          action,
          next_step as "nextStep",
          policy_flags as "policyFlags",
          reasoning,
          review_decision as "reviewDecision",
          review_notes as "reviewNotes",
          processed_at as "processedAt",
          created_at as "createdAt",
          updated_at as "updatedAt"
        FROM topic_signals
        WHERE (${personaFilter}::text IS NULL OR persona_id = ${personaFilter})
          AND (${actionFilter}::text IS NULL OR action = ${actionFilter})
          AND (${reviewFilter}::text IS NULL OR review_decision = ${reviewFilter})
        ORDER BY
          CASE WHEN ${orderByNewsworthy} THEN is_newsworthy END DESC NULLS LAST,
          CASE WHEN ${orderByProcessed} THEN processed_at END DESC NULLS LAST,
          CASE WHEN ${orderByUpdated} THEN updated_at END DESC NULLS LAST,
          CASE WHEN ${orderByCreated} THEN created_at END DESC NULLS LAST,
          CASE WHEN ${orderByAction} THEN action END DESC NULLS LAST,
          CASE WHEN ${orderByReview} THEN review_decision END DESC NULLS LAST,
          id DESC
        LIMIT ${pageSize} OFFSET ${offset}
      `;

  return { rows, total, page, pageSize };
}

async function fetchSummary24h(sql) {
  const byPersona = await sql`
    SELECT
      persona_id as "personaId",
      COUNT(*)::int as total,
      COUNT(*) FILTER (WHERE action = 'promote')::int as promoted,
      COUNT(*) FILTER (WHERE action = 'reject')::int as rejected,
      COUNT(*) FILTER (WHERE action = 'watch')::int as watch
    FROM topic_signals
    WHERE created_at >= NOW() - interval '24 hours'
    GROUP BY persona_id
    ORDER BY total DESC, persona_id ASC
  `;

  const totals = await sql`
    SELECT
      COUNT(*)::int as total,
      COUNT(*) FILTER (WHERE action = 'promote')::int as promoted,
      COUNT(*) FILTER (WHERE action = 'reject')::int as rejected,
      COUNT(*) FILTER (WHERE action = 'watch')::int as watch
    FROM topic_signals
    WHERE created_at >= NOW() - interval '24 hours'
  `;

  return {
    byPersona,
    totals: totals[0] || { total: 0, promoted: 0, rejected: 0, watch: 0 }
  };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'GET') {
    res.setHeader('Allow', ['GET']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const personaId = cleanText(req.query?.persona_id || req.query?.personaId || '', 255);
  const action = cleanText(req.query?.action || '', 40).toLowerCase();
  const reviewDecision = cleanText(req.query?.review_decision || req.query?.reviewDecision || '', 40).toLowerCase();
  const page = parsePositiveInt(req.query?.page, 1, 1, 100000);
  const pageSize = parsePositiveInt(req.query?.pageSize || req.query?.limit, 25, 1, 200);
  const sortBy = normalizeSortField(req.query?.sortBy || req.query?.sort_by);
  const sortDir = normalizeSortDir(req.query?.sortDir || req.query?.sort_dir);

  try {
    const sql = neon(process.env.DATABASE_URL);
    const list = await fetchSignals(sql, {
      personaId,
      action,
      reviewDecision,
      page,
      pageSize,
      sortBy,
      sortDir
    });
    const summary24h = await fetchSummary24h(sql);

    return res.status(200).json({
      filters: {
        personaId: personaId || null,
        action: action || null,
        reviewDecision: reviewDecision || null
      },
      pagination: {
        page: list.page,
        pageSize: list.pageSize,
        total: list.total,
        totalPages: Math.max(1, Math.ceil(list.total / list.pageSize))
      },
      sort: { sortBy, sortDir },
      summary24h,
      signals: list.rows
    });
  } catch (error) {
    console.error('Admin signals list error:', error);
    if (String(error?.message || '').toLowerCase().includes('topic_signals')) {
      return res.status(500).json({
        error: 'Topic signal table missing',
        details: 'Run migration 20260304_05_gatekeeper_signals.sql first.'
      });
    }
    return res.status(500).json({ error: 'Failed to load signals' });
  }
};
