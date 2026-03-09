const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

function isMissingColumnError(error) {
  return String(error?.code || '') === '42703';
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const section = String(req.query.section || 'all').toLowerCase();
    const imageStatus = String(req.query.image_status || '').toLowerCase();
    const parsedLimit = Number.parseInt(String(req.query.limit || '50'), 10);
    const limit = Math.max(1, Math.min(Number.isFinite(parsedLimit) ? parsedLimit : 50, 5000));
    const sortMode = String(req.query.sort || '').toLowerCase();

    let rows;
    try {
      rows = await sql`
        SELECT
          id,
          slug,
          title,
          description,
          content,
          research,
          section,
          beat,
          persona,
          image,
          image_caption as "imageCaption",
          image_credit as "imageCredit",
          COALESCE(image_status, 'text_only') as "imageStatus",
          image_status_changed_at as "imageStatusChangedAt",
          COALESCE(render_class, COALESCE(image_status, 'text_only')) as "renderClass",
          placement_eligible as "placementEligible",
          pub_date as "pubDate",
          COALESCE(status, 'published') as status,
          created_at as "createdAt",
          updated_at as "updatedAt"
        FROM articles
        WHERE COALESCE(status, 'published') = 'published'
          AND (${section} = 'all' OR section = ${section})
          AND (
            ${imageStatus} = ''
            OR ${imageStatus} = 'all'
            OR COALESCE(image_status, 'text_only') = ${imageStatus}
          )
        ORDER BY
          CASE WHEN ${sortMode} = 'text_only_follow_up' THEN image_status_changed_at END DESC NULLS LAST,
          pub_date DESC,
          id DESC
        LIMIT ${limit}
      `;
    } catch (error) {
      if (!isMissingColumnError(error)) throw error;

      rows = await sql`
        SELECT
          id,
          slug,
          title,
          description,
          content,
          section,
          image,
          pub_date as "pubDate",
          COALESCE(status, 'published') as status,
          created_at as "createdAt",
          updated_at as "updatedAt"
        FROM articles
        WHERE COALESCE(status, 'published') = 'published'
          AND (${section} = 'all' OR section = ${section})
        ORDER BY
          pub_date DESC,
          id DESC
        LIMIT ${limit}
      `;

      rows = rows
        .map((row) => {
          const hasImage = Boolean(String(row.image || '').trim());
          const normalizedStatus = hasImage ? 'with_image' : 'text_only';
          if (imageStatus && imageStatus !== 'all' && imageStatus !== normalizedStatus) return null;
          return {
            ...row,
            research: null,
            beat: null,
            persona: null,
            imageCaption: null,
            imageCredit: null,
            imageStatus: normalizedStatus,
            imageStatusChangedAt: null,
            renderClass: normalizedStatus,
            placementEligible: hasImage
              ? ['main', 'top', 'carousel', 'grid', 'sidebar', 'extra_headlines']
              : ['sidebar', 'extra_headlines']
          };
        })
        .filter(Boolean);
    }

    let totalRows;
    try {
      totalRows = await sql`
        SELECT COUNT(*)::int AS "totalCount"
        FROM articles
        WHERE COALESCE(status, 'published') = 'published'
          AND (${section} = 'all' OR section = ${section})
          AND (
            ${imageStatus} = ''
            OR ${imageStatus} = 'all'
            OR COALESCE(image_status, 'text_only') = ${imageStatus}
          )
      `;
    } catch (error) {
      if (!isMissingColumnError(error)) throw error;
      totalRows = await sql`
        SELECT COUNT(*)::int AS "totalCount"
        FROM articles
        WHERE COALESCE(status, 'published') = 'published'
          AND (${section} = 'all' OR section = ${section})
          AND (
            ${imageStatus} = ''
            OR ${imageStatus} = 'all'
            OR (${imageStatus} = 'with_image' AND COALESCE(NULLIF(BTRIM(image), ''), '') <> '')
            OR (${imageStatus} = 'text_only' AND COALESCE(NULLIF(BTRIM(image), ''), '') = '')
          )
      `;
    }
    const totalCount = totalRows?.[0]?.totalCount || 0;

    const totalAllRows = await sql`
      SELECT COUNT(*)::int AS "totalAllCount"
      FROM articles
      WHERE COALESCE(status, 'published') = 'published'
    `;
    const totalAllCount = totalAllRows?.[0]?.totalAllCount || 0;

    return res.status(200).json({
      articles: rows,
      count: rows.length,
      totalCount,
      totalAllCount
    });
  } catch (error) {
    console.error('Admin articles list error:', error);
    return res.status(500).json({ error: 'Failed to load articles' });
  }
};
