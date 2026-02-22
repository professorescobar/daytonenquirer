const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { generateSlug } = require('./_draft-utils');

const ET_TIME_ZONE = 'America/New_York';

function getEtPartsFromDate(date) {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone: ET_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  const parts = dtf.formatToParts(date);
  const out = {};
  for (const part of parts) {
    if (part.type !== 'literal') out[part.type] = part.value;
  }
  return out;
}

function etLocalToUtcIso(localValue) {
  if (!localValue) return null;
  const match = String(localValue).match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/
  );
  if (!match) return null;

  const [, y, mo, d, h, mi] = match;
  const year = Number(y);
  const month = Number(mo);
  const day = Number(d);
  const hour = Number(h);
  const minute = Number(mi);

  const guessUtc = Date.UTC(year, month - 1, day, hour + 5, minute, 0);
  const windowStart = guessUtc - 12 * 60 * 60 * 1000;
  const windowEnd = guessUtc + 12 * 60 * 60 * 1000;

  for (let t = windowStart; t <= windowEnd; t += 60 * 1000) {
    const p = getEtPartsFromDate(new Date(t));
    if (
      Number(p.year) === year &&
      Number(p.month) === month &&
      Number(p.day) === day &&
      Number(p.hour) === hour &&
      Number(p.minute) === minute
    ) {
      return new Date(t).toISOString();
    }
  }

  return null;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const { id, publishAtEt } = req.body || {};

    if (!id) {
      return res.status(400).json({ error: 'Missing draft id' });
    }

    let publishAtIso = new Date().toISOString();
    const publishAtEtTrimmed = String(publishAtEt || '').trim();
    if (publishAtEtTrimmed) {
      const converted = etLocalToUtcIso(publishAtEtTrimmed);
      if (!converted) {
        return res.status(400).json({ error: 'Invalid ET publish date format' });
      }
      publishAtIso = converted;
    }

    const rows = await sql`
      SELECT
        id,
        slug,
        title,
        description,
        content,
        section,
        image,
        image_caption as "imageCaption",
        image_credit as "imageCredit",
        status
      FROM article_drafts
      WHERE id = ${id}
      LIMIT 1
    `;
    const draft = rows[0];

    if (!draft) return res.status(404).json({ error: 'Draft not found' });
    if (draft.status === 'published') {
      return res.status(400).json({ error: 'Draft already published' });
    }

    let slug = draft.slug || generateSlug(draft.title);
    const exists = await sql`SELECT id FROM articles WHERE slug = ${slug} LIMIT 1`;
    if (exists.length > 0) {
      slug = `${slug}-${Date.now().toString().slice(-6)}`;
    }

    const inserted = await sql`
      INSERT INTO articles (
        slug,
        title,
        description,
        content,
        section,
        image,
        image_caption,
        image_credit,
        pub_date,
        status
      )
      VALUES (
        ${slug},
        ${draft.title},
        ${draft.description || ''},
        ${draft.content || ''},
        ${draft.section},
        ${draft.image || ''},
        ${draft.imageCaption || ''},
        ${draft.imageCredit || ''},
        ${publishAtIso},
        'published'
      )
      RETURNING id
    `;

    const articleId = inserted[0].id;

    await sql`
      UPDATE article_drafts
      SET
        slug = ${slug},
        status = 'published',
        published_article_id = ${articleId},
        updated_at = NOW()
      WHERE id = ${id}
    `;

    return res.status(200).json({ ok: true, articleId, slug });
  } catch (error) {
    console.error('Publish draft error:', error);
    return res.status(500).json({ error: 'Failed to publish draft' });
  }
};
