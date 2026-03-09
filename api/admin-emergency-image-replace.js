const { neon } = require('@neondatabase/serverless');
const crypto = require('crypto');
const { requireAdmin } = require('./_admin-auth');

function cleanText(value, max = 5000) {
  return String(value || '').trim().slice(0, max);
}

function normalizeImageStatus(value) {
  const status = cleanText(value || '', 40);
  return status === 'with_image' || status === 'text_only' ? status : '';
}

function buildContract(imageUrl, requestedStatus) {
  const hasImage = cleanText(imageUrl, 5000).length > 0;
  const imageStatus = normalizeImageStatus(requestedStatus) || (hasImage ? 'with_image' : 'text_only');
  return {
    imageStatus,
    renderClass: imageStatus,
    placementEligible: hasImage
      ? ['main', 'top', 'carousel', 'grid', 'sidebar', 'extra_headlines']
      : ['sidebar', 'extra_headlines']
  };
}

function actorTokenFingerprint(tokenValue) {
  const token = cleanText(tokenValue || '', 400);
  if (!token) return 'admin_token_fingerprint:unknown';
  const digest = crypto.createHash('sha256').update(token).digest('hex').slice(0, 16);
  return `admin_token_fingerprint:${digest}`;
}

function getRequestAdminToken(req) {
  const authHeader = String(req.headers.authorization || '');
  const bearer = authHeader.startsWith('Bearer ')
    ? authHeader.slice('Bearer '.length).trim()
    : '';
  return (
    bearer
    || req.headers['x-admin-token']
    || req.headers['x-cron-token']
    || req.query.token
    || ''
  );
}

function isMissingRelationError(error, relationName) {
  const code = cleanText(error?.code || '', 40);
  const message = cleanText(error?.message || '', 500).toLowerCase();
  if (code === '42P01') return true;
  return message.includes(`relation "${String(relationName || '').toLowerCase()}" does not exist`);
}

function buildCloudinaryDeliveryUrl(publicId) {
  const cloudName = cleanText(process.env.CLOUDINARY_CLOUD_NAME || '', 120)
    || cleanText(process.env.NEXT_PUBLIC_CLOUDINARY_CLOUD_NAME || '', 120);
  const safeId = cleanText(publicId || '', 500);
  if (!cloudName || !safeId) return '';
  const encodedId = safeId.split('/').map((part) => encodeURIComponent(part)).join('/');
  return `https://res.cloudinary.com/${encodeURIComponent(cloudName)}/image/upload/f_auto,q_auto/${encodedId}`;
}

async function ensureAuditTable(sql) {
  const rows = await sql`
    SELECT to_regclass('public.topic_engine_image_replace_audit') as name
  `;
  if (!rows[0]?.name) {
    const error = new Error('Schema not ready: missing topic_engine_image_replace_audit. Apply migration 20260308_20.');
    error.statusCode = 503;
    throw error;
  }
}

async function invalidateArticleRelatedCacheAndPaths(res, article) {
  const paths = Array.from(
    new Set(
      [
        '/',
        '/latest',
        '/api/carousel-stories',
        article?.section ? '/section.html' : '',
        article?.slug ? '/article.html' : ''
      ]
        .map((value) => cleanText(value, 300))
        .filter(Boolean)
    )
  );

  const revalidated = [];
  if (typeof res.revalidate === 'function') {
    for (const path of paths) {
      try {
        await res.revalidate(path);
        revalidated.push(path);
      } catch (_) {
        // Keep route resilient when ISR revalidation is unavailable for a path.
      }
    }
  }
  return { paths, revalidated };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const articleId = Number(req.body?.articleId || 0);
    const reasonCode = cleanText(req.body?.reasonCode || '', 120);
    if (!articleId) return res.status(400).json({ error: 'articleId is required' });
    if (!reasonCode) return res.status(400).json({ error: 'reasonCode is required' });

    const articleRows = await sql`
      SELECT
        id,
        slug,
        section,
        beat,
        persona,
        image,
        image_caption as "imageCaption",
        image_credit as "imageCredit"
      FROM articles
      WHERE id = ${articleId}
      LIMIT 1
    `;
    const article = articleRows[0];
    if (!article) return res.status(404).json({ error: 'Article not found' });

    const personaRows = await sql`
      SELECT
        image_fallback_asset_url as "fallbackUrl",
        image_fallback_cloudinary_public_id as "fallbackCloudinaryPublicId"
      FROM personas
      WHERE id = ${cleanText(article.persona || '', 255)}
      LIMIT 1
    `;
    const personaFallback = personaRows[0] || {};

    const previousImage = cleanText(article.image || '', 5000);
    let nextImage = '';
    let nextCredit = '';
    let nextCaption = '';
    let method = 'postgres_candidate';

    try {
      if (previousImage) {
        await sql`
          DELETE FROM media_library
          WHERE image_url = ${previousImage}
        `;
      }
    } catch (error) {
      if (!isMissingRelationError(error, 'media_library')) throw error;
    }

    try {
      const mediaRows = await sql`
        SELECT
          image_url as "imageUrl",
          credit,
          COALESCE(title, description, '') as "caption"
        FROM media_library
        WHERE image_url IS NOT NULL
          AND trim(image_url) <> ''
          AND approved = TRUE
          AND (${previousImage} = '' OR image_url <> ${previousImage})
          AND (${cleanText(article.section || '', 80)} = '' OR section = ${cleanText(article.section || '', 80)})
          AND (
            ${cleanText(article.persona || '', 255)} = ''
            OR persona = ${cleanText(article.persona || '', 255)}
            OR persona IS NULL
          )
          AND (
            ${cleanText(article.beat || '', 120)} = ''
            OR beat = ${cleanText(article.beat || '', 120)}
            OR beat IS NULL
          )
        ORDER BY approved DESC, created_at DESC
        LIMIT 1
      `;
      if (mediaRows[0]?.imageUrl) {
        nextImage = cleanText(mediaRows[0].imageUrl, 5000);
        nextCredit = cleanText(mediaRows[0].credit || '', 300) || nextCredit;
        nextCaption = cleanText(mediaRows[0].caption || '', 800) || nextCaption;
      }
    } catch (error) {
      if (!isMissingRelationError(error, 'media_library')) throw error;
    }

    if (!nextImage) {
      nextImage = cleanText(personaFallback.fallbackUrl || '', 5000)
        || buildCloudinaryDeliveryUrl(personaFallback.fallbackCloudinaryPublicId);
      method = 'persona_fallback';
    }

    if (!nextImage) {
      nextImage = '';
      nextCredit = '';
      nextCaption = '';
      method = 'remove_image_text_only';
    }

    const explicitStatus = method === 'remove_image_text_only' ? 'text_only' : 'with_image';
    const contract = buildContract(nextImage, explicitStatus);
    await ensureAuditTable(sql);
    const auditRows = await sql`
      WITH updated AS (
        UPDATE articles
        SET
          image = ${nextImage},
          image_caption = ${nextCaption},
          image_credit = ${nextCredit},
          image_status = ${contract.imageStatus},
          image_status_changed_at = NOW(),
          render_class = ${contract.renderClass},
          placement_eligible = ${JSON.stringify(contract.placementEligible)}::jsonb,
          updated_at = NOW()
        WHERE id = ${articleId}
        RETURNING id
      )
      INSERT INTO topic_engine_image_replace_audit (
        actor_key,
        article_id,
        reason_code,
        previous_image,
        new_image,
        method,
        created_at
      )
      SELECT
        ${actorTokenFingerprint(getRequestAdminToken(req))},
        updated.id,
        ${reasonCode},
        ${previousImage || null},
        ${nextImage || null},
        ${method},
        NOW()
      FROM updated
      RETURNING article_id
    `;
    if (!auditRows?.length) {
      return res.status(404).json({ error: 'Article not found' });
    }

    const invalidation = await invalidateArticleRelatedCacheAndPaths(res, article);
    res.setHeader('Cache-Control', 'no-store, max-age=0');
    return res.status(200).json({
      ok: true,
      articleId,
      method,
      image: nextImage || null,
      imageStatus: contract.imageStatus,
      renderClass: contract.renderClass,
      placementEligible: contract.placementEligible,
      cacheInvalidation: invalidation
    });
  } catch (error) {
    console.error('Emergency image replace error:', error);
    if (Number(error?.statusCode || 0) === 503) {
      return res.status(503).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Failed emergency image replace', details: error.message });
  }
};
