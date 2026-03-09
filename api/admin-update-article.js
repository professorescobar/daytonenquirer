const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { cleanText, normalizeSection, generateSlug } = require('./_draft-utils');

function isUniqueViolation(error) {
  const code = String(error?.code || '').trim();
  const message = String(error?.message || '').toLowerCase();
  return code === '23505' || message.includes('duplicate key value violates unique constraint');
}

function normalizeComparableTitle(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function normalizeImageStatus(value) {
  const status = String(value || '').trim();
  return status === 'with_image' || status === 'text_only' ? status : '';
}

function normalizePlacementEligible(value) {
  if (!Array.isArray(value)) return null;
  const cleaned = value
    .map((v) => String(v || '').trim())
    .filter(Boolean);
  return cleaned.length ? cleaned : null;
}

function buildImagePlacementContract(imageUrl, requestedImageStatus, requestedPlacementEligible, existingImageStatus) {
  const hasImage = String(imageUrl || '').trim().length > 0;
  const explicitStatus = normalizeImageStatus(requestedImageStatus);
  const persistedStatus = normalizeImageStatus(existingImageStatus);
  const imageStatus = explicitStatus || persistedStatus || (hasImage ? 'with_image' : 'text_only');
  const renderClass = imageStatus;

  const defaultEligible = imageStatus === 'with_image'
    ? ['main', 'top', 'carousel', 'grid', 'sidebar', 'extra_headlines']
    : ['sidebar', 'extra_headlines'];
  const parsedEligible = normalizePlacementEligible(requestedPlacementEligible) || defaultEligible;
  const placementEligible = imageStatus === 'text_only'
    ? parsedEligible.filter((slot) => slot === 'sidebar' || slot === 'extra_headlines')
    : parsedEligible;

  return {
    imageStatus,
    renderClass,
    placementEligible: placementEligible.length ? placementEligible : defaultEligible
  };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const {
      id,
      title,
      description,
      content,
      research,
      section,
      beat,
      persona,
      image,
      imageCaption,
      imageCredit,
      imageStatus,
      placementEligible,
      pubDate
    } = req.body || {};

    const articleId = Number(id || 0);
    if (!articleId) {
      return res.status(400).json({ error: 'Missing article id' });
    }

    const existingRows = await sql`
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
        image_status as "imageStatus",
        placement_eligible as "placementEligible",
        pub_date as "pubDate"
      FROM articles
      WHERE id = ${articleId}
      LIMIT 1
    `;

    const existing = existingRows[0];
    if (!existing) {
      return res.status(404).json({ error: 'Article not found' });
    }

    const payload = req.body || {};
    const hasTitle = Object.prototype.hasOwnProperty.call(payload, 'title');
    const hasDescription = Object.prototype.hasOwnProperty.call(payload, 'description');
    const hasContent = Object.prototype.hasOwnProperty.call(payload, 'content');
    const hasResearch = Object.prototype.hasOwnProperty.call(payload, 'research');
    const hasSection = Object.prototype.hasOwnProperty.call(payload, 'section');
    const hasBeat = Object.prototype.hasOwnProperty.call(payload, 'beat');
    const hasPersona = Object.prototype.hasOwnProperty.call(payload, 'persona');
    const hasImage = Object.prototype.hasOwnProperty.call(payload, 'image');
    const hasImageCaption = Object.prototype.hasOwnProperty.call(payload, 'imageCaption');
    const hasImageCredit = Object.prototype.hasOwnProperty.call(payload, 'imageCredit');
    const hasPubDate = Object.prototype.hasOwnProperty.call(payload, 'pubDate');

    const nextTitle = hasTitle ? (cleanText(title) || existing.title) : existing.title;
    const titleChanged = normalizeComparableTitle(nextTitle) !== normalizeComparableTitle(existing.title);
    const baseSlug = titleChanged
      ? (generateSlug(nextTitle) || existing.slug || `article-${articleId}`)
      : (existing.slug || generateSlug(nextTitle) || `article-${articleId}`);
    const slugCandidates = [
      baseSlug,
      `${baseSlug}-${Date.now().toString().slice(-6)}`,
      `${baseSlug}-${Math.random().toString(36).slice(2, 8)}`
    ];

    let nextImage = hasImage ? cleanText(image) : cleanText(existing.image || '');
    const imageContract = buildImagePlacementContract(
      nextImage,
      imageStatus,
      placementEligible,
      existing.imageStatus
    );
    if (imageContract.imageStatus === 'text_only') {
      nextImage = '';
    }
    if (imageContract.imageStatus === 'with_image' && !nextImage) {
      return res.status(400).json({ error: 'with_image requires a non-empty image URL' });
    }

    const nextDescription = hasDescription ? cleanText(description) : cleanText(existing.description || '');
    const nextContent = hasContent ? cleanText(content) : cleanText(existing.content || '');
    const nextResearch = hasResearch ? cleanText(research) : cleanText(existing.research || '');
    const nextSection = hasSection ? normalizeSection(section || 'local') : existing.section;
    const nextBeat = hasBeat ? (cleanText(beat) || null) : (cleanText(existing.beat || '') || null);
    const nextPersona = hasPersona ? (cleanText(persona) || null) : (cleanText(existing.persona || '') || null);
    const nextImageCaption = hasImageCaption ? cleanText(imageCaption) : cleanText(existing.imageCaption || '');
    const nextImageCredit = hasImageCredit ? cleanText(imageCredit) : cleanText(existing.imageCredit || '');
    const nextPubDate = hasPubDate ? (pubDate || null) : existing.pubDate;

    let lastError = null;
    for (const nextSlug of slugCandidates) {
      try {
        const updatedRows = await sql`
          UPDATE articles
          SET
            slug = ${nextSlug},
            title = ${nextTitle},
            description = ${nextDescription},
            content = ${nextContent},
            research = ${nextResearch},
            section = ${nextSection},
            beat = ${nextBeat},
            persona = ${nextPersona},
            image = ${nextImage},
            image_caption = ${nextImageCaption},
            image_credit = ${nextImageCredit},
            image_status = ${imageContract.imageStatus},
            image_status_changed_at = CASE
              WHEN COALESCE(image_status, '') <> ${imageContract.imageStatus} THEN NOW()
              ELSE image_status_changed_at
            END,
            render_class = ${imageContract.renderClass},
            placement_eligible = ${JSON.stringify(imageContract.placementEligible)}::jsonb,
            pub_date = COALESCE(${nextPubDate}, pub_date),
            updated_at = NOW()
          WHERE id = ${articleId}
          RETURNING id
        `;
        if (!updatedRows?.length) {
          return res.status(404).json({ error: 'Article not found' });
        }
        return res.status(200).json({ ok: true, id: articleId, slug: nextSlug });
      } catch (error) {
        if (!isUniqueViolation(error)) throw error;
        lastError = error;
      }
    }

    if (lastError) {
      return res.status(409).json({ error: 'Article slug already exists' });
    }
    return res.status(500).json({ error: 'Failed to update article' });
  } catch (error) {
    console.error('Admin update article error:', error);
    return res.status(500).json({ error: 'Failed to update article' });
  }
};
