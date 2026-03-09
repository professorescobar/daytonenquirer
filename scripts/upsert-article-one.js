require('dotenv').config();
const { neon } = require('@neondatabase/serverless');
const fs = require('fs');

function generateSlug(title) {
  return String(title || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function stableSlugSuffix(seed) {
  const text = String(seed || 'article').trim().toLowerCase();
  let hash = 0;
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  }
  return Math.abs(hash || 1).toString(36);
}

function buildFallbackSeed(article) {
  const seed = [
    article?.sourceUrl,
    article?.source_url,
    article?.url,
    article?.externalId,
    article?.external_id,
    article?.id,
    article?.title,
    article?.section,
    article?.pubDate,
    article?.pub_date
  ].find((value) => String(value || '').trim());
  return String(seed || 'article').trim();
}

function ensureNonEmptySlug(rawSlug, fallbackSeed = '') {
  const normalized = generateSlug(rawSlug);
  if (normalized) return normalized;
  const fallback = generateSlug(fallbackSeed);
  if (fallback) return fallback;
  return `article-${stableSlugSuffix(fallbackSeed)}`;
}

function buildImageContract(imageValue) {
  const image = String(imageValue || '').trim();
  const hasImage = image.length > 0;
  return {
    image: hasImage ? image : '',
    imageStatus: hasImage ? 'with_image' : 'text_only',
    renderClass: hasImage ? 'with_image' : 'text_only',
    placementEligible: hasImage
      ? ['main', 'top', 'carousel', 'grid', 'sidebar', 'extra_headlines']
      : ['sidebar', 'extra_headlines']
  };
}

function normalizeArticle(article) {
  const slug = ensureNonEmptySlug(
    article.slug,
    buildFallbackSeed(article)
  );
  const imageContract = buildImageContract(article.image);
  return {
    slug,
    title: article.title || '',
    description: article.description || '',
    content: article.content || '',
    section: article.section || 'local',
    image: imageContract.image,
    imageCaption: article.imageCaption || '',
    imageCredit: article.imageCredit || '',
    imageStatus: imageContract.imageStatus,
    renderClass: imageContract.renderClass,
    placementEligible: imageContract.placementEligible,
    pubDate: article.pubDate || new Date().toISOString(),
    status: article.status || 'published'
  };
}

function getSlugArg() {
  const arg = process.argv.find((item) => item.startsWith('--slug='));
  if (!arg) return null;
  return arg.slice('--slug='.length).trim();
}

async function upsertOneArticle() {
  if (!process.env.DATABASE_URL) {
    throw new Error('Missing DATABASE_URL in environment.');
  }

  const targetSlug = getSlugArg();
  if (!targetSlug) {
    throw new Error('Missing slug. Use: npm run db:upsert:one -- --slug=your-article-slug');
  }

  const sql = neon(process.env.DATABASE_URL);
  const articlesJson = fs.readFileSync('content/custom-articles.json', 'utf8');
  const rawArticles = JSON.parse(articlesJson);
  const article = rawArticles.map(normalizeArticle).find((a) => a.slug === targetSlug);

  if (!article) {
    throw new Error(`No article found in content/custom-articles.json with slug: ${targetSlug}`);
  }

  await sql`
    INSERT INTO articles (
      slug,
      title,
      description,
      content,
      section,
      image,
      image_caption,
      image_credit,
      image_status,
      image_status_changed_at,
      render_class,
      placement_eligible,
      pub_date,
      status
    )
    VALUES (
      ${article.slug},
      ${article.title},
      ${article.description},
      ${article.content},
      ${article.section},
      ${article.image},
      ${article.imageCaption},
      ${article.imageCredit},
      ${article.imageStatus},
      NOW(),
      ${article.renderClass},
      ${JSON.stringify(article.placementEligible)}::jsonb,
      ${article.pubDate},
      ${article.status}
    )
    ON CONFLICT ((lower(trim(slug))))
      WHERE slug IS NOT NULL
        AND trim(slug) <> ''
      DO UPDATE SET
      title = EXCLUDED.title,
      description = EXCLUDED.description,
      content = EXCLUDED.content,
      section = EXCLUDED.section,
      image = EXCLUDED.image,
      image_caption = EXCLUDED.image_caption,
      image_credit = EXCLUDED.image_credit,
      image_status = EXCLUDED.image_status,
      image_status_changed_at = CASE
        WHEN COALESCE(articles.image_status, '') <> EXCLUDED.image_status THEN NOW()
        ELSE articles.image_status_changed_at
      END,
      render_class = EXCLUDED.render_class,
      placement_eligible = EXCLUDED.placement_eligible,
      pub_date = EXCLUDED.pub_date,
      status = EXCLUDED.status,
      updated_at = NOW()
  `;

  console.log(`✅ Upserted single article: ${article.slug}`);
}

upsertOneArticle().catch((err) => {
  console.error('❌ Single upsert failed:', err.message);
  process.exit(1);
});
