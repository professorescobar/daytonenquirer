require('dotenv').config();
const { neon } = require('@neondatabase/serverless');
const fs = require('fs');

// Function to generate slug from title
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

async function migrateArticles() {
  const sql = neon(process.env.DATABASE_URL);
  
  // Read existing articles
  const articlesJson = fs.readFileSync('content/custom-articles.json', 'utf8');
  const articles = JSON.parse(articlesJson);
  
  console.log(`Found ${articles.length} articles to migrate...`);
  
  let migrated = 0;
  let skipped = 0;
  
  for (const article of articles) {
    try {
      // Generate slug if missing
      const slug = ensureNonEmptySlug(
        article.slug,
        buildFallbackSeed(article)
      );
      const imageContract = buildImageContract(article.image);
      
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
          image_status,
          image_status_changed_at,
          render_class,
          placement_eligible,
          pub_date,
          status
        )
        VALUES (
          ${slug},
          ${article.title},
          ${article.description || ''},
          ${article.content || ''},
          ${article.section},
          ${imageContract.image},
          ${article.imageCaption || ''},
          ${article.imageCredit || ''},
          ${imageContract.imageStatus},
          NOW(),
          ${imageContract.renderClass},
          ${JSON.stringify(imageContract.placementEligible)}::jsonb,
          ${article.pubDate},
          'published'
        )
        ON CONFLICT ((lower(trim(slug))))
          WHERE slug IS NOT NULL
            AND trim(slug) <> ''
          DO NOTHING
        RETURNING id
      `;
      if (inserted.length > 0) {
        migrated++;
        console.log(`✅ Migrated: ${article.title}`);
      } else {
        skipped++;
        console.log(`↩️  Existing (skipped): ${article.title}`);
      }
    } catch (err) {
      skipped++;
      console.log(`⚠️  Skipped: ${article.title} - ${err.message}`);
    }
  }
  
  console.log(`\n✅ Migration complete! ${migrated} migrated, ${skipped} skipped.`);
}

migrateArticles().catch(console.error);
