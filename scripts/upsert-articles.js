require('dotenv').config();
const { neon } = require('@neondatabase/serverless');
const fs = require('fs');

function generateSlug(title) {
  return String(title || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function normalizeArticle(article) {
  const slug = article.slug || generateSlug(article.title);
  return {
    slug,
    title: article.title || '',
    description: article.description || '',
    content: article.content || '',
    section: article.section || 'local',
    image: article.image || '',
    imageCaption: article.imageCaption || '',
    imageCredit: article.imageCredit || '',
    pubDate: article.pubDate || new Date().toISOString(),
    status: article.status || 'published'
  };
}

async function upsertArticles() {
  if (!process.env.DATABASE_URL) {
    throw new Error('Missing DATABASE_URL in environment.');
  }

  const sql = neon(process.env.DATABASE_URL);
  const articlesJson = fs.readFileSync('content/custom-articles.json', 'utf8');
  const rawArticles = JSON.parse(articlesJson);
  const articles = rawArticles.map(normalizeArticle);

  console.log(`Found ${articles.length} articles to upsert...`);

  let insertedOrUpdated = 0;
  let failed = 0;

  for (const article of articles) {
    try {
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
          ${article.pubDate},
          ${article.status}
        )
        ON CONFLICT (slug) DO UPDATE SET
          title = EXCLUDED.title,
          description = EXCLUDED.description,
          content = EXCLUDED.content,
          section = EXCLUDED.section,
          image = EXCLUDED.image,
          image_caption = EXCLUDED.image_caption,
          image_credit = EXCLUDED.image_credit,
          pub_date = EXCLUDED.pub_date,
          status = EXCLUDED.status,
          updated_at = NOW()
      `;

      insertedOrUpdated++;
      console.log(`✅ Upserted: ${article.slug}`);
    } catch (err) {
      failed++;
      console.log(`⚠️  Failed: ${article.slug} - ${err.message}`);
    }
  }

  console.log(`\n✅ Upsert complete! ${insertedOrUpdated} upserted, ${failed} failed.`);
}

upsertArticles().catch((err) => {
  console.error('❌ Upsert failed:', err.message);
  process.exit(1);
});
