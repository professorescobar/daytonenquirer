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

  console.log(`✅ Upserted single article: ${article.slug}`);
}

upsertOneArticle().catch((err) => {
  console.error('❌ Single upsert failed:', err.message);
  process.exit(1);
});
