require('dotenv').config();
const { neon } = require('@neondatabase/serverless');
const fs = require('fs');

// Function to generate slug from title
function generateSlug(title) {
  return title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
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
      const slug = article.slug || generateSlug(article.title);
      
      await sql`
        INSERT INTO articles (slug, title, description, content, section, image, image_caption, image_credit, pub_date, status)
        VALUES (
          ${slug},
          ${article.title},
          ${article.description || ''},
          ${article.content || ''},
          ${article.section},
          ${article.image || ''},
          ${article.imageCaption || ''},
          ${article.imageCredit || ''},
          ${article.pubDate},
          'published'
        )
        ON CONFLICT (slug) DO NOTHING
      `;
      migrated++;
      console.log(`✅ Migrated: ${article.title}`);
    } catch (err) {
      skipped++;
      console.log(`⚠️  Skipped: ${article.title} - ${err.message}`);
    }
  }
  
  console.log(`\n✅ Migration complete! ${migrated} migrated, ${skipped} skipped.`);
}

migrateArticles().catch(console.error);