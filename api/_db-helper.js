const { neon } = require('@neondatabase/serverless');

async function getArticlesBySection(section, status = 'published') {
  const sql = neon(process.env.DATABASE_URL);
  
  const articles = await sql`
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
      pub_date as "pubDate"
    FROM articles
    WHERE section = ${section} AND status = ${status}
    ORDER BY pub_date DESC
  `;
  
  return articles;
}

module.exports = { getArticlesBySection };