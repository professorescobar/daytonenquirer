const { neon } = require('@neondatabase/serverless');

async function getArticlesBySection(section) {
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
      COALESCE(image_status, 'text_only') as "imageStatus",
      COALESCE(render_class, COALESCE(image_status, 'text_only')) as "renderClass",
      placement_eligible as "placementEligible",
      image_status_changed_at as "imageStatusChangedAt",
      pub_date as "pubDate"
    FROM articles
    WHERE section = ${section} AND COALESCE(status, 'published') = 'published'
    ORDER BY
      CASE WHEN COALESCE(image_status, 'text_only') = 'with_image' THEN 0 ELSE 1 END ASC,
      pub_date DESC
  `;
  
  return articles;
}

module.exports = { getArticlesBySection };
