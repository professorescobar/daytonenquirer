const { neon } = require('@neondatabase/serverless');

export default async function handler(req, res) {
  const sql = neon(process.env.DATABASE_URL);
  
  try {
    // Get all published articles for technology section, sorted by date
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
      WHERE section = 'technology' AND status = 'published'
      ORDER BY pub_date DESC
    `;
    
    res.json({ articles });
  } catch (error) {
    console.error('Database error:', error);
    res.status(500).json({ error: 'Failed to fetch articles' });
  }
}