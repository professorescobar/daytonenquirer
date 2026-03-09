const { neon } = require('@neondatabase/serverless');

export default async function handler(req, res) {
  try {
    const sql = neon(process.env.DATABASE_URL);
    
    const sections = ['local', 'national', 'world', 'business', 'sports', 'health', 'entertainment', 'technology'];
    const stories = [];

    for (const section of sections) {
      // Get second most recent article with image from each section
      const articles = await sql`
        SELECT 
          slug,
          title,
          description,
          image,
          section,
          pub_date as "pubDate"
        FROM articles
        WHERE section = ${section} 
          AND COALESCE(status, 'published') = 'published'
          AND image_status = 'with_image'
          AND (
            jsonb_typeof(placement_eligible) = 'array'
            AND placement_eligible ? 'carousel'
          )
          AND image IS NOT NULL 
          AND image != ''
        ORDER BY pub_date DESC
        LIMIT 2
      `;

      if (articles.length >= 2) {
        const story = articles[1]; // Second most recent
        stories.push({
          title: story.title,
          description: story.description || '',
          image: story.image,
          category: section.charAt(0).toUpperCase() + section.slice(1),
          slug: story.slug,
          section: story.section,
          pubDate: story.pubDate
        });
      } else if (articles.length === 1) {
        // Fallback to first if only one exists
        const story = articles[0];
        stories.push({
          title: story.title,
          description: story.description || '',
          image: story.image,
          category: section.charAt(0).toUpperCase() + section.slice(1),
          slug: story.slug,
          section: story.section,
          pubDate: story.pubDate
        });
      }
    }

    const result = { stories };
    res.setHeader('Cache-Control', 'no-store, max-age=0');
    res.json(result);
  } catch (error) {
    console.error('Carousel database error:', error);
    res.status(500).json({ error: 'Failed to fetch carousel stories' });
  }
}
