import { sql } from '@neondatabase/serverless';

export default async function handler(req, res) {
  try {
    await sql`
      ALTER TABLE articles
      ADD COLUMN IF NOT EXISTS persona VARCHAR(255);
    `;
    await sql`
      ALTER TABLE article_drafts
      ADD COLUMN IF NOT EXISTS persona VARCHAR(255);
    `;
    return res.status(200).json({ message: 'Success: "persona" column added to "articles" and "article_drafts" tables.' });
  } catch (error) {
    console.error('Setup failed:', error);
    return res.status(500).json({ error: error.message });
  }
}