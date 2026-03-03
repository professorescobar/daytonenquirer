const { neon } = require('@neondatabase/serverless');

module.exports = async (req, res) => {
  const sql = neon(process.env.DATABASE_URL);
  try {
    await sql`
      ALTER TABLE articles
      ADD COLUMN IF NOT EXISTS persona VARCHAR(255),
      ADD COLUMN IF NOT EXISTS beat VARCHAR(255)
    `;
    await sql`
      ALTER TABLE article_drafts
      ADD COLUMN IF NOT EXISTS persona VARCHAR(255),
      ADD COLUMN IF NOT EXISTS beat VARCHAR(255)
    `;
    return res.status(200).json({ message: 'Success: "persona" and "beat" columns added.' });
  } catch (error) {
    console.error('Setup failed:', error);
    return res.status(500).json({ error: error.message });
  }
};