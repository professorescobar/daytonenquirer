import { sql } from '@neondatabase/serverless';

export default async function handler(req, res) {
  try {
    await sql`
      CREATE TABLE IF NOT EXISTS personas (
        id VARCHAR(255) PRIMARY KEY,
        avatar_url TEXT,
        disclosure TEXT,
        updated_at TIMESTAMPTZ DEFAULT now()
      );
    `;
    return res.status(200).json({ message: 'Success: "personas" table created.' });
  } catch (error) {
    console.error('Setup failed:', error);
    return res.status(500).json({ error: error.message });
  }
}
