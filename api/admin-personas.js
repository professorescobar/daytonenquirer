import { sql } from '@neondatabase/serverless';

export default async function handler(req, res) {
  // Basic admin token check
  const token = req.headers['x-admin-token'];
  if (process.env.ADMIN_TOKEN && token !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  if (req.method === 'GET') {
    try {
      const { rows } = await sql`SELECT id, avatar_url as "avatarUrl", disclosure FROM personas`;
      return res.status(200).json({ personas: rows });
    } catch (error) {
      console.error('Error fetching personas:', error);
      // If table doesn't exist yet, return empty list instead of crashing
      if (error.message.includes('does not exist')) {
         return res.status(200).json({ personas: [] });
      }
      return res.status(500).json({ error: 'Failed to fetch personas' });
    }
  }

  if (req.method === 'PUT') {
    try {
      const { id, avatarUrl, disclosure } = req.body;
      if (!id) {
        return res.status(400).json({ error: 'Persona ID is required' });
      }

      const { rows } = await sql`
        INSERT INTO personas (id, avatar_url, disclosure)
        VALUES (${id}, ${avatarUrl}, ${disclosure})
        ON CONFLICT (id) DO UPDATE
        SET avatar_url = EXCLUDED.avatar_url,
            disclosure = EXCLUDED.disclosure,
            updated_at = now()
        RETURNING id, avatar_url as "avatarUrl", disclosure;
      `;

      return res.status(200).json({ persona: rows[0] });
    } catch (error) {
      console.error('Error saving persona:', error);
      return res.status(500).json({ error: 'Failed to save persona' });
    }
  }

  res.setHeader('Allow', ['GET', 'PUT']);
  return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
}
