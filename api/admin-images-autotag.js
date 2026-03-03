const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');
const { cleanText, truncate, normalizeSection } = require('./_draft-utils');

function stripCodeFences(text) {
  const value = String(text || '').trim();
  if (!value.startsWith('```')) return value;
  return value
    .replace(/^```[a-zA-Z0-9_-]*\s*/, '')
    .replace(/```$/, '')
    .trim();
}

function extractJsonCandidate(text) {
  const source = String(text || '');
  let start = -1;
  let openChar = '';
  for (let i = 0; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === '{' || ch === '[') {
      start = i;
      openChar = ch;
      break;
    }
  }
  if (start < 0) return '';
  const closeChar = openChar === '{' ? '}' : ']';
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === openChar) {
      depth += 1;
      continue;
    }
    if (ch === closeChar) {
      depth -= 1;
      if (depth === 0) return source.slice(start, i + 1).trim();
    }
  }
  return '';
}

function safeJsonParse(text) {
  const cleaned = stripCodeFences(text);
  try {
    return JSON.parse(cleaned);
  } catch (_) {
    const candidate = extractJsonCandidate(cleaned);
    if (!candidate) return null;
    try {
      return JSON.parse(candidate);
    } catch (_) {
      return null;
    }
  }
}

function asList(value, maxItems = 20, maxLen = 80) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => truncate(cleanText(item || ''), maxLen))
    .filter(Boolean)
    .slice(0, maxItems);
}

async function ensureMediaLibraryTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS media_library (
      id SERIAL PRIMARY KEY,
      section TEXT NOT NULL DEFAULT 'entertainment',
      beat TEXT,
      persona TEXT,
      title TEXT,
      description TEXT,
      tags JSONB NOT NULL DEFAULT '[]'::jsonb,
      entities JSONB NOT NULL DEFAULT '[]'::jsonb,
      tone TEXT,
      image_url TEXT NOT NULL,
      image_public_id TEXT,
      credit TEXT,
      license_type TEXT,
      license_source_url TEXT,
      approved BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `;
}

async function runGeminiVisionTagging({ imageBytes, mimeType, section, beat, persona, existingTitle, existingDescription }) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) throw new Error('Missing GEMINI_API_KEY');
  const model = cleanText(process.env.GEMINI_VISION_MODEL || process.env.GEMINI_MODEL || 'gemini-2.5-pro');
  if (!model) throw new Error('Missing Gemini model name');

  const prompt = `
You are generating metadata for a newsroom image library.

Context:
- section: ${section || 'entertainment'}
- beat: ${beat || 'n/a'}
- persona: ${persona || 'n/a'}
- existingTitle: ${existingTitle || 'n/a'}
- existingDescription: ${existingDescription || 'n/a'}

Task:
Describe what is visible in this image in concise newsroom terms.
Return valid JSON only:
{
  "title": "short title",
  "description": "1-2 sentence description",
  "tags": ["tag1", "tag2"],
  "entities": ["entity1", "entity2"],
  "tone": "neutral|hype|serious|celebratory|dramatic"
}

Rules:
- No markdown.
- Keep title under 100 chars.
- Keep description under 260 chars.
- tags/entities should be specific and useful for matching.
`;

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      contents: [{
        parts: [
          { text: prompt },
          {
            inlineData: {
              mimeType,
              data: imageBytes.toString('base64')
            }
          }
        ]
      }],
      generationConfig: {
        temperature: 0.2,
        maxOutputTokens: 700,
        responseMimeType: 'application/json'
      }
    })
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Gemini API error ${response.status}: ${body.slice(0, 200)}`);
  }

  const data = await response.json();
  const text = data?.candidates?.[0]?.content?.parts?.map((part) => part?.text || '').join('') || '';
  const parsed = safeJsonParse(text);
  if (!parsed) {
    const preview = String(text || '').replace(/\s+/g, ' ').slice(0, 200);
    throw new Error(`Gemini returned invalid JSON${preview ? `: ${preview}` : ''}`);
  }

  return {
    title: truncate(cleanText(parsed.title || ''), 240),
    description: truncate(cleanText(parsed.description || ''), 2000),
    tags: asList(parsed.tags, 20, 80),
    entities: asList(parsed.entities, 20, 80),
    tone: truncate(cleanText(parsed.tone || ''), 80)
  };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const id = Number(req.body?.id || 0);
    const imageUrlInput = truncate(cleanText(req.body?.imageUrl || ''), 3000);
    const hasId = Number.isInteger(id) && id > 0;
    const hasImageUrl = !!imageUrlInput;
    if (!hasId && !hasImageUrl) {
      return res.status(400).json({ error: 'Provide id or imageUrl' });
    }

    if (!Number.isInteger(id) || id <= 0) {
      // preview mode is allowed with imageUrl only
      if (!hasImageUrl) return res.status(400).json({ error: 'Valid image id is required' });
    }
    const overwrite = req.body?.overwrite === true;

    const sql = neon(process.env.DATABASE_URL);
    await ensureMediaLibraryTable(sql);

    let row = null;
    if (hasId) {
      const rows = await sql`
        SELECT
          id,
          section,
          beat,
          persona,
          title,
          description,
          tags,
          entities,
          tone,
          image_url as "imageUrl"
        FROM media_library
        WHERE id = ${id}
        LIMIT 1
      `;
      row = rows[0];
      if (!row) return res.status(404).json({ error: 'Image not found' });
      if (!row.imageUrl) return res.status(400).json({ error: 'Image URL is missing' });
    } else {
      row = {
        id: 0,
        section: normalizeSection(req.body?.section || 'entertainment'),
        beat: cleanText(req.body?.beat || ''),
        persona: cleanText(req.body?.persona || ''),
        title: cleanText(req.body?.title || ''),
        description: cleanText(req.body?.description || ''),
        tags: asList(req.body?.tags || []),
        entities: asList(req.body?.entities || []),
        tone: cleanText(req.body?.tone || ''),
        imageUrl: imageUrlInput
      };
    }

    const imageRes = await fetch(row.imageUrl);
    if (!imageRes.ok) {
      return res.status(422).json({ error: `Failed to fetch image (${imageRes.status})` });
    }

    const mimeType = cleanText(imageRes.headers.get('content-type') || '').split(';')[0] || 'image/jpeg';
    if (!mimeType.startsWith('image/')) {
      return res.status(422).json({ error: `Unsupported content-type: ${mimeType}` });
    }

    const arrayBuffer = await imageRes.arrayBuffer();
    const imageBytes = Buffer.from(arrayBuffer);
    if (!imageBytes.length) {
      return res.status(422).json({ error: 'Fetched image is empty' });
    }

    const tagged = await runGeminiVisionTagging({
      imageBytes,
      mimeType,
      section: normalizeSection(row.section),
      beat: cleanText(row.beat || ''),
      persona: cleanText(row.persona || ''),
      existingTitle: cleanText(row.title || ''),
      existingDescription: cleanText(row.description || '')
    });

    const existingTags = Array.isArray(row.tags) ? row.tags.map((v) => cleanText(v)).filter(Boolean) : [];
    const existingEntities = Array.isArray(row.entities) ? row.entities.map((v) => cleanText(v)).filter(Boolean) : [];

    const title = overwrite || !cleanText(row.title || '') ? tagged.title : cleanText(row.title || '');
    const description = overwrite || !cleanText(row.description || '') ? tagged.description : cleanText(row.description || '');
    const tone = overwrite || !cleanText(row.tone || '') ? tagged.tone : cleanText(row.tone || '');
    const tags = overwrite ? tagged.tags : (existingTags.length ? existingTags : tagged.tags);
    const entities = overwrite ? tagged.entities : (existingEntities.length ? existingEntities : tagged.entities);

    if (hasId) {
      await sql`
        UPDATE media_library
        SET
          title = ${title || null},
          description = ${description || null},
          tone = ${tone || null},
          tags = ${JSON.stringify(tags)}::jsonb,
          entities = ${JSON.stringify(entities)}::jsonb,
          updated_at = NOW()
        WHERE id = ${id}
      `;

      const updated = await sql`
        SELECT
          id,
          section,
          beat,
          persona,
          title,
          description,
          tags,
          entities,
          tone,
          image_url as "imageUrl",
          image_public_id as "imagePublicId",
          credit,
          license_type as "licenseType",
          license_source_url as "licenseSourceUrl",
          approved,
          created_at as "createdAt",
          updated_at as "updatedAt"
        FROM media_library
        WHERE id = ${id}
        LIMIT 1
      `;

      return res.status(200).json({
        ok: true,
        image: updated[0],
        metadata: {
          title: title || '',
          description: description || '',
          tags,
          entities,
          tone: tone || ''
        },
        model: cleanText(process.env.GEMINI_VISION_MODEL || process.env.GEMINI_MODEL || 'gemini-2.5-pro')
      });
    }

    return res.status(200).json({
      ok: true,
      metadata: {
        title: title || '',
        description: description || '',
        tags,
        entities,
        tone: tone || ''
      },
      model: cleanText(process.env.GEMINI_VISION_MODEL || process.env.GEMINI_MODEL || 'gemini-2.5-pro')
    });
  } catch (error) {
    console.error('Admin images autotag error:', error);
    return res.status(500).json({ error: 'Failed to auto-tag image', details: error.message });
  }
};
