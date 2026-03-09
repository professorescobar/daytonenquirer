const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

function cleanText(value, max = 5000) {
  return String(value || '').trim().slice(0, max);
}

function uniqueStrings(values) {
  const seen = new Set();
  const out = [];
  for (const value of values || []) {
    const key = cleanText(value, 5000);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }
  return out;
}

function parseModes(rawModes) {
  const values = Array.isArray(rawModes) ? rawModes : [];
  const normalized = uniqueStrings(values.map((value) => cleanText(value, 80).toLowerCase()));
  const allowed = normalized.filter((value) => value === 'database' || value === 'source' || value === 'generate');
  return allowed.length ? allowed : ['database'];
}

function isMissingRelationError(error, relationName) {
  const code = cleanText(error?.code || '', 40);
  const message = cleanText(error?.message || '', 500).toLowerCase();
  if (code === '42P01') return true;
  return message.includes(`relation "${String(relationName || '').toLowerCase()}" does not exist`);
}

async function queryMediaLibrary(sql, payload) {
  const section = cleanText(payload.section || '', 80);
  const beat = cleanText(payload.beat || '', 120);
  const persona = cleanText(payload.persona || '', 255);
  const title = cleanText(payload.title || '', 240);
  const titleProbe = title ? `%${title.slice(0, 80).toLowerCase()}%` : '';

  try {
    const rows = await sql`
      SELECT
        image_url as "imageUrl",
        credit as "imageCredit",
        COALESCE(title, description, '') as "imageCaption"
      FROM media_library
      WHERE image_url IS NOT NULL
        AND trim(image_url) <> ''
        AND approved = TRUE
        AND (${section} = '' OR section = ${section} OR section IS NULL)
        AND (${beat} = '' OR beat = ${beat} OR beat IS NULL)
        AND (${persona} = '' OR persona = ${persona} OR persona IS NULL)
      ORDER BY
        CASE
          WHEN ${titleProbe} = '' THEN 0
          WHEN lower(COALESCE(title, '')) LIKE ${titleProbe} THEN 0
          WHEN lower(COALESCE(description, '')) LIKE ${titleProbe} THEN 1
          ELSE 2
        END ASC,
        created_at DESC
      LIMIT 1
    `;
    const row = rows[0];
    if (!row?.imageUrl) return null;
    return {
      source: 'database',
      imageUrl: cleanText(row.imageUrl, 5000),
      imageCredit: cleanText(row.imageCredit || '', 300) || null,
      imageCaption: cleanText(row.imageCaption || '', 800) || null
    };
  } catch (error) {
    if (isMissingRelationError(error, 'media_library')) return null;
    throw error;
  }
}

async function queryExaImage(payload) {
  const exaApiKey = cleanText(process.env.EXA_API_KEY || '', 500);
  if (!exaApiKey) return null;

  const query = cleanText(`${payload.title || ''} ${payload.section || ''} news photo`, 400);
  if (!query) return null;

  const response = await fetch('https://api.exa.ai/search', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${exaApiKey}`
    },
    body: JSON.stringify({
      query,
      numResults: 8,
      type: 'keyword',
      useAutoprompt: true,
      contents: { text: { maxCharacters: 400 } }
    })
  });

  if (!response.ok) return null;
  const data = await response.json().catch(() => ({}));
  const rows = Array.isArray(data?.results) ? data.results : [];
  for (const row of rows) {
    const imageUrl = cleanText(row?.image || row?.imageUrl || row?.image_url || '', 5000);
    if (!imageUrl) continue;
    return {
      source: 'source',
      imageUrl,
      imageCredit: cleanText(row?.author || row?.source || 'Exa source', 300) || null,
      imageCaption: cleanText(row?.title || payload.title || '', 800) || null
    };
  }
  return null;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms || 0))));
}

async function pollFluxResult(url, headers) {
  for (let i = 0; i < 6; i += 1) {
    await sleep(1200);
    const poll = await fetch(url, { method: 'GET', headers });
    if (!poll.ok) continue;
    const data = await poll.json().catch(() => ({}));
    const imageUrl = cleanText(data?.image || data?.imageUrl || data?.output?.image || '', 5000);
    if (imageUrl) return imageUrl;
  }
  return '';
}

async function queryFluxGeneratedImage(payload) {
  const baseUrl = cleanText(process.env.FLUX_GENERATE_ENDPOINT || '', 2000);
  if (!baseUrl) return null;

  const fluxApiKey = cleanText(process.env.FLUX_API_KEY || '', 500);
  const prompt = cleanText(
    `Create a professional news-style hero image for: ${payload.title || 'Dayton local news'}. Section: ${payload.section || 'local'}. No text overlay.`,
    1200
  );
  if (!prompt) return null;

  const headers = {
    'Content-Type': 'application/json',
    ...(fluxApiKey
      ? { Authorization: `Bearer ${fluxApiKey}`, 'x-api-key': fluxApiKey, 'x-key': fluxApiKey }
      : {})
  };

  const response = await fetch(baseUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      prompt,
      width: 1365,
      height: 768
    })
  });
  if (!response.ok) return null;

  const data = await response.json().catch(() => ({}));
  let imageUrl = cleanText(data?.image || data?.imageUrl || data?.output?.image || '', 5000);
  if (!imageUrl) {
    const pollUrl = cleanText(data?.pollingUrl || data?.statusUrl || data?.url || '', 5000);
    if (pollUrl) {
      imageUrl = await pollFluxResult(pollUrl, headers);
    }
  }
  if (!imageUrl) return null;

  return {
    source: 'generate',
    imageUrl,
    imageCredit: 'AI generated (Flux)',
    imageCaption: cleanText(payload.title || '', 800) || null
  };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const payload = req.body || {};
    const title = cleanText(payload.title || '', 300);
    const section = cleanText(payload.section || '', 80);
    const beat = cleanText(payload.beat || '', 120);
    const persona = cleanText(payload.persona || '', 255);
    const modes = parseModes(payload.modes);

    if (!title) {
      return res.status(400).json({ error: 'Title is required' });
    }

    const sql = neon(process.env.DATABASE_URL);
    const orderedModes = uniqueStrings(modes);
    let selected = null;

    for (const mode of orderedModes) {
      if (mode === 'database') {
        selected = await queryMediaLibrary(sql, { title, section, beat, persona });
      } else if (mode === 'source') {
        selected = await queryExaImage({ title, section, beat, persona });
      } else if (mode === 'generate') {
        selected = await queryFluxGeneratedImage({ title, section, beat, persona });
      }
      if (selected?.imageUrl) break;
    }

    if (!selected?.imageUrl) {
      return res.status(404).json({ error: 'No matching image found for selected modes' });
    }

    return res.status(200).json({
      ok: true,
      modeUsed: selected.source,
      imageUrl: selected.imageUrl,
      imageCaption: selected.imageCaption || null,
      imageCredit: selected.imageCredit || null
    });
  } catch (error) {
    console.error('Admin source image error:', error);
    return res.status(500).json({
      error: 'Failed to source image',
      details: cleanText(error?.message || '', 500)
    });
  }
};
