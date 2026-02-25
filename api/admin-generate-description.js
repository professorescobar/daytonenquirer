const { requireAdmin } = require('./_admin-auth');
const { cleanText, truncate } = require('./_draft-utils');
const { resolveProvider, getDefaultModel, callModelJson } = require('./_llm-admin');

function wordCount(value) {
  return String(value || '').trim().split(/\s+/).filter(Boolean).length;
}

function buildDescriptionPrompt({ title, content, section }) {
  return `
You are an SEO-focused newsroom editor.

Section: ${section || 'local'}
Headline: ${title}

Article:
${content}

Write one SEO description in plain text.

Return valid JSON only:
{
  "description": "..."
}

Rules:
- 50 to 70 words.
- Thoughtful and informative, no clickbait.
- Avoid fluff, repetition, and verbosity.
- Do not mirror headline wording exactly.
`;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const title = cleanText(req.body?.title || '');
    const content = cleanText(req.body?.content || '');
    const section = cleanText(req.body?.section || 'local');
    const provider = resolveProvider(req.body?.provider);
    const model = cleanText(req.body?.model || '') || getDefaultModel(provider);

    if (!title) return res.status(400).json({ error: 'Title is required' });
    if (!content) return res.status(400).json({ error: 'Content is required' });

    const prompt = buildDescriptionPrompt({ title, content, section });
    const data = await callModelJson({
      provider,
      model,
      prompt,
      maxOutputTokens: 500
    });

    const description = truncate(cleanText(data?.description || ''), 800);
    const words = wordCount(description);
    if (words < 50 || words > 70) {
      return res.status(422).json({
        error: 'Description failed word-count rule',
        details: `Expected 50-70 words, got ${words}`
      });
    }

    return res.status(200).json({
      ok: true,
      provider,
      model,
      description,
      words
    });
  } catch (error) {
    console.error('Generate description error:', error);
    return res.status(500).json({ error: 'Failed to generate description', details: error.message });
  }
};
