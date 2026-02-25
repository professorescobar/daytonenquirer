const { requireAdmin } = require('./_admin-auth');
const { cleanText, normalizeSection, truncate } = require('./_draft-utils');
const { resolveProvider, getDefaultModel, callModelJson } = require('./_llm-admin');

function buildResearchPrompt({ title, section, sourceTitle, sourceUrl }) {
  return `
You are a newsroom research analyst.

Headline/topic:
${title}

Section:
${section}

Optional source context:
- sourceTitle: ${sourceTitle || 'n/a'}
- sourceUrl: ${sourceUrl || 'n/a'}

Task:
What more can you tell me about this? Compile research.

Return valid JSON only with this schema:
{
  "summary": "short research summary",
  "keyFacts": ["fact 1", "fact 2"],
  "timeline": ["date/event", "date/event"],
  "mainAngles": ["angle 1", "angle 2"],
  "otherPointsOfInterest": ["point 1", "point 2"],
  "sources": [{"name":"source name","url":"https://...","note":"why relevant"}]
}

Rules:
- Include at least 5 keyFacts.
- Include at least 3 mainAngles.
- Include at least 3 otherPointsOfInterest.
- Be specific and concrete.
- Do not include markdown.
`;
}

function buildWritePrompt({ title, section, research }) {
  const safeResearch = JSON.stringify(research);
  return `
You are a news writer.

Headline:
${title}

Section:
${section}

Research packet:
${safeResearch}

Write a thought-provoking, thoroughly detailed, concise article about this. Avoid fluff, repetition, and verbosity.

Return valid JSON only with this schema:
{
  "description": "50-70 word SEO summary",
  "content": "article body using paragraph breaks with \\n\\n",
  "section": "${section}"
}

Rules:
- 650-950 words for content.
- Must weave in both mainAngles and otherPointsOfInterest from research.
- Must remain factual and avoid unsupported claims.
- No markdown, no code fences.
`;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const title = cleanText(req.body?.title || '');
    const section = normalizeSection(req.body?.section) || 'local';
    const sourceTitle = cleanText(req.body?.sourceTitle || '');
    const sourceUrl = cleanText(req.body?.sourceUrl || '');
    const provider = resolveProvider(req.body?.provider);
    const model = cleanText(req.body?.model || '') || getDefaultModel(provider);

    if (!title) return res.status(400).json({ error: 'Title is required' });

    const researchPrompt = buildResearchPrompt({ title, section, sourceTitle, sourceUrl });
    const research = await callModelJson({
      provider,
      model,
      prompt: researchPrompt,
      maxOutputTokens: 2800
    });

    const otherPoints = Array.isArray(research?.otherPointsOfInterest)
      ? research.otherPointsOfInterest.map((v) => cleanText(v)).filter(Boolean)
      : [];
    if (otherPoints.length < 1) {
      return res.status(422).json({ error: 'Research packet missing otherPointsOfInterest' });
    }

    const writePrompt = buildWritePrompt({ title, section, research });
    const article = await callModelJson({
      provider,
      model,
      prompt: writePrompt,
      maxOutputTokens: 3200
    });

    const description = truncate(cleanText(article?.description || ''), 800);
    const content = cleanText(article?.content || '');
    if (!content) return res.status(422).json({ error: 'Model returned empty article content' });

    return res.status(200).json({
      ok: true,
      provider,
      model,
      research: {
        summary: cleanText(research?.summary || ''),
        keyFacts: Array.isArray(research?.keyFacts) ? research.keyFacts.map((v) => cleanText(v)).filter(Boolean) : [],
        timeline: Array.isArray(research?.timeline) ? research.timeline.map((v) => cleanText(v)).filter(Boolean) : [],
        mainAngles: Array.isArray(research?.mainAngles) ? research.mainAngles.map((v) => cleanText(v)).filter(Boolean) : [],
        otherPointsOfInterest: otherPoints,
        sources: Array.isArray(research?.sources) ? research.sources : []
      },
      article: {
        title,
        section,
        description,
        content
      }
    });
  } catch (error) {
    console.error('Generate article error:', error);
    return res.status(500).json({ error: 'Failed to generate article', details: error.message });
  }
};
