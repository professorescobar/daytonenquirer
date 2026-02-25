const { requireAdmin } = require('./_admin-auth');
const { cleanText, normalizeSection } = require('./_draft-utils');
const { resolveProvider, getDefaultModel, callModelJson } = require('./_llm-admin');

function buildResearchPrompt({ topic, section }) {
  return `
You are a newsroom research analyst.

Topic seed:
${topic}

Section:
${section}

Task:
Research this topic and compile what else is important right now.

Return valid JSON only with this schema:
{
  "summary": "short summary",
  "keyFacts": ["fact 1", "fact 2"],
  "mainAngles": ["angle 1", "angle 2"],
  "otherPointsOfInterest": ["point 1", "point 2"],
  "recentDevelopments": ["development 1", "development 2"]
}

Rules:
- Include at least 5 keyFacts.
- Include at least 3 mainAngles.
- Include at least 3 otherPointsOfInterest.
- Include at least 3 recentDevelopments.
- No markdown.
`;
}

function buildHeadlinePrompt({ topic, section, research }) {
  return `
You are a senior news editor.

Topic seed:
${topic}

Section:
${section}

Research packet:
${JSON.stringify(research)}

Create strong, newsworthy headlines.

Return valid JSON only with this schema:
{
  "bestHeadline": "headline",
  "alternates": ["headline 2", "headline 3"],
  "rationale": "one short sentence"
}

Rules:
- Headlines should be specific and thought-provoking.
- Avoid clickbait and vague language.
- Keep each headline under 16 words.
- Ensure each headline is materially different.
- No markdown.
`;
}

function normalizeHeadline(value) {
  return cleanText(value).replace(/\s+/g, ' ');
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const topic = cleanText(req.body?.topic || req.body?.title || '');
    const section = normalizeSection(req.body?.section) || 'local';
    const provider = resolveProvider(req.body?.provider);
    const model = cleanText(req.body?.model || '') || getDefaultModel(provider);

    if (!topic) {
      return res.status(400).json({ error: 'Topic or title is required' });
    }

    const research = await callModelJson({
      provider,
      model,
      prompt: buildResearchPrompt({ topic, section }),
      maxOutputTokens: 2200
    });

    const otherPoints = Array.isArray(research?.otherPointsOfInterest)
      ? research.otherPointsOfInterest.map((item) => cleanText(item)).filter(Boolean)
      : [];
    if (otherPoints.length < 1) {
      return res.status(422).json({ error: 'Research packet missing otherPointsOfInterest' });
    }

    const headlineData = await callModelJson({
      provider,
      model,
      prompt: buildHeadlinePrompt({ topic, section, research }),
      maxOutputTokens: 900
    });

    const all = [
      normalizeHeadline(headlineData?.bestHeadline || ''),
      ...(Array.isArray(headlineData?.alternates) ? headlineData.alternates.map(normalizeHeadline) : [])
    ].filter(Boolean);

    const unique = [];
    for (const item of all) {
      if (!unique.includes(item)) unique.push(item);
      if (unique.length >= 3) break;
    }

    if (!unique.length) {
      return res.status(422).json({ error: 'Model returned no headline options' });
    }

    const bestHeadline = unique[0];
    const alternates = unique.slice(1);

    return res.status(200).json({
      ok: true,
      provider,
      model,
      topic,
      section,
      bestHeadline,
      alternates,
      rationale: cleanText(headlineData?.rationale || ''),
      research: {
        summary: cleanText(research?.summary || ''),
        keyFacts: Array.isArray(research?.keyFacts) ? research.keyFacts.map((v) => cleanText(v)).filter(Boolean) : [],
        mainAngles: Array.isArray(research?.mainAngles) ? research.mainAngles.map((v) => cleanText(v)).filter(Boolean) : [],
        otherPointsOfInterest: otherPoints,
        recentDevelopments: Array.isArray(research?.recentDevelopments)
          ? research.recentDevelopments.map((v) => cleanText(v)).filter(Boolean)
          : []
      }
    });
  } catch (error) {
    console.error('Generate headlines error:', error);
    return res.status(500).json({ error: 'Failed to generate headlines', details: error.message });
  }
};
