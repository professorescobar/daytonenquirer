const { requireAdmin } = require('./_admin-auth');
const { cleanText } = require('./_draft-utils');
const { resolveProvider, getDefaultModel, callModelJson } = require('./_llm-admin');

const ISSUE_LIBRARY = {
  headline: {
    too_generic: 'Make the headline more specific and concrete.',
    not_newsworthy: 'Raise the newsworthiness and timeliness of the angle.',
    unclear: 'Improve clarity and readability.',
    too_long: 'Make it tighter and shorter.',
    too_clickbait: 'Remove clickbait language and keep it credible.',
    weak_hook: 'Make the hook stronger and more thought-provoking.'
  },
  description: {
    too_vague: 'Make it more specific and informative.',
    too_hypey: 'Reduce hype and keep the tone credible.',
    too_generic: 'Avoid generic phrasing and add concrete context.',
    too_wordy: 'Make it concise without losing substance.',
    weak_seo: 'Improve search relevance while sounding natural.',
    weak_hook: 'Make the opening hook stronger.'
  },
  article: {
    not_long_enough: 'Add depth with useful detail and context.',
    too_much_fluff: 'Remove fluff and filler while preserving substance.',
    cheesy_corny: 'Remove cheesy or corny phrasing.',
    not_enough_enthusiasm: 'Increase energy slightly while staying credible.',
    too_much_enthusiasm: 'Tone down overhyped language.',
    not_thought_provoking: 'Increase insight and thought-provoking analysis.',
    repetitive: 'Remove repetition and tighten structure.',
    unclear_structure: 'Improve flow and section transitions.',
    overcautious: 'Reduce defensive caveats and write with clearer editorial confidence.',
    overhedging: 'Reduce hedging language and strengthen direct claims where supported.',
    surface_level: 'Add deeper analysis and stronger connective context.',
    hot_take_bias: 'Reduce hot-take framing and keep a balanced, grounded tone.'
  }
};

function normalizeIssues(target, rawIssues) {
  if (!Array.isArray(rawIssues)) return [];
  const valid = ISSUE_LIBRARY[target] || {};
  const unique = [];
  for (const item of rawIssues) {
    const id = cleanText(item);
    if (!id || !valid[id] || unique.includes(id)) continue;
    unique.push(id);
    if (unique.length >= 3) break;
  }
  return unique;
}

function buildPrompt({ target, title, description, content, issues }) {
  const rules = issues.map((id) => `- ${ISSUE_LIBRARY[target][id]}`).join('\n');

  if (target === 'headline') {
    return `
You are an editor rewriting a news headline.

Current headline:
${title}

Context (article summary):
${description || 'n/a'}

Rewrite goals:
${rules}

Return valid JSON only:
{
  "headline": "..."
}

Rules:
- Keep under 16 words.
- Keep it specific, factual, and thought-provoking.
- No markdown.
`;
  }

  if (target === 'description') {
    return `
You are an editor rewriting an SEO description.

Headline:
${title}

Current description:
${description}

Article:
${content}

Rewrite goals:
${rules}

Return valid JSON only:
{
  "description": "..."
}

Rules:
- 50 to 70 words.
- Plain text only.
- No markdown.
`;
  }

  return `
You are an editor rewriting a news article.

Headline:
${title}

Current article:
${content}

Rewrite goals:
${rules}

Return valid JSON only:
{
  "content": "..."
}

Rules:
- Keep it thought-provoking, detailed, and concise.
- Avoid fluff, repetition, and verbosity.
- Preserve factual integrity.
- 650-950 words.
- No markdown.
`;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const target = cleanText(req.body?.target || '').toLowerCase();
    if (!['headline', 'description', 'article'].includes(target)) {
      return res.status(400).json({ error: 'Invalid rewrite target' });
    }

    const title = cleanText(req.body?.title || '');
    const description = cleanText(req.body?.description || '');
    const content = cleanText(req.body?.content || '');
    const issues = normalizeIssues(target, req.body?.issues);
    const provider = resolveProvider(req.body?.provider);
    const model = cleanText(req.body?.model || '') || getDefaultModel(provider);

    if (issues.length < 1 || issues.length > 3) {
      return res.status(400).json({ error: 'Select 1 to 3 issues' });
    }

    if (target === 'headline' && !title) {
      return res.status(400).json({ error: 'Title is required for headline rewrite' });
    }
    if (target === 'description' && (!title || !description || !content)) {
      return res.status(400).json({ error: 'Title, description, and content are required for description rewrite' });
    }
    if (target === 'article' && (!title || !content)) {
      return res.status(400).json({ error: 'Title and content are required for article rewrite' });
    }

    const data = await callModelJson({
      provider,
      model,
      prompt: buildPrompt({ target, title, description, content, issues }),
      maxOutputTokens: target === 'article' ? 3400 : 900
    });

    if (target === 'headline') {
      const headline = cleanText(data?.headline || '');
      if (!headline) return res.status(422).json({ error: 'Model returned empty headline rewrite' });
      return res.status(200).json({ ok: true, provider, model, target, issues, headline });
    }

    if (target === 'description') {
      const rewrittenDescription = cleanText(data?.description || '');
      if (!rewrittenDescription) return res.status(422).json({ error: 'Model returned empty description rewrite' });
      return res.status(200).json({ ok: true, provider, model, target, issues, description: rewrittenDescription });
    }

    const rewrittenContent = cleanText(data?.content || '');
    if (!rewrittenContent) return res.status(422).json({ error: 'Model returned empty article rewrite' });
    return res.status(200).json({ ok: true, provider, model, target, issues, content: rewrittenContent });
  } catch (error) {
    console.error('Rewrite content error:', error);
    return res.status(500).json({ error: 'Failed to rewrite content', details: error.message });
  }
};
