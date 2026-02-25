function resolveProvider(raw) {
  const value = String(raw || '').trim().toLowerCase();
  if (['anthropic', 'openai', 'gemini', 'grok'].includes(value)) return value;
  return 'anthropic';
}

function getDefaultModel(provider) {
  if (provider === 'openai') return process.env.OPENAI_MODEL || 'gpt-5';
  if (provider === 'gemini') return process.env.GEMINI_MODEL || 'gemini-3-pro-preview';
  if (provider === 'grok') return process.env.GROK_MODEL || 'grok-4';
  return process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6';
}

function stripCodeFences(text) {
  const value = String(text || '').trim();
  if (!value.startsWith('```')) return value;
  return value
    .replace(/^```[a-zA-Z0-9_-]*\s*/, '')
    .replace(/```$/, '')
    .trim();
}

function safeJsonParse(text) {
  const cleaned = stripCodeFences(text);
  try {
    return JSON.parse(cleaned);
  } catch (_) {
    return null;
  }
}

async function callAnthropicJson({ prompt, model, maxOutputTokens = 2400 }) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error('Missing ANTHROPIC_API_KEY');
  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model,
      max_tokens: maxOutputTokens,
      temperature: 0.3,
      messages: [{ role: 'user', content: prompt }]
    })
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Anthropic API error ${response.status}: ${body.slice(0, 200)}`);
  }
  const data = await response.json();
  const text = data?.content?.find((part) => part.type === 'text')?.text || '';
  const parsed = safeJsonParse(text);
  if (!parsed) throw new Error('Model did not return valid JSON');
  return parsed;
}

async function callOpenAiJson({ prompt, model, maxOutputTokens = 2400 }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('Missing OPENAI_API_KEY');
  const fallbackModel = process.env.OPENAI_FALLBACK_MODEL || 'gpt-4.1';
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${apiKey}`
  };
  const request = async (body) => {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers,
      body: JSON.stringify(body)
    });
    const raw = await response.text();
    if (!response.ok) {
      return { ok: false, status: response.status, raw };
    }
    const data = safeJsonParse(raw) || {};
    const text = data?.choices?.[0]?.message?.content || '';
    const parsed = safeJsonParse(text);
    if (!parsed) {
      return { ok: false, status: response.status, raw: 'Model did not return valid JSON' };
    }
    return { ok: true, parsed };
  };

  const attempts = [
    {
      model,
      temperature: 0.3,
      max_completion_tokens: maxOutputTokens,
      response_format: { type: 'json_object' },
      messages: [{ role: 'user', content: prompt }]
    },
    {
      model,
      max_completion_tokens: maxOutputTokens,
      response_format: { type: 'json_object' },
      messages: [{ role: 'user', content: prompt }]
    },
    {
      model,
      max_tokens: maxOutputTokens,
      response_format: { type: 'json_object' },
      messages: [{ role: 'user', content: prompt }]
    },
    {
      model: fallbackModel,
      max_tokens: maxOutputTokens,
      messages: [{ role: 'user', content: `${prompt}\n\nReturn valid JSON only.` }]
    }
  ];

  let lastError = 'OpenAI request failed';
  for (const attempt of attempts) {
    const result = await request(attempt);
    if (result.ok) return result.parsed;
    lastError = `OpenAI API error ${result.status}: ${String(result.raw || '').slice(0, 300)}`;
  }
  throw new Error(lastError);
}

async function callGeminiJson({ prompt, model, maxOutputTokens = 2400 }) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) throw new Error('Missing GEMINI_API_KEY');
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0.3,
        maxOutputTokens,
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
  if (!parsed) throw new Error('Model did not return valid JSON');
  return parsed;
}

async function callGrokJson({ prompt, model, maxOutputTokens = 2400 }) {
  const apiKey = process.env.GROK_API_KEY;
  if (!apiKey) throw new Error('Missing GROK_API_KEY');
  const response = await fetch('https://api.x.ai/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model,
      temperature: 0.3,
      max_tokens: maxOutputTokens,
      response_format: { type: 'json_object' },
      messages: [{ role: 'user', content: prompt }]
    })
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Grok API error ${response.status}: ${body.slice(0, 200)}`);
  }
  const data = await response.json();
  const text = data?.choices?.[0]?.message?.content || '';
  const parsed = safeJsonParse(text);
  if (!parsed) throw new Error('Model did not return valid JSON');
  return parsed;
}

async function callModelJson({ provider, model, prompt, maxOutputTokens = 2400 }) {
  const resolvedProvider = resolveProvider(provider);
  const resolvedModel = String(model || '').trim() || getDefaultModel(resolvedProvider);
  if (resolvedProvider === 'openai') return callOpenAiJson({ prompt, model: resolvedModel, maxOutputTokens });
  if (resolvedProvider === 'gemini') return callGeminiJson({ prompt, model: resolvedModel, maxOutputTokens });
  if (resolvedProvider === 'grok') return callGrokJson({ prompt, model: resolvedModel, maxOutputTokens });
  return callAnthropicJson({ prompt, model: resolvedModel, maxOutputTokens });
}

module.exports = {
  resolveProvider,
  getDefaultModel,
  callModelJson
};
