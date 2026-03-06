const { neon } = require('@neondatabase/serverless');
const { callModelJson, getDefaultModel } = require('./_llm-admin');
const { getPersonaLabel } = require('../lib/personas');

const MAX_QUERY_CHARS = 1400;
const MAX_HISTORY_ITEMS = 8;
const MAX_HISTORY_CHARS = 500;
const MAX_ARTICLE_CONTEXT_CHARS = 14000;
const DEFAULT_CHAT_PROVIDER = 'gemini';
const DEFAULT_CHAT_MODEL = 'gemini-2.0-flash';
const RELATED_INTENTS = new Set(['out_of_bounds', 'off_topic']);

function cleanText(value, max = 4000) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function toPlainText(value) {
  return String(value || '')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function isMissingColumn(error, columnName) {
  return new RegExp(`column\\s+"?${columnName}"?\\s+does\\s+not\\s+exist`, 'i').test(String(error?.message || ''));
}

function normalizeProvider(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'google') return 'gemini';
  if (raw === 'xai') return 'grok';
  if (['openai', 'gemini', 'anthropic', 'grok'].includes(raw)) return raw;
  return '';
}

function providerHasApiKey(provider) {
  if (provider === 'openai') return Boolean(process.env.OPENAI_API_KEY);
  if (provider === 'gemini') return Boolean(process.env.GEMINI_API_KEY);
  if (provider === 'grok') return Boolean(process.env.GROK_API_KEY);
  if (provider === 'anthropic') return Boolean(process.env.ANTHROPIC_API_KEY);
  return false;
}

function pickChatProvider(preferred) {
  const normalized = normalizeProvider(preferred);
  if (normalized && providerHasApiKey(normalized)) return normalized;
  for (const provider of [DEFAULT_CHAT_PROVIDER, 'openai', 'anthropic', 'grok']) {
    if (providerHasApiKey(provider)) return provider;
  }
  return normalized || 'anthropic';
}

function sanitizeHistory(history) {
  if (!Array.isArray(history)) return [];
  const items = history
    .slice(-MAX_HISTORY_ITEMS)
    .map((item) => ({
      role: String(item?.role || '').toLowerCase() === 'assistant' ? 'assistant' : 'user',
      content: cleanText(item?.content || '', MAX_HISTORY_CHARS)
    }))
    .filter((item) => item.content);
  return items;
}

function normalizeIntent(value, outOfScope) {
  const raw = String(value || '').trim().toLowerCase();
  if (['article_question', 'topic_suggestion', 'feedback', 'abuse', 'off_topic', 'out_of_bounds', 'unknown'].includes(raw)) {
    return raw;
  }
  return outOfScope ? 'out_of_bounds' : 'article_question';
}

function normalizeInBounds(value, outOfScope) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'article' || raw === 'archive' || raw === 'out_of_bounds') return raw;
  return outOfScope ? 'out_of_bounds' : 'article';
}

async function fetchRelatedArticles(sql, slug, userQuery, limitCount = 3) {
  try {
    const rows = await sql`
      SELECT
        slug,
        title,
        description,
        section,
        pub_date as "pubDate",
        score
      FROM find_related_articles(${slug}, ${userQuery || null}, ${limitCount})
    `;
    return rows;
  } catch (error) {
    if (String(error?.message || '').toLowerCase().includes('find_related_articles')) {
      return [];
    }
    throw error;
  }
}

async function ensureChatFeedbackTable(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS topic_engine_chat_feedback (
      id BIGSERIAL PRIMARY KEY,
      article_id BIGINT,
      article_slug TEXT NOT NULL,
      section TEXT,
      persona_id TEXT,
      user_query TEXT NOT NULL,
      assistant_answer TEXT NOT NULL,
      out_of_scope BOOLEAN NOT NULL DEFAULT false,
      suggested_topic TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS idx_topic_engine_chat_feedback_slug_created
    ON topic_engine_chat_feedback(article_slug, created_at DESC)
  `;
}

async function getArticleForChat(sql, slug) {
  let hasStatusColumn = true;
  let hasPersonaColumn = true;

  async function queryArticle() {
    if (hasStatusColumn && hasPersonaColumn) {
      return sql`
        SELECT id, slug, title, description, content, section, persona
        FROM articles
        WHERE slug = ${slug}
          AND COALESCE(status, 'published') = 'published'
        LIMIT 1
      `;
    }
    if (hasStatusColumn && !hasPersonaColumn) {
      return sql`
        SELECT id, slug, title, description, content, section, NULL::text as persona
        FROM articles
        WHERE slug = ${slug}
          AND COALESCE(status, 'published') = 'published'
        LIMIT 1
      `;
    }
    if (!hasStatusColumn && hasPersonaColumn) {
      return sql`
        SELECT id, slug, title, description, content, section, persona
        FROM articles
        WHERE slug = ${slug}
        LIMIT 1
      `;
    }
    return sql`
      SELECT id, slug, title, description, content, section, NULL::text as persona
      FROM articles
      WHERE slug = ${slug}
      LIMIT 1
    `;
  }

  let rows;
  try {
    rows = await queryArticle();
  } catch (error) {
    const missingStatus = isMissingColumn(error, 'status');
    const missingPersona = isMissingColumn(error, 'persona');
    if (!missingStatus && !missingPersona) throw error;
    if (missingStatus) hasStatusColumn = false;
    if (missingPersona) hasPersonaColumn = false;
    rows = await queryArticle();
  }
  return rows[0] || null;
}

async function getPersonaChatConfig(sql, personaId) {
  if (!personaId) return null;
  try {
    const rows = await sql`
      SELECT
        runner_type as "runnerType",
        provider,
        model_or_endpoint as "modelOrEndpoint",
        prompt_template as "promptTemplate",
        workflow_config as "workflowConfig"
      FROM topic_engine_stage_configs
      WHERE persona_id = ${personaId}
        AND stage_name = 'final_review'
      LIMIT 1
    `;
    return rows[0] || null;
  } catch (_) {
    return null;
  }
}

async function getPersonaName(sql, personaId) {
  if (!personaId) return getPersonaLabel(personaId);
  try {
    const rows = await sql`
      SELECT COALESCE(NULLIF(trim(display_name), ''), '') as "displayName"
      FROM personas
      WHERE id = ${personaId}
      LIMIT 1
    `;
    const fromDb = cleanText(rows[0]?.displayName || '', 160);
    if (fromDb) return fromDb;
  } catch (_) {
    // fall through to static fallback
  }
  return getPersonaLabel(personaId);
}

function buildPrompt({ article, query, personaName, history, promptTemplate }) {
  const articleText = cleanText(
    `${toPlainText(article.description || '')}\n\n${toPlainText(article.content || '')}`,
    MAX_ARTICLE_CONTEXT_CHARS
  );
  const serializedHistory = history.length
    ? history.map((item) => `${item.role.toUpperCase()}: ${item.content}`).join('\n')
    : 'No prior messages.';
  const personaInstruction = cleanText(promptTemplate || '', 3000);

  return `
You are ${personaName}, a newsroom topic engine answering reader questions about one article.

Rules:
- Answer using only facts supported by the provided article context.
- If the question is outside the article, say so clearly and set outOfScope=true.
- Keep responses concise, useful, and conversational.
- Do not fabricate facts, names, dates, or quotes.

${personaInstruction ? `Additional persona guidance:\n${personaInstruction}\n` : ''}

Return JSON ONLY with this schema:
{
  "answer": "string",
  "intent": "article_question|off_topic|out_of_bounds|topic_suggestion|feedback|abuse|unknown",
  "inBounds": "article|archive|out_of_bounds",
  "outOfScope": true/false,
  "suggestedTopic": "string or empty",
  "confidence": 0.0-1.0
}

ARTICLE TITLE: ${cleanText(article.title || '', 300)}
ARTICLE SECTION: ${cleanText(article.section || '', 80)}
ARTICLE CONTEXT:
${articleText}

RECENT CHAT:
${serializedHistory}

READER QUESTION:
${query}
  `.trim();
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.setHeader('Allow', ['POST']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const slug = cleanText(req.body?.slug || '', 180);
  const query = cleanText(req.body?.query || '', MAX_QUERY_CHARS);
  const history = sanitizeHistory(req.body?.history);
  if (!slug) return res.status(400).json({ error: 'slug is required' });
  if (!query) return res.status(400).json({ error: 'query is required' });

  try {
    const sql = neon(process.env.DATABASE_URL);
    const article = await getArticleForChat(sql, slug);
    if (!article) return res.status(404).json({ error: 'Article not found' });

    const personaId = cleanText(article.persona || '', 255) || null;
    const personaName = await getPersonaName(sql, personaId);
    const stageConfig = await getPersonaChatConfig(sql, personaId);
    const workflowConfig = stageConfig?.workflowConfig && typeof stageConfig.workflowConfig === 'object'
      ? stageConfig.workflowConfig
      : {};

    const preferredProvider = normalizeProvider(
      workflowConfig.chatProvider ||
      stageConfig?.provider ||
      process.env.TOPIC_ENGINE_CHAT_PROVIDER ||
      DEFAULT_CHAT_PROVIDER
    );
    const provider = pickChatProvider(preferredProvider);
    const configuredModel = cleanText(
      workflowConfig.chatModel ||
      stageConfig?.modelOrEndpoint ||
      process.env.TOPIC_ENGINE_CHAT_MODEL ||
      DEFAULT_CHAT_MODEL,
      180
    );
    const model = configuredModel && !/^https?:\/\//i.test(configuredModel)
      ? configuredModel
      : getDefaultModel(provider);

    const promptTemplate = workflowConfig.chatPrompt || stageConfig?.promptTemplate || '';
    const prompt = buildPrompt({ article, query, personaName, history, promptTemplate });

    const result = await callModelJson({
      provider,
      model,
      prompt,
      maxOutputTokens: 900
    });

    const answer = cleanText(result?.answer || '', 4000) || 'I could not generate a reliable answer from this article.';
    const outOfScope = Boolean(result?.outOfScope);
    const intent = normalizeIntent(result?.intent, outOfScope);
    const inBounds = normalizeInBounds(result?.inBounds, outOfScope);
    const suggestedTopic = cleanText(result?.suggestedTopic || '', 500);
    const confidenceRaw = Number(result?.confidence);
    const confidence = Number.isFinite(confidenceRaw)
      ? Math.max(0, Math.min(1, confidenceRaw))
      : null;

    try {
      await ensureChatFeedbackTable(sql);
      await sql`
        INSERT INTO topic_engine_chat_feedback (
          article_id,
          article_slug,
          section,
          persona_id,
          user_query,
          assistant_answer,
          out_of_scope,
          suggested_topic,
          metadata
        )
        VALUES (
          ${article.id || null},
          ${article.slug},
          ${article.section || null},
          ${personaId},
          ${query},
          ${answer},
          ${outOfScope},
          ${suggestedTopic || null},
          ${{
            provider,
            model,
            intent,
            inBounds,
            confidence,
            historyCount: history.length
          }}::jsonb
        )
      `;
    } catch (feedbackError) {
      console.error('Article chat feedback insert failed:', feedbackError.message);
    }

    const relatedArticles = RELATED_INTENTS.has(intent)
      ? await fetchRelatedArticles(sql, article.slug, query, 3)
      : [];

    return res.status(200).json({
      answer,
      outOfScope,
      intent,
      inBounds,
      suggestedTopic: suggestedTopic || null,
      relatedArticles,
      confidence,
      provider,
      model
    });
  } catch (error) {
    console.error('Article chat error:', error);
    return res.status(500).json({ error: 'Failed to process chat request' });
  }
};
