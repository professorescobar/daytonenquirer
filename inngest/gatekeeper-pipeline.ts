// Skeleton only: wire this into your Inngest client bootstrap when ready.
// This file defines the 7-step contract for Layer 1 Gatekeeper.

import { randomUUID } from "node:crypto";
import { neon } from "@neondatabase/serverless";
import type { Inngest } from "inngest";

type SourceType = "rss" | "webhook" | "chat_yes" | "chat_specify";
type RelationToArchive = "none" | "duplicate" | "update" | "follow_up";
type Action = "reject" | "watch" | "promote";
type NextStep = "none" | "research_discovery" | "cluster_update";

type SignalRecord = {
  id: number;
  personaId: string;
  sourceType: SourceType;
  title: string;
  snippet: string;
  sectionHint: string;
  metadata: Record<string, unknown>;
  createdAt: string;
  isAutoPromoteEnabled: boolean;
};

type PriorArtMatch = {
  sourceType: "article" | "candidate";
  sourceId: string;
  sourceSlug: string | null;
  title: string;
  snippet: string;
  section: string | null;
  occurredAt: string;
  score: number;
};

type CorroborationSummary = {
  similarSignals24h: number;
  distinctSourceTypes24h: string[];
  distinctChatSessions24h: number;
};

type GatekeeperOutput = {
  is_newsworthy: number;
  is_local: boolean;
  confidence: number;
  category: string;
  relation_to_archive: RelationToArchive;
  event_key: string;
  action: Action;
  next_step: NextStep;
  policy_flags: string[];
  reasoning: string;
};

type PersistedDecision = GatekeeperOutput & {
  action: Action;
  next_step: NextStep;
  processed_at: string;
};

type ResearchSignalContext = {
  id: number;
  personaId: string;
  title: string;
  snippet: string | null;
  sourceName: string | null;
  sourceUrl: string | null;
  eventKey: string | null;
  dedupeKey: string | null;
};

type TavilyResult = {
  title: string;
  url: string;
  content: string;
  rawContent: string;
  score: number;
  publishedAt: string | null;
  query: string;
};

type EvidenceSource = {
  sourceUrl: string;
  title: string | null;
  content: string | null;
  score: number;
};

type EvidenceClaim = {
  claim: string;
  sourceUrl: string;
  evidenceQuote: string;
  confidence: number;
  whyItMatters: string;
};

const TEST_SIGNAL_ID = 12345;
const TEST_MODE_ENABLED = String(process.env.TOPIC_ENGINE_TEST_MODE || "").trim().toLowerCase() === "true";

function isTestSignalId(signalId: number): boolean {
  return TEST_MODE_ENABLED && signalId === TEST_SIGNAL_ID;
}

function cleanText(value: unknown, max = 8000): string {
  return String(value || "").trim().slice(0, max);
}

function toSafeJsonObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function stripCodeFences(text: string): string {
  const value = cleanText(text, 200000);
  if (!value.startsWith("```")) return value;
  return value.replace(/^```[a-zA-Z0-9_-]*\s*/, "").replace(/```$/, "").trim();
}

function extractJsonCandidate(text: string): string {
  const source = String(text || "");
  let start = -1;
  let openChar = "";
  for (let i = 0; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === "{" || ch === "[") {
      start = i;
      openChar = ch;
      break;
    }
  }
  if (start < 0) return "";

  const closeChar = openChar === "{" ? "}" : "]";
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === "\"") {
        inString = false;
      }
      continue;
    }
    if (ch === "\"") {
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
  return "";
}

function safeJsonParse(text: string): any {
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

function uniqueQueries(queries: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of queries) {
    const q = cleanText(raw, 220);
    if (!q) continue;
    const key = q.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(q);
    if (out.length >= 5) break;
  }
  return out;
}

function fallbackQueries(signal: ResearchSignalContext): string[] {
  const title = cleanText(signal.title, 220);
  const snippet = cleanText(signal.snippet || "", 240);
  const sourceName = cleanText(signal.sourceName || "", 120);
  return uniqueQueries([
    `${title} latest`,
    `${title} local impact`,
    sourceName ? `${title} ${sourceName}` : "",
    snippet ? `${title} ${snippet.split(/\s+/).slice(0, 8).join(" ")}` : "",
    `${title} timeline`
  ]);
}

async function loadResearchSignalContext(signalId: number): Promise<ResearchSignalContext> {
  if (isTestSignalId(signalId)) {
    return {
      id: TEST_SIGNAL_ID,
      personaId: "dayton-local",
      title: "Mock Signal 12345: Downtown Dayton road closures planned this weekend",
      snippet: "City crews announced temporary closures downtown for utility work and detours affecting weekend traffic.",
      sourceName: "Mock Feed",
      sourceUrl: "https://example.com/mock-signal-12345",
      eventKey: "mock-event-12345",
      dedupeKey: "mock-dedupe-12345"
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const rows = await sql`
    SELECT
      id,
      persona_id as "personaId",
      title,
      snippet,
      source_name as "sourceName",
      source_url as "sourceUrl",
      event_key as "eventKey",
      dedupe_key as "dedupeKey"
    FROM topic_signals
    WHERE id = ${signalId}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row) throw new Error(`Signal ${signalId} not found`);
  return {
    id: Number(row.id),
    personaId: cleanText(row.personaId, 255),
    title: cleanText(row.title, 500),
    snippet: cleanText(row.snippet || "", 4000) || null,
    sourceName: cleanText(row.sourceName || "", 500) || null,
    sourceUrl: cleanText(row.sourceUrl || "", 2000) || null,
    eventKey: cleanText(row.eventKey || "", 500) || null,
    dedupeKey: cleanText(row.dedupeKey || "", 500) || null
  };
}

async function generateResearchQueries(signal: ResearchSignalContext): Promise<string[]> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

  const model = cleanText(
    process.env.TOPIC_ENGINE_RESEARCH_QUERY_MODEL ||
      process.env.GEMINI_FLASH_MODEL ||
      process.env.GEMINI_MODEL ||
      "gemini-2.0-flash",
    120
  );
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    model
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const prompt = [
    "You generate search queries for local journalism research.",
    "Return strict JSON only: {\"queries\":[\"...\"]}",
    "Rules:",
    "- Generate 3 to 5 highly specific web search queries.",
    "- Focus on verifiable reporting evidence and source documents.",
    "- Prefer local/regional angle when available.",
    "- No commentary, no markdown, no extra keys.",
    "",
    `Title: ${signal.title}`,
    `Snippet: ${signal.snippet || ""}`,
    `Source Name: ${signal.sourceName || ""}`,
    `Source URL: ${signal.sourceUrl || ""}`
  ].join("\n");

  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0.2,
        maxOutputTokens: 700,
        responseMimeType: "application/json"
      }
    })
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Gemini query generation failed ${response.status}: ${body.slice(0, 200)}`);
  }
  const data = await response.json();
  const text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
  const parsed = safeJsonParse(text);
  const candidateQueries = Array.isArray(parsed?.queries) ? parsed.queries : [];
  const queries = uniqueQueries(candidateQueries.map((value: unknown) => cleanText(value, 220)));
  if (queries.length >= 3) return queries.slice(0, 5);
  return fallbackQueries(signal).slice(0, 5);
}

async function searchTavily(query: string): Promise<TavilyResult[]> {
  const apiKey = cleanText(process.env.TAVILY_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing TAVILY_API_KEY");

  const response = await fetch("https://api.tavily.com/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      api_key: apiKey,
      query,
      search_depth: "advanced",
      include_raw_content: true,
      max_results: 8
    })
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Tavily search failed ${response.status}: ${body.slice(0, 200)}`);
  }

  const data = await response.json();
  const rawResults = Array.isArray(data?.results) ? data.results : [];
  return rawResults.map((item: any) => {
    const title = cleanText(item?.title || "", 600);
    const url = cleanText(item?.url || "", 2000);
    const content = cleanText(item?.content || "", 18000);
    const rawContent = cleanText(item?.raw_content || "", 50000);
    const score = Number.isFinite(Number(item?.score)) ? Number(item?.score) : 0;
    const publishedAtRaw = cleanText(item?.published_date || item?.published_at || "", 120);
    const publishedAt = publishedAtRaw || null;
    return {
      title,
      url,
      content,
      rawContent,
      score,
      publishedAt,
      query
    };
  });
}

function parseSourceDomain(url: string): string | null {
  if (!url) return null;
  try {
    return new URL(url).hostname.toLowerCase();
  } catch (_) {
    return null;
  }
}

function selectTopResults(results: TavilyResult[]): TavilyResult[] {
  const deduped: TavilyResult[] = [];
  const seen = new Set<string>();
  const sorted = [...results].sort((a, b) => b.score - a.score);
  for (const item of sorted) {
    if (!item.url) continue;
    const key = item.url.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
    if (deduped.length >= 5) break;
  }
  return deduped;
}

async function persistResearchArtifacts(
  signal: ResearchSignalContext,
  queries: string[],
  selected: TavilyResult[],
  allResultCount: number
): Promise<void> {
  if (!selected.length) return;

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const runId = randomUUID();
  const engineId = randomUUID();
  const candidateId = randomUUID();

  for (let index = 0; index < selected.length; index += 1) {
    const result = selected[index];
    const publishedAt = result.publishedAt ? new Date(result.publishedAt) : null;
    const safePublishedAt =
      publishedAt && !Number.isNaN(publishedAt.getTime()) ? publishedAt.toISOString() : null;
    const metadata = {
      signalId: signal.id,
      personaId: signal.personaId,
      eventKey: signal.eventKey,
      dedupeKey: signal.dedupeKey,
      query: result.query,
      score: result.score,
      rank: index + 1,
      selectedTopN: 5,
      queryCount: queries.length,
      fetchedResultCount: allResultCount,
      tavily: {
        search_depth: "advanced",
        include_raw_content: true
      },
      raw_content: result.rawContent
    };

    await sql`
      INSERT INTO research_artifacts (
        id,
        run_id,
        engine_id,
        candidate_id,
        signal_id,
        persona_id,
        stage,
        artifact_type,
        source_url,
        source_domain,
        title,
        published_at,
        content,
        metadata,
        created_at
      )
      SELECT
        ${randomUUID()},
        ${runId},
        ${engineId},
        ${candidateId},
        ${signal.id},
        ${signal.personaId || null},
        'research_discovery',
        'tavily_result',
        ${result.url || null},
        ${parseSourceDomain(result.url)},
        ${result.title || null},
        ${safePublishedAt},
        ${result.content || null},
        ${toSafeJsonObject(metadata)}::jsonb,
        NOW()
      WHERE NOT EXISTS (
        SELECT 1
        FROM research_artifacts ra
        WHERE ra.signal_id = ${signal.id}
          AND ra.stage = 'research_discovery'
          AND ra.artifact_type = 'tavily_result'
          AND ra.source_url = ${result.url}
      )
    `;
  }
}

async function runResearchDiscovery(signalId: number): Promise<{ queries: string[]; saved: number; fetched: number }> {
  const signal = await loadResearchSignalContext(signalId);
  const queries = await generateResearchQueries(signal);
  const allResults: TavilyResult[] = [];
  for (const query of queries) {
    const results = await searchTavily(query);
    allResults.push(...results);
  }
  const topResults = selectTopResults(allResults);
  await persistResearchArtifacts(signal, queries, topResults, allResults.length);
  return {
    queries,
    saved: topResults.length,
    fetched: allResults.length
  };
}

async function loadEvidenceSources(signalId: number): Promise<EvidenceSource[]> {
  if (isTestSignalId(signalId)) {
    return [
      {
        sourceUrl: "https://example.com/mock-signal-12345",
        title: "City announces weekend downtown detours",
        content: "Dayton public works said lane closures start Saturday morning and detours will be posted.",
        score: 0.92
      },
      {
        sourceUrl: "https://example.com/mock-signal-12345-traffic",
        title: "Transit agency updates downtown service map",
        content: "RTA said two routes will shift stops near utility work zones for safety through Sunday night.",
        score: 0.88
      }
    ];
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const rows = await sql`
    SELECT
      source_url as "sourceUrl",
      title,
      content,
      CASE
        WHEN COALESCE(metadata->>'score', '') ~ '^[0-9]+(\\.[0-9]+)?$'
          THEN (metadata->>'score')::numeric
        ELSE 0
      END as "score"
    FROM research_artifacts
    WHERE signal_id = ${signalId}
      AND stage = 'research_discovery'
      AND artifact_type = 'tavily_result'
      AND source_url IS NOT NULL
    ORDER BY "score" DESC, created_at DESC
    LIMIT 5
  `;

  return rows
    .map((row: any) => ({
      sourceUrl: cleanText(row.sourceUrl, 2000),
      title: cleanText(row.title || "", 600) || null,
      content: cleanText(row.content || "", 12000) || null,
      score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0
    }))
    .filter((row: EvidenceSource) => row.sourceUrl);
}

function clampConfidence(value: unknown): number {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0.5;
  return Math.max(0, Math.min(1, num));
}

async function extractEvidenceClaimsWithGemini(
  signal: ResearchSignalContext,
  sources: EvidenceSource[]
): Promise<EvidenceClaim[]> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

  const candidateModels = Array.from(
    new Set(
      [
        cleanText(process.env.TOPIC_ENGINE_EVIDENCE_MODEL || "", 120),
        cleanText(process.env.GEMINI_PRO_MODEL || "", 120),
        cleanText(process.env.GEMINI_MODEL || "", 120),
        "gemini-1.5-pro-002",
        "gemini-2.5-pro",
        "gemini-2.0-flash"
      ].filter(Boolean)
    )
  );
  const sourceContext = sources
    .slice(0, 5)
    .map((source, index) =>
      [
        `Source ${index + 1}:`,
        `URL: ${source.sourceUrl}`,
        `Title: ${source.title || ""}`,
        `Score: ${source.score}`,
        `Content: ${cleanText(source.content || "", 5000)}`
      ].join("\n")
    )
    .join("\n\n");
  const prompt = [
    "You extract evidence claims for newsroom writing from provided sources only.",
    "Return strict JSON only in this shape:",
    "{\"claims\":[{\"claim\":\"...\",\"sourceUrl\":\"...\",\"evidenceQuote\":\"...\",\"confidence\":0.0,\"whyItMatters\":\"...\"}]}",
    "Rules:",
    "- Use only provided sources.",
    "- sourceUrl must exactly match one provided URL.",
    "- Return 2 to 5 claims max.",
    "- Keep evidenceQuote under 280 chars.",
    "- No markdown and no keys outside schema.",
    "",
    `Signal Title: ${signal.title}`,
    `Signal Snippet: ${signal.snippet || ""}`,
    "",
    sourceContext
  ].join("\n");

  let text = "";
  let lastError = "Gemini evidence extraction failed";
  for (const model of candidateModels) {
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
      model
    )}:generateContent?key=${encodeURIComponent(apiKey)}`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.2,
          maxOutputTokens: 1800,
          responseMimeType: "application/json"
        }
      })
    });
    if (response.ok) {
      const data = await response.json();
      text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
      if (text) break;
      lastError = `Gemini evidence extraction returned empty content for model ${model}`;
      continue;
    }
    const body = await response.text();
    lastError = `Gemini evidence extraction failed for model ${model} ${response.status}: ${body.slice(0, 200)}`;
  }
  if (!text) throw new Error(lastError);

  const parsed = safeJsonParse(text);
  const claims = Array.isArray(parsed?.claims) ? parsed.claims : [];
  return claims.map((item: any) => ({
    claim: cleanText(item?.claim || "", 600),
    sourceUrl: cleanText(item?.sourceUrl || "", 2000),
    evidenceQuote: cleanText(item?.evidenceQuote || "", 280),
    confidence: clampConfidence(item?.confidence),
    whyItMatters: cleanText(item?.whyItMatters || "", 500)
  }));
}

function normalizeEvidenceClaims(claims: EvidenceClaim[], sourceUrls: Set<string>): EvidenceClaim[] {
  const out: EvidenceClaim[] = [];
  const seenSource = new Set<string>();

  for (const claim of claims) {
    const sourceUrl = cleanText(claim.sourceUrl, 2000);
    const normalizedUrl = sourceUrl.toLowerCase();
    if (!sourceUrl || !sourceUrls.has(normalizedUrl)) continue;
    if (seenSource.has(normalizedUrl)) continue;
    if (!claim.claim || !claim.evidenceQuote) continue;
    seenSource.add(normalizedUrl);
    out.push({
      claim: cleanText(claim.claim, 600),
      sourceUrl,
      evidenceQuote: cleanText(claim.evidenceQuote, 280),
      confidence: clampConfidence(claim.confidence),
      whyItMatters: cleanText(claim.whyItMatters, 500)
    });
    if (out.length >= 5) break;
  }

  return out;
}

async function persistEvidenceArtifacts(signal: ResearchSignalContext, claims: EvidenceClaim[]): Promise<number> {
  if (!claims.length) return 0;

  if (isTestSignalId(signal.id)) {
    console.log("test-mode persistEvidenceArtifacts skip", {
      signalId: signal.id,
      claimCount: claims.length,
      claims
    });
    return claims.length;
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const runId = randomUUID();
  const engineId = randomUUID();
  const candidateId = randomUUID();

  let saved = 0;
  for (let index = 0; index < claims.length; index += 1) {
    const claim = claims[index];
    const metadata = {
      signalId: signal.id,
      personaId: signal.personaId,
      rank: index + 1,
      confidence: claim.confidence,
      evidenceQuote: claim.evidenceQuote,
      whyItMatters: claim.whyItMatters
    };

    const rows = await sql`
      INSERT INTO research_artifacts (
        id,
        run_id,
        engine_id,
        candidate_id,
        signal_id,
        persona_id,
        stage,
        artifact_type,
        source_url,
        source_domain,
        title,
        published_at,
        content,
        metadata,
        created_at
      )
      SELECT
        ${randomUUID()},
        ${runId},
        ${engineId},
        ${candidateId},
        ${signal.id},
        ${signal.personaId || null},
        'evidence_extraction',
        'evidence_extract',
        ${claim.sourceUrl},
        ${parseSourceDomain(claim.sourceUrl)},
        ${`Evidence claim ${index + 1}`},
        ${null},
        ${claim.claim},
        ${toSafeJsonObject(metadata)}::jsonb,
        NOW()
      WHERE NOT EXISTS (
        SELECT 1
        FROM research_artifacts ra
        WHERE ra.signal_id = ${signal.id}
          AND ra.stage = 'evidence_extraction'
          AND ra.artifact_type = 'evidence_extract'
          AND ra.source_url = ${claim.sourceUrl}
      )
      RETURNING id
    `;
    if (rows.length) saved += 1;
  }

  return saved;
}

async function runEvidenceExtraction(signalId: number): Promise<{
  sourceCount: number;
  claimCount: number;
  saved: number;
  skipped?: boolean;
}> {
  const signal = await loadResearchSignalContext(signalId);
  const sources = await loadEvidenceSources(signalId);
  if (!sources.length) {
    return { sourceCount: 0, claimCount: 0, saved: 0, skipped: true };
  }

  const rawClaims = await extractEvidenceClaimsWithGemini(signal, sources);
  const allowedUrls = new Set(sources.map((source) => source.sourceUrl.toLowerCase()));
  const claims = normalizeEvidenceClaims(rawClaims, allowedUrls);
  const saved = await persistEvidenceArtifacts(signal, claims);

  return {
    sourceCount: sources.length,
    claimCount: claims.length,
    saved
  };
}

// STEP 1: load_signal
async function loadSignalById(signalId: number): Promise<SignalRecord> {
  if (isTestSignalId(signalId)) {
    return {
      id: TEST_SIGNAL_ID,
      personaId: "dayton-local",
      sourceType: "rss",
      title: "Mock Signal 12345: Downtown Dayton road closures planned this weekend",
      snippet: "City crews announced temporary closures downtown for utility work and detours affecting weekend traffic.",
      sectionHint: "local",
      metadata: {
        testMode: true,
        signalId: TEST_SIGNAL_ID
      },
      createdAt: new Date().toISOString(),
      isAutoPromoteEnabled: true
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const rows = await sql`
    SELECT
      s.id,
      s.persona_id as "personaId",
      s.source_type as "sourceType",
      s.title,
      COALESCE(s.snippet, '') as "snippet",
      COALESCE(s.section_hint, '') as "sectionHint",
      COALESCE(s.metadata, '{}'::jsonb) as "metadata",
      s.created_at as "createdAt",
      COALESCE(te.is_auto_promote_enabled, false) as "isAutoPromoteEnabled"
    FROM topic_signals s
    LEFT JOIN topic_engines te
      ON te.persona_id = s.persona_id
    WHERE s.id = ${signalId}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row) {
    throw new Error(`Signal ${signalId} not found`);
  }

  return {
    id: Number(row.id),
    personaId: cleanText(row.personaId, 255),
    sourceType: cleanText(row.sourceType, 30) as SourceType,
    title: cleanText(row.title, 500),
    snippet: cleanText(row.snippet, 8000),
    sectionHint: cleanText(row.sectionHint, 255),
    metadata: toSafeJsonObject(row.metadata),
    createdAt: new Date(row.createdAt).toISOString(),
    isAutoPromoteEnabled: Boolean(row.isAutoPromoteEnabled)
  };
}

// STEP 2: lookup_prior_art
async function lookupPriorArt(signal: SignalRecord): Promise<PriorArtMatch[]> {
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const title = cleanText(signal.title, 500);
  const snippet = cleanText(signal.snippet, 1000);
  const snippetProbe = cleanText(snippet.split(/\s+/).slice(0, 12).join(" "), 220);

  const articleRows = await sql`
    SELECT
      'article'::text as "sourceType",
      a.id::text as "sourceId",
      a.slug::text as "sourceSlug",
      COALESCE(a.title, '')::text as "title",
      COALESCE(a.description, '')::text as "snippet",
      COALESCE(a.section, '')::text as "section",
      COALESCE(a.pub_date, a.created_at, NOW()) as "occurredAt",
      (
        CASE WHEN lower(COALESCE(a.title, '')) LIKE '%' || lower(${title}) || '%' THEN 0.85 ELSE 0 END +
        CASE WHEN lower(COALESCE(a.description, '')) LIKE '%' || lower(${snippetProbe}) || '%' THEN 0.55 ELSE 0 END
      )::float8 as "score"
    FROM articles a
    WHERE
      lower(COALESCE(a.title, '')) LIKE '%' || lower(${title}) || '%'
      OR lower(COALESCE(a.description, '')) LIKE '%' || lower(${snippetProbe}) || '%'
    ORDER BY "score" DESC, COALESCE(a.pub_date, a.created_at) DESC
    LIMIT 3
  `;

  const candidateRows = await sql`
    SELECT
      'candidate'::text as "sourceType",
      c.id::text as "sourceId",
      NULL::text as "sourceSlug",
      COALESCE(c.title, '')::text as "title",
      COALESCE(c.snippet, '')::text as "snippet",
      NULL::text as "section",
      COALESCE(c.published_at, c.created_at, NOW()) as "occurredAt",
      (
        CASE WHEN lower(COALESCE(c.title, '')) LIKE '%' || lower(${title}) || '%' THEN 0.85 ELSE 0 END +
        CASE WHEN lower(COALESCE(c.snippet, '')) LIKE '%' || lower(${snippetProbe}) || '%' THEN 0.55 ELSE 0 END
      )::float8 as "score"
    FROM topic_engine_candidates c
    WHERE c.persona_id = ${signal.personaId}
      AND (
        lower(COALESCE(c.title, '')) LIKE '%' || lower(${title}) || '%'
        OR lower(COALESCE(c.snippet, '')) LIKE '%' || lower(${snippetProbe}) || '%'
      )
    ORDER BY "score" DESC, COALESCE(c.published_at, c.created_at) DESC
    LIMIT 3
  `;

  return [...articleRows, ...candidateRows]
    .map((row: any) => ({
      sourceType: row.sourceType === "candidate" ? "candidate" : "article",
      sourceId: cleanText(row.sourceId, 80),
      sourceSlug: cleanText(row.sourceSlug || "", 255) || null,
      title: cleanText(row.title || "", 600),
      snippet: cleanText(row.snippet || "", 1200),
      section: cleanText(row.section || "", 120) || null,
      occurredAt: new Date(row.occurredAt || Date.now()).toISOString(),
      score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);
}

// STEP 3: check_corroboration_pre_ai
async function checkCorroborationPreAI(signal: SignalRecord): Promise<CorroborationSummary> {
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const titleProbe = cleanText(signal.title, 220);
  const snippetProbe = cleanText(signal.snippet.split(/\s+/).slice(0, 12).join(" "), 220);

  const corroborationRows = await sql`
    SELECT
      COUNT(*)::int as "similarSignals24h",
      COALESCE(
        array_agg(DISTINCT s.source_type) FILTER (WHERE s.source_type IS NOT NULL),
        ARRAY[]::text[]
      ) as "distinctSourceTypes24h",
      COUNT(DISTINCT s.session_hash)::int FILTER (
        WHERE s.source_type IN ('chat_yes', 'chat_specify')
          AND s.session_hash IS NOT NULL
          AND length(trim(s.session_hash)) > 0
      ) as "distinctChatSessions24h"
    FROM topic_signals s
    WHERE s.persona_id = ${signal.personaId}
      AND s.id <> ${signal.id}
      AND s.created_at >= NOW() - interval '24 hours'
      AND (
        lower(COALESCE(s.title, '')) LIKE '%' || lower(${titleProbe}) || '%'
        OR lower(COALESCE(s.snippet, '')) LIKE '%' || lower(${snippetProbe}) || '%'
      )
  `;
  const row = corroborationRows[0] || {};
  return {
    similarSignals24h: Number(row.similarSignals24h || 0),
    distinctSourceTypes24h: Array.isArray(row.distinctSourceTypes24h)
      ? row.distinctSourceTypes24h.map((v: unknown) => cleanText(v, 30)).filter(Boolean)
      : [],
    distinctChatSessions24h: Number(row.distinctChatSessions24h || 0)
  };
}

// STEP 4: gatekeeper_classify (Gemini 2.0 Flash)
async function classifyWithGatekeeper(
  signal: SignalRecord,
  priorArt: PriorArtMatch[],
  corroboration: CorroborationSummary
): Promise<GatekeeperOutput> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

  const model = cleanText(
    process.env.TOPIC_ENGINE_GATEKEEPER_MODEL ||
      process.env.GEMINI_FLASH_MODEL ||
      process.env.GEMINI_MODEL ||
      "gemini-1.5-flash",
    120
  );
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    model
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const prompt = [
    "You are a local newsroom gatekeeper classifier.",
    "Return strict JSON only.",
    "Schema:",
    "{\"is_newsworthy\":0-1,\"is_local\":true|false,\"confidence\":0-1,\"category\":\"...\",\"relation_to_archive\":\"none|duplicate|update|follow_up\",\"event_key\":\"...\",\"action\":\"reject|watch|promote\",\"next_step\":\"none|research_discovery|cluster_update\",\"policy_flags\":[\"...\"],\"reasoning\":\"...\"}",
    "Rules:",
    "- Prefer watch over promote when evidence is thin.",
    "- Keep next_step consistent with action.",
    "- event_key should be a stable short key for same event family.",
    "",
    `Signal: ${JSON.stringify(signal)}`,
    `Prior art: ${JSON.stringify(priorArt)}`,
    `Corroboration: ${JSON.stringify(corroboration)}`
  ].join("\n");

  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0.1,
        maxOutputTokens: 1300,
        responseMimeType: "application/json"
      }
    })
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Gemini gatekeeper classify failed ${response.status}: ${body.slice(0, 220)}`);
  }
  const data = await response.json();
  const text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
  const parsed = safeJsonParse(text) || {};

  const relation = ["none", "duplicate", "update", "follow_up"].includes(String(parsed.relation_to_archive || ""))
    ? (parsed.relation_to_archive as RelationToArchive)
    : "none";
  const action = ["reject", "watch", "promote"].includes(String(parsed.action || ""))
    ? (parsed.action as Action)
    : "watch";
  const nextStep = ["none", "research_discovery", "cluster_update"].includes(String(parsed.next_step || ""))
    ? (parsed.next_step as NextStep)
    : "none";
  const flags = Array.isArray(parsed.policy_flags) ? parsed.policy_flags : [];

  const eventKeySeed = cleanText(parsed.event_key || "", 140) || cleanText(signal.title, 120);
  const normalizedEventKey = eventKeySeed
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "")
    .slice(0, 120);

  return {
    is_newsworthy: Math.max(0, Math.min(1, Number(parsed.is_newsworthy) || 0)),
    is_local: Boolean(parsed.is_local),
    confidence: Math.max(0, Math.min(1, Number(parsed.confidence) || 0)),
    category: cleanText(parsed.category || "Other", 120) || "Other",
    relation_to_archive: relation,
    event_key: normalizedEventKey,
    action,
    next_step: nextStep,
    policy_flags: flags.map((value: unknown) => cleanText(value, 80)).filter(Boolean),
    reasoning: cleanText(parsed.reasoning || "", 3000)
  };
}

// STEP 5: apply_guardrails (deterministic code)
function applyGatekeeperGuardrails(
  signal: SignalRecord,
  modelOut: GatekeeperOutput,
  corroboration: CorroborationSummary
): GatekeeperOutput {
  const out: GatekeeperOutput = { ...modelOut };

  if (out.relation_to_archive === "duplicate") {
    out.action = "reject";
    out.next_step = "none";
    out.policy_flags = Array.from(new Set([...(out.policy_flags || []), "duplicate"]));
  }

  if (out.confidence < 0.55 || out.is_newsworthy < 0.5) {
    if (out.action === "promote") out.action = "watch";
    out.next_step = "none";
    out.policy_flags = Array.from(new Set([...(out.policy_flags || []), "low_evidence"]));
  }

  if (!out.is_local) {
    out.action = "reject";
    out.next_step = "none";
    out.policy_flags = Array.from(new Set([...(out.policy_flags || []), "not_local"]));
  }

  const isChatSignal = signal.sourceType === "chat_yes" || signal.sourceType === "chat_specify";
  const hasNonChatCorroboration = corroboration.distinctSourceTypes24h.some((t) => t === "rss" || t === "webhook");
  const hasTwoChatSessions = corroboration.distinctChatSessions24h >= 2;
  if (isChatSignal && out.action === "promote" && !(hasNonChatCorroboration || hasTwoChatSessions)) {
    out.action = "watch";
    out.next_step = "none";
    out.policy_flags = Array.from(new Set([...(out.policy_flags || []), "low_evidence"]));
  }

  // Safety brake: manual autonomy gate.
  if (!signal.isAutoPromoteEnabled && out.action === "promote") {
    out.action = "watch";
    out.next_step = "none";
    out.policy_flags = Array.from(new Set([...(out.policy_flags || []), "auto_promote_disabled"]));
  }

  if (out.action === "reject" || out.action === "watch") {
    out.next_step = "none";
  } else if (out.action === "promote") {
    out.next_step = out.relation_to_archive === "update" ? "cluster_update" : "research_discovery";
  }

  return out;
}

// STEP 6: persist_decision
async function persistDecision(signalId: number, decision: GatekeeperOutput): Promise<PersistedDecision> {
  if (isTestSignalId(signalId)) {
    // Test-mode path: avoid DB writes for synthetic signal IDs.
    console.log("test-mode persistDecision skip", {
      signalId,
      action: decision.action,
      nextStep: decision.next_step,
      reviewDecision:
        decision.action === "promote" ? "promoted" : decision.action === "reject" ? "rejected" : "pending_review",
      decision
    });
    return {
      ...decision,
      processed_at: new Date().toISOString()
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const reviewDecision =
    decision.action === "promote" ? "promoted" : decision.action === "reject" ? "rejected" : "pending_review";

  const rows = await sql`
    UPDATE topic_signals
    SET
      is_newsworthy = ${decision.is_newsworthy},
      is_local = ${decision.is_local},
      confidence = ${decision.confidence},
      category = ${cleanText(decision.category, 120) || null},
      relation_to_archive = ${decision.relation_to_archive},
      event_key = ${cleanText(decision.event_key, 500) || null},
      action = ${decision.action},
      next_step = ${decision.next_step},
      policy_flags = ${Array.isArray(decision.policy_flags) ? decision.policy_flags : []},
      reasoning = ${cleanText(decision.reasoning, 4000)},
      review_decision = ${reviewDecision},
      processed_at = NOW(),
      updated_at = NOW()
    WHERE id = ${signalId}
    RETURNING
      action,
      next_step as "nextStep",
      processed_at as "processedAt"
  `;
  const row = rows[0];
  if (!row) throw new Error(`Failed to persist decision for signal ${signalId}`);

  return {
    ...decision,
    action: cleanText(row.action, 20) as Action,
    next_step: cleanText(row.nextStep, 40) as NextStep,
    processed_at: new Date(row.processedAt).toISOString()
  };
}

// STEP 7: route_next_step
async function routeNextStep(
  step: any,
  signalId: number,
  decision: PersistedDecision
): Promise<void> {
  if (decision.next_step === "research_discovery") {
    await step.sendEvent("emit-research-start", {
      name: "research.start",
      data: {
        signalId
      }
    });
    return;
  }
  if (decision.next_step === "cluster_update") {
    await step.sendEvent("emit-cluster-update-start", {
      name: "cluster.update.start",
      data: {
        signalId
      }
    });
    return;
  }
  if (decision.next_step === "none") return;
}

/**
 * Register this skeleton in your Inngest bootstrap.
 * Example:
 * export const gatekeeperPipeline = createGatekeeperPipeline(inngest);
 */
export function createGatekeeperPipeline(inngest: Inngest) {
  return inngest.createFunction(
    { id: "gatekeeper-pipeline" },
    { event: "signal.received" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const signal = await step.run("1-load_signal", async () => loadSignalById(signalId));
      const priorArt = await step.run("2-lookup_prior_art", async () => lookupPriorArt(signal));
      const corroboration = await step.run("3-check_corroboration_pre_ai", async () => checkCorroborationPreAI(signal));
      const modelOut = await step.run("4-gatekeeper_classify", async () =>
        classifyWithGatekeeper(signal, priorArt, corroboration)
      );
      const guarded = await step.run("5-apply_guardrails", async () =>
        applyGatekeeperGuardrails(signal, modelOut, corroboration)
      );
      const persisted = await step.run("6-persist_decision", async () => persistDecision(signalId, guarded));
      await step.run("7-route_next_step", async () => routeNextStep(step, signalId, persisted));

      return {
        ok: true,
        signalId,
        action: persisted.action,
        nextStep: persisted.next_step
      };
    }
  );
}

export function createResearchStartFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "research-start" },
    { event: "research.start" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("research-discovery", async () => runResearchDiscovery(signalId));
      await step.sendEvent("emit-evidence-extraction-start", {
        name: "evidence.extraction.start",
        data: {
          signalId
        }
      });
      return {
        ok: true,
        signalId,
        ...result
      };
    }
  );
}

export function createEvidenceExtractionStartFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "evidence-extraction-start" },
    { event: "evidence.extraction.start" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("evidence-extraction", async () => runEvidenceExtraction(signalId));
      return {
        ok: true,
        signalId,
        ...result
      };
    }
  );
}

export function createClusterUpdateStartFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "cluster-update-start" },
    { event: "cluster.update.start" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      await step.run("cluster-update-placeholder", async () => {
        console.log("cluster.update.start received", { signalId });
        return { ok: true };
      });

      return {
        ok: true,
        signalId,
        routed: true,
        stage: "cluster_update"
      };
    }
  );
}

export function createEvidenceExtractionMockFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "evidence-extraction-mock" },
    { event: "evidence.extraction.mock" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("evidence-extraction-direct", async () => runEvidenceExtraction(signalId));
      return {
        ok: true,
        signalId,
        ...result
      };
    }
  );
}

export function createManualGatekeeperRouteFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "gatekeeper-manual-route" },
    { event: "signal.gatekeeper.route.manual" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const nextStep = cleanText(event?.data?.nextStep || event?.data?.next_step || "", 40).toLowerCase();
      const action = cleanText(event?.data?.action || "", 30).toLowerCase();
      const shouldResearch = nextStep === "research_discovery" || action === "promote";
      if (!shouldResearch) {
        return {
          ok: true,
          signalId,
          routed: false,
          reason: "next_step_not_research_discovery"
        };
      }

      await step.sendEvent("emit-research-start-from-manual", {
        name: "research.start",
        data: {
          signalId,
          trigger: "admin_manual"
        }
      });

      return {
        ok: true,
        signalId,
        routed: true,
        targetEvent: "research.start"
      };
    }
  );
}

export function createResearchDiscoveryMockFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "research-discovery-mock" },
    { event: "signal.research_discovery.mock" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("research-discovery-direct", async () => runResearchDiscovery(signalId));
      return {
        ok: true,
        signalId,
        ...result
      };
    }
  );
}
