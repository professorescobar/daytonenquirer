import { createHash } from "node:crypto";
import { neon } from "@neondatabase/serverless";
import type { Inngest } from "inngest";
import {
  PHASE_C_EXTRACTION_VERSION,
  deriveCandidateKey,
  normalizeCandidatePayload,
  type CandidateType
} from "./dictionary-substrate-phase-c-contract";

type DispatchTrigger = "scheduled" | "manual";
type PipelineRunStatus = "running" | "succeeded" | "failed" | "needs_review";
type ReviewQueueItemType = "fetch_failure" | "artifact_parse_failure" | "extraction_contract_failure";
type ReviewQueueSeverity = "low" | "medium" | "high" | "critical";
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const PHASE_C_STAGE_NAME = "phase_c_extraction_candidates";
const PHASE_C_EXTRACTION_EVENT = "dictionary.substrate.extraction.artifact";

type RootSourceRow = {
  id: string;
  sourceName: string;
  sourceType: string;
  sourceDomain: string;
  rootUrl: string;
  trustTier: "authoritative" | "corroborative" | "contextual";
  supportedEntityClasses: string[];
  crawlCadenceDays: number | null;
  freshnessSlaDays: number | null;
  failureThreshold: number;
  enabled: boolean;
  lastCrawledAt: string | null;
  latestArtifactId: string | null;
  latestArtifactContentHash: string | null;
};

type PipelineRunRow = {
  id: string;
  rootSourceId: string | null;
  parentRunId: string | null;
};

type FetchArtifactResult = {
  sourceUrl: string;
  sourceDomain: string;
  fetchedAt: string;
  httpStatus: number;
  contentType: string | null;
  rawHtml: string | null;
  extractedText: string | null;
  contentHash: string;
  changeState: "initial" | "unchanged" | "changed";
  priorArtifactId: string | null;
  metadata: Record<string, unknown>;
};

type ExtractionArtifactRow = {
  id: string;
  rootSourceId: string;
  substrateRunId: string;
  sourceUrl: string;
  sourceDomain: string;
  extractedText: string | null;
  metadata: Record<string, unknown>;
  contentType: string | null;
  fetchedAt: string;
};

type ExtractionExecutionCandidate = {
  candidateType: CandidateType;
  status: "extracted" | "rejected";
  candidateKey: string;
  candidatePayload: Record<string, unknown>;
  evidenceSnippets: string[];
  diagnostics: Record<string, unknown>[];
  rejectionReason: string | null;
};

type ExtractionExecutionResult = {
  extractionVersion: string;
  provider: string;
  model: string;
  normalizedCandidates: ExtractionExecutionCandidate[];
  metrics: {
    extractedCount: number;
    rejectedCount: number;
    diagnosticCount: number;
  };
};

type IngestionFailure = Error & {
  itemType: ReviewQueueItemType;
  crawlArtifactId?: string | null;
  details?: Record<string, unknown>;
};

type PhaseCExtractionFailure = Error & {
  failureKind: "contract_failure";
  details?: Record<string, unknown>;
};

function isArtifactHandoffReady(artifact: FetchArtifactResult): boolean {
  const handoff = artifact.metadata.handoff;
  if (!handoff || typeof handoff !== "object" || Array.isArray(handoff)) return false;
  return (handoff as { extraction_ready?: unknown }).extraction_ready === true;
}

function createIngestionFailure(
  itemType: ReviewQueueItemType,
  message: string,
  details?: Record<string, unknown>
): IngestionFailure {
  const error = new Error(message) as IngestionFailure;
  error.itemType = itemType;
  error.details = details || {};
  return error;
}

function createPhaseCExtractionFailure(
  message: string,
  details?: Record<string, unknown>
): PhaseCExtractionFailure {
  const error = new Error(message) as PhaseCExtractionFailure;
  error.failureKind = "contract_failure";
  error.details = details || {};
  return error;
}

function buildHandoffOutputPayload(params: {
  rootSourceId: string;
  artifactId: string | null;
  fetchedAt: string | null;
  changeState: FetchArtifactResult["changeState"] | null;
  priorArtifactId: string | null;
  handoffReady: boolean;
  reviewQueueId?: string;
  reviewSeverity?: ReviewQueueSeverity;
  failureCount?: number;
}) {
  return {
    artifactId: params.artifactId,
    rootSourceId: params.rootSourceId,
    fetchedAt: params.fetchedAt,
    changeState: params.changeState,
    handoffReady: params.handoffReady,
    extractionReady: params.handoffReady,
    priorArtifactId: params.priorArtifactId,
    nextStage: params.handoffReady ? PHASE_C_STAGE_NAME : null,
    phaseBoundary: "phase_b_ingestion_complete",
    phaseCEventEmitted: false,
    reviewQueueId: params.reviewQueueId || null,
    reviewSeverity: params.reviewSeverity || null,
    failureCount: params.failureCount ?? null
  };
}

async function markPhaseCEventEmission(params: {
  runId: string;
  phaseCDispatchAttempted?: boolean;
  phaseCEventEmitted: boolean;
  phaseCDispatchEventName?: string | null;
  phaseCDispatchArtifactId?: string | null;
  phaseCDispatchError?: string | null;
}) {
  const sql = getSql();
  const existingRows = await sql`
    SELECT output_payload as "outputPayload"
    FROM dictionary.dictionary_pipeline_runs
    WHERE id = ${params.runId}::uuid
    LIMIT 1
  `;

  const currentOutput = (existingRows[0]?.outputPayload || {}) as Record<string, unknown>;
  const nextOutput = {
    ...currentOutput,
    phaseCDispatchAttempted: params.phaseCDispatchAttempted ?? true,
    phaseCEventEmitted: params.phaseCEventEmitted,
    phaseCDispatchEventName: params.phaseCDispatchEventName || null,
    phaseCDispatchArtifactId: params.phaseCDispatchArtifactId || null,
    phaseCDispatchError: params.phaseCDispatchError || null
  };

  await sql`
    UPDATE dictionary.dictionary_pipeline_runs
    SET
      output_payload = ${JSON.stringify(nextOutput)}::jsonb,
      updated_at = NOW()
    WHERE id = ${params.runId}::uuid
  `;
}

function cleanText(value: unknown, max = 1000): string {
  return String(value || "").trim().slice(0, max);
}

function cleanArrayOfStrings(value: unknown, maxItems = 8, maxLength = 500): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((item) => cleanText(item, maxLength)).filter(Boolean).slice(0, maxItems);
}

function parsePositiveInt(value: unknown, fallback: number, min: number, max: number): number {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function getSql() {
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 4000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  return neon(databaseUrl);
}

function getPhaseCProvider(): "anthropic" | "openai" | "gemini" | "grok" {
  const configured = cleanText(process.env.DICTIONARY_SUBSTRATE_PHASE_C_PROVIDER || "", 40).toLowerCase();
  if (configured === "openai" || configured === "gemini" || configured === "grok") return configured;
  return "anthropic";
}

function getPhaseCModel(provider: "anthropic" | "openai" | "gemini" | "grok"): string {
  if (provider === "openai") {
    return cleanText(process.env.DICTIONARY_SUBSTRATE_PHASE_C_OPENAI_MODEL || process.env.OPENAI_MODEL || "gpt-5", 160);
  }
  if (provider === "gemini") {
    return cleanText(process.env.DICTIONARY_SUBSTRATE_PHASE_C_GEMINI_MODEL || process.env.GEMINI_MODEL || "gemini-3-pro-preview", 160);
  }
  if (provider === "grok") {
    return cleanText(process.env.DICTIONARY_SUBSTRATE_PHASE_C_GROK_MODEL || process.env.GROK_MODEL || "grok-4", 160);
  }
  return cleanText(
    process.env.DICTIONARY_SUBSTRATE_PHASE_C_ANTHROPIC_MODEL || process.env.ANTHROPIC_MODEL || "claude-sonnet-4-6",
    160
  );
}

function stripCodeFences(text: string): string {
  const value = String(text || "").trim();
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

async function callPhaseCModelJson(params: {
  provider: "anthropic" | "openai" | "gemini" | "grok";
  model: string;
  prompt: string;
  maxOutputTokens?: number;
}): Promise<any> {
  const maxOutputTokens = params.maxOutputTokens || 3200;

  if (params.provider === "openai") {
    const apiKey = cleanText(process.env.OPENAI_API_KEY || "", 400);
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY");
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: params.model,
        temperature: 0.2,
        max_completion_tokens: maxOutputTokens,
        response_format: { type: "json_object" },
        messages: [{ role: "user", content: params.prompt }]
      })
    });
    const raw = await response.text();
    if (!response.ok) {
      throw new Error(`OpenAI API error ${response.status}: ${raw.slice(0, 300)}`);
    }
    const parsed = safeJsonParse(safeJsonParse(raw)?.choices?.[0]?.message?.content || "");
    if (!parsed) throw new Error("OpenAI model did not return valid JSON");
    return parsed;
  }

  if (params.provider === "gemini") {
    const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 400);
    if (!apiKey) throw new Error("Missing GEMINI_API_KEY");
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(params.model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: params.prompt }] }],
        generationConfig: {
          temperature: 0.2,
          maxOutputTokens,
          responseMimeType: "application/json"
        }
      })
    });
    const raw = await response.text();
    if (!response.ok) {
      throw new Error(`Gemini API error ${response.status}: ${raw.slice(0, 300)}`);
    }
    const data = safeJsonParse(raw) || {};
    const text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
    const parsed = safeJsonParse(text);
    if (!parsed) throw new Error("Gemini model did not return valid JSON");
    return parsed;
  }

  if (params.provider === "grok") {
    const apiKey = cleanText(process.env.GROK_API_KEY || "", 400);
    if (!apiKey) throw new Error("Missing GROK_API_KEY");
    const response = await fetch("https://api.x.ai/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: params.model,
        temperature: 0.2,
        max_tokens: maxOutputTokens,
        response_format: { type: "json_object" },
        messages: [{ role: "user", content: params.prompt }]
      })
    });
    const raw = await response.text();
    if (!response.ok) {
      throw new Error(`Grok API error ${response.status}: ${raw.slice(0, 300)}`);
    }
    const parsed = safeJsonParse(safeJsonParse(raw)?.choices?.[0]?.message?.content || "");
    if (!parsed) throw new Error("Grok model did not return valid JSON");
    return parsed;
  }

  const apiKey = cleanText(process.env.ANTHROPIC_API_KEY || "", 400);
  if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");
  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01"
    },
    body: JSON.stringify({
      model: params.model,
      max_tokens: maxOutputTokens,
      temperature: 0.2,
      messages: [{ role: "user", content: params.prompt }]
    })
  });
  const raw = await response.text();
  if (!response.ok) {
    throw new Error(`Anthropic API error ${response.status}: ${raw.slice(0, 300)}`);
  }
  const parsedOuter = safeJsonParse(raw) || {};
  const text = parsedOuter?.content?.find((part: any) => part?.type === "text")?.text || "";
  const parsed = safeJsonParse(text);
  if (!parsed) throw new Error("Anthropic model did not return valid JSON");
  return parsed;
}

function normalizeWhitespace(value: string): string {
  return value
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function isArtifactExtractionReadyMetadata(metadata: Record<string, unknown>): boolean {
  const handoff = metadata?.handoff;
  if (!handoff || typeof handoff !== "object" || Array.isArray(handoff)) return false;
  return (handoff as { extraction_ready?: unknown }).extraction_ready === true;
}

function buildPhaseCPrompt(params: {
  rootSource: RootSourceRow;
  artifact: ExtractionArtifactRow;
  extractionVersion: string;
}): string {
  const text = cleanText(params.artifact.extractedText || "", 12000);
  const supportedEntityClasses = Array.isArray(params.rootSource.supportedEntityClasses)
    ? params.rootSource.supportedEntityClasses.map((value) => cleanText(value, 80)).filter(Boolean)
    : [];

  return [
    "You are running structured extraction for the Dayton Enquirer dictionary substrate.",
    "Return strict JSON only.",
    `Extraction contract version: ${params.extractionVersion}.`,
    "This is Phase C extraction only. Do not resolve merges, canonical IDs, or validation outcomes.",
    "Rules:",
    "- Emit fully qualified canonical names only.",
    "- jurisdiction_hint is non-authoritative scoping context only, never a resolved canonical link.",
    "- Reject vague or generic references like 'the mayor', 'police', 'city hall', or 'downtown' unless the reference is anchored to a fully qualified proposed record.",
    "- Non-diagnostic items must include at least one evidence snippet copied from the artifact text.",
    "- If unsure, reject the item instead of inventing detail.",
    "Allowed normalized vocab:",
    "- entity_kind: person, organization, government_body, institution, place, venue",
    "- alias_kind: short_name, alternate_name, abbreviation, historical_name",
    "- role_kind: elected_office, appointed_office, board_role, institutional_role",
    "- jurisdiction_type: city, county, township, village, school_district, neighborhood, district, state, country",
    "- diagnostic_type: generic_reference_rejected, schema_invalid, unsupported_candidate, empty_extraction, normalization_adjustment",
    "- diagnostic severity: low, medium, high",
    "Return a single JSON object with this shape:",
    JSON.stringify({
      entities: [
        {
          candidate_payload: {
            canonical_name: "string",
            entity_kind: "person",
            jurisdiction_hint: {
              label: "string",
              scope_type: "string|null",
              state_code: "string|null",
              country_code: "string|null",
              is_authoritative: false
            },
            description: "string|null",
            source_mentions: ["string"]
          },
          evidence_snippets: ["string"],
          diagnostics: [{ code: "string", message: "string" }]
        }
      ],
      aliases: [],
      roles: [],
      assertions: [],
      jurisdictions: [],
      diagnostics: [
        {
          candidate_payload: {
            diagnostic_type: "generic_reference_rejected",
            severity: "low",
            message: "string",
            related_candidate_key: null,
            details: {}
          },
          diagnostics: []
        }
      ],
      rejections: [
        {
          candidate_type: "entity",
          rejection_reason:
            "generic_reference|missing_required_field|invalid_normalized_value|missing_evidence|unsupported_candidate_type|non_authoritative_jurisdiction_hint",
          candidate_payload: {},
          evidence_snippets: ["string"],
          diagnostics: [{ code: "string", message: "string" }]
        }
      ]
    }),
    `Root source: ${params.rootSource.sourceName}`,
    `Root URL: ${params.rootSource.rootUrl}`,
    `Trust tier: ${params.rootSource.trustTier}`,
    `Supported entity classes: ${supportedEntityClasses.join(", ") || "none specified"}`,
    `Artifact source URL: ${params.artifact.sourceUrl}`,
    `Artifact fetched at: ${params.artifact.fetchedAt}`,
    "Artifact text:",
    text
  ].join("\n");
}

function normalizeDiagnosticArray(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return null;
      return item as Record<string, unknown>;
    })
    .filter(Boolean) as Record<string, unknown>[];
}

function normalizeRawExtractionItem(
  candidateType: CandidateType,
  raw: unknown,
  status: "extracted" | "rejected",
  explicitRejectionReason?: string | null
): ExtractionExecutionCandidate {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error(`${candidateType} extraction item must be an object`);
  }

  const item = raw as Record<string, unknown>;
  const normalizedPayload = normalizeCandidatePayload(
    candidateType,
    item.candidate_payload || item.payload || {}
  ) as Record<string, unknown>;
  const candidateKey = deriveCandidateKey(candidateType, normalizedPayload as any);
  const evidenceSnippets =
    candidateType === "diagnostic" ? [] : cleanArrayOfStrings(item.evidence_snippets, 10, 500);
  if (candidateType !== "diagnostic" && !evidenceSnippets.length) {
    throw new Error(`${candidateType} extraction item must include evidence_snippets`);
  }

  return {
    candidateType,
    status,
    candidateKey,
    candidatePayload: normalizedPayload,
    evidenceSnippets,
    diagnostics: normalizeDiagnosticArray(item.diagnostics),
    rejectionReason: status === "rejected" ? cleanText(explicitRejectionReason || item.rejection_reason, 120) || "unsupported_candidate_type" : null
  };
}

function normalizePhaseCExtractionResponse(raw: unknown): ExtractionExecutionCandidate[] {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("Phase C extraction response must be an object");
  }

  const payload = raw as Record<string, unknown>;
  const normalized: ExtractionExecutionCandidate[] = [];
  const extractedGroups: Array<{ key: string; candidateType: CandidateType }> = [
    { key: "entities", candidateType: "entity" },
    { key: "aliases", candidateType: "alias" },
    { key: "roles", candidateType: "role" },
    { key: "assertions", candidateType: "assertion" },
    { key: "jurisdictions", candidateType: "jurisdiction" },
    { key: "diagnostics", candidateType: "diagnostic" }
  ];

  for (const group of extractedGroups) {
    const items = payload[group.key];
    if (items === undefined) continue;
    if (!Array.isArray(items)) {
      throw new Error(`${group.key} must be an array`);
    }
    for (const item of items) {
      normalized.push(
        normalizeRawExtractionItem(group.candidateType, item, "extracted")
      );
    }
  }

  const rejections = payload.rejections;
  if (rejections !== undefined) {
    if (!Array.isArray(rejections)) {
      throw new Error("rejections must be an array");
    }
    for (const item of rejections) {
      if (!item || typeof item !== "object" || Array.isArray(item)) {
        throw new Error("rejection item must be an object");
      }
      const rawItem = item as Record<string, unknown>;
      const candidateType = cleanText(rawItem.candidate_type, 40) as CandidateType;
      if (
        candidateType !== "entity" &&
        candidateType !== "alias" &&
        candidateType !== "role" &&
        candidateType !== "assertion" &&
        candidateType !== "jurisdiction" &&
        candidateType !== "diagnostic"
      ) {
        throw new Error(`Unsupported rejection candidate_type: ${String(rawItem.candidate_type || "")}`);
      }
      normalized.push(
        normalizeRawExtractionItem(
          candidateType,
          rawItem,
          "rejected",
          cleanText(rawItem.rejection_reason, 120)
        )
      );
    }
  }

  return normalized;
}

async function runPhaseCExtractionExecution(params: {
  rootSource: RootSourceRow;
  artifact: ExtractionArtifactRow;
}): Promise<ExtractionExecutionResult> {
  if (!params.rootSource.enabled) {
    throw new Error(`Phase C extraction requires an enabled root source: ${params.rootSource.id}`);
  }
  if (!params.artifact.extractedText) {
    throw new Error("Phase C extraction requires artifact.extractedText");
  }
  if (!isArtifactExtractionReadyMetadata(params.artifact.metadata)) {
    throw new Error("Phase C extraction requires a handoff-ready artifact");
  }

  const provider = getPhaseCProvider();
  const model = getPhaseCModel(provider);
  const prompt = buildPhaseCPrompt({
    rootSource: params.rootSource,
    artifact: params.artifact,
    extractionVersion: PHASE_C_EXTRACTION_VERSION
  });
  let rawResponse: any;
  try {
    rawResponse = await callPhaseCModelJson({
      provider,
      model,
      prompt,
      maxOutputTokens: 3200
    });
  } catch (error: any) {
    const message = cleanText(error?.message || "phase_c_model_call_failed", 2000);
    if (message.includes("did not return valid JSON")) {
      throw createPhaseCExtractionFailure(message, {
        provider,
        model
      });
    }
    throw error;
  }

  let normalizedCandidates: ExtractionExecutionCandidate[];
  try {
    normalizedCandidates = normalizePhaseCExtractionResponse(rawResponse);
  } catch (error: any) {
    throw createPhaseCExtractionFailure(
      cleanText(error?.message || "phase_c_extraction_contract_invalid", 2000),
      {
        provider,
        model
      }
    );
  }

  return {
    extractionVersion: PHASE_C_EXTRACTION_VERSION,
    provider,
    model,
    normalizedCandidates,
    metrics: {
      extractedCount: normalizedCandidates.filter((item) => item.status === "extracted" && item.candidateType !== "diagnostic").length,
      rejectedCount: normalizedCandidates.filter((item) => item.status === "rejected").length,
      diagnosticCount: normalizedCandidates.filter((item) => item.candidateType === "diagnostic").length
    }
  };
}

function shouldEscalateZeroOutputToReview(params: {
  rootSource: RootSourceRow;
  extraction: ExtractionExecutionResult;
}): boolean {
  if (params.extraction.metrics.extractedCount > 0) return false;
  if (!params.rootSource.supportedEntityClasses.length) return false;

  const hasContractFailureDiagnostic = params.extraction.normalizedCandidates.some((candidate) => {
    if (candidate.candidateType !== "diagnostic") return false;
    const diagnosticType = cleanText(
      (candidate.candidatePayload as Record<string, unknown>).diagnostic_type,
      80
    );
    return (
      diagnosticType === "schema_invalid" ||
      diagnosticType === "unsupported_candidate" ||
      diagnosticType === "empty_extraction"
    );
  });
  if (hasContractFailureDiagnostic) return true;

  return params.extraction.normalizedCandidates.some((candidate) => {
    if (candidate.status !== "rejected") return false;
    return (
      candidate.rejectionReason === "missing_required_field" ||
      candidate.rejectionReason === "invalid_normalized_value" ||
      candidate.rejectionReason === "unsupported_candidate_type" ||
      candidate.rejectionReason === "non_authoritative_jurisdiction_hint"
    );
  });
}

function stripHtmlToText(html: string): string {
  return normalizeWhitespace(
    html
      .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
      .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
      .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/gi, " ")
      .replace(/&amp;/gi, "&")
      .replace(/&lt;/gi, "<")
      .replace(/&gt;/gi, ">")
      .replace(/&#39;/gi, "'")
      .replace(/&quot;/gi, "\"")
  );
}

function buildNormalizedArtifact(body: string, contentType: string | null) {
  const normalizedBody = normalizeWhitespace(body);
  const isHtml = cleanText(contentType || "", 200).toLowerCase().includes("html");
  const extractedText = normalizedBody ? (isHtml ? stripHtmlToText(body) : normalizedBody) : null;
  const comparisonBasis = extractedText || normalizedBody;
  const contentHash = createHash("sha256").update(comparisonBasis).digest("hex");

  return {
    rawHtml: isHtml ? body : null,
    extractedText,
    comparisonBasis,
    contentHash,
    hashAlgorithm: "sha256"
  };
}

async function loadDueRootSources(limit: number): Promise<RootSourceRow[]> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      rs.id,
      rs.source_name as "sourceName",
      rs.source_type as "sourceType",
      rs.source_domain as "sourceDomain",
      rs.root_url as "rootUrl",
      rs.trust_tier as "trustTier",
      rs.supported_entity_classes as "supportedEntityClasses",
      rs.crawl_cadence_days as "crawlCadenceDays",
      rs.freshness_sla_days as "freshnessSlaDays",
      rs.failure_threshold as "failureThreshold",
      rs.enabled,
      rs.last_crawled_at as "lastCrawledAt",
      latest.id as "latestArtifactId",
      latest.content_hash as "latestArtifactContentHash"
    FROM dictionary.root_sources_due_for_ingestion() due
    JOIN dictionary.dictionary_root_sources rs
      ON rs.id = due.root_source_id
    LEFT JOIN LATERAL dictionary.latest_root_source_artifact(rs.id, rs.root_url) latest
      ON true
    ORDER BY
      CASE rs.trust_tier
        WHEN 'authoritative' THEN 0
        WHEN 'corroborative' THEN 1
        ELSE 2
      END,
      due.last_successful_crawl_at NULLS FIRST,
      rs.source_name ASC
    LIMIT ${limit}
  `;
  return rows as RootSourceRow[];
}

async function loadRootSourceById(rootSourceId: string): Promise<RootSourceRow | null> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      rs.id,
      rs.source_name as "sourceName",
      rs.source_type as "sourceType",
      rs.source_domain as "sourceDomain",
      rs.root_url as "rootUrl",
      rs.trust_tier as "trustTier",
      rs.supported_entity_classes as "supportedEntityClasses",
      rs.crawl_cadence_days as "crawlCadenceDays",
      rs.freshness_sla_days as "freshnessSlaDays",
      rs.failure_threshold as "failureThreshold",
      rs.enabled,
      rs.last_crawled_at as "lastCrawledAt",
      latest.id as "latestArtifactId",
      latest.content_hash as "latestArtifactContentHash"
    FROM dictionary.dictionary_root_sources rs
    LEFT JOIN LATERAL dictionary.latest_root_source_artifact(rs.id, rs.root_url) latest
      ON true
    WHERE rs.id = ${rootSourceId}::uuid
    LIMIT 1
  `;
  return (rows[0] as RootSourceRow) || null;
}

async function loadExtractionArtifactForPhaseC(
  rootSourceId: string,
  artifactId: string
): Promise<ExtractionArtifactRow | null> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      ca.id,
      ca.root_source_id as "rootSourceId",
      ca.substrate_run_id as "substrateRunId",
      ca.source_url as "sourceUrl",
      ca.source_domain as "sourceDomain",
      ca.extracted_text as "extractedText",
      ca.metadata,
      ca.content_type as "contentType",
      ca.fetched_at as "fetchedAt"
    FROM dictionary.dictionary_crawl_artifacts ca
    WHERE ca.id = ${artifactId}::uuid
      AND ca.root_source_id = ${rootSourceId}::uuid
    LIMIT 1
  `;
  return (rows[0] as ExtractionArtifactRow) || null;
}

async function persistPhaseCExtractionCandidates(params: {
  artifact: ExtractionArtifactRow;
  candidates: ExtractionExecutionCandidate[];
  extractionVersion: string;
}) {
  const sql = getSql();
  const queries = [
    sql`
      DELETE FROM dictionary.dictionary_extraction_candidates
      WHERE crawl_artifact_id = ${params.artifact.id}::uuid
        AND extraction_version = ${params.extractionVersion}
    `
  ];

  for (const candidate of params.candidates) {
    queries.push(
      sql`
        INSERT INTO dictionary.dictionary_extraction_candidates (
          substrate_run_id,
          root_source_id,
          crawl_artifact_id,
          candidate_type,
          status,
          candidate_key,
          candidate_payload,
          evidence_snippets,
          diagnostics,
          extraction_version,
          rejection_reason,
          created_at,
          updated_at
        )
        VALUES (
          ${params.artifact.substrateRunId}::uuid,
          ${params.artifact.rootSourceId}::uuid,
          ${params.artifact.id}::uuid,
          ${candidate.candidateType},
          ${candidate.status},
          ${candidate.candidateKey},
          ${JSON.stringify(candidate.candidatePayload)}::jsonb,
          ${JSON.stringify(candidate.evidenceSnippets)}::jsonb,
          ${JSON.stringify(candidate.diagnostics)}::jsonb,
          ${params.extractionVersion},
          ${candidate.rejectionReason},
          NOW(),
          NOW()
        )
        RETURNING
          id,
          candidate_type as "candidateType",
          status,
          candidate_key as "candidateKey",
          rejection_reason as "rejectionReason"
      `
    );
  }

  const transactionResults = await sql.transaction(queries);
  const persisted = transactionResults.slice(1).map((rows: any) => rows?.[0]).filter(Boolean);

  return {
    persistedRows: persisted,
    persistedCount: persisted.length,
    extractedCount: persisted.filter(
      (row: any) => row.status === "extracted" && row.candidateType !== "diagnostic"
    ).length,
    rejectedCount: persisted.filter((row: any) => row.status === "rejected").length,
    diagnosticCount: persisted.filter((row: any) => row.candidateType === "diagnostic").length
  };
}

async function createPipelineRun(params: {
  stageName: string;
  triggerType: string;
  parentRunId?: string | null;
  rootSourceId?: string | null;
  inputPayload?: Record<string, unknown>;
}): Promise<PipelineRunRow> {
  const sql = getSql();
  const rows = await sql`
    INSERT INTO dictionary.dictionary_pipeline_runs (
      parent_run_id,
      stage_name,
      trigger_type,
      status,
      root_source_id,
      input_payload,
      started_at,
      created_at,
      updated_at
    )
    VALUES (
      ${params.parentRunId || null}::uuid,
      ${params.stageName},
      ${params.triggerType},
      'running',
      ${params.rootSourceId || null}::uuid,
      ${JSON.stringify(params.inputPayload || {})}::jsonb,
      NOW(),
      NOW(),
      NOW()
    )
    RETURNING
      id,
      root_source_id as "rootSourceId",
      parent_run_id as "parentRunId"
  `;
  return rows[0] as PipelineRunRow;
}

async function finalizePipelineRun(params: {
  runId: string;
  status: PipelineRunStatus;
  crawlArtifactId?: string | null;
  outputPayload?: Record<string, unknown>;
  errorPayload?: Record<string, unknown>;
  metrics?: Record<string, unknown>;
}) {
  const sql = getSql();
  await sql`
    UPDATE dictionary.dictionary_pipeline_runs
    SET
      status = ${params.status},
      crawl_artifact_id = COALESCE(${params.crawlArtifactId || null}::uuid, crawl_artifact_id),
      output_payload = ${JSON.stringify(params.outputPayload || {})}::jsonb,
      error_payload = ${JSON.stringify(params.errorPayload || {})}::jsonb,
      metrics = ${JSON.stringify(params.metrics || {})}::jsonb,
      ended_at = NOW(),
      updated_at = NOW()
    WHERE id = ${params.runId}::uuid
  `;
}

function deriveFailureSeverity(
  failureCount: number,
  failureThreshold: number,
  trustTier: RootSourceRow["trustTier"]
): ReviewQueueSeverity {
  if (failureCount >= failureThreshold) {
    return trustTier === "authoritative" ? "critical" : "high";
  }
  if (failureCount >= Math.max(2, failureThreshold - 1)) return "medium";
  return "low";
}

function buildSuggestedAction(
  itemType: ReviewQueueItemType,
  severity: ReviewQueueSeverity,
  rootSource: RootSourceRow
): string {
  if (itemType === "artifact_parse_failure") {
    return severity === "critical"
      ? `Inspect parser behavior for ${rootSource.rootUrl} and update capture handling before the next crawl.`
      : `Inspect parse output for ${rootSource.rootUrl} and verify the page still yields usable substrate text.`;
  }

  return severity === "critical"
    ? `Inspect access or availability for ${rootSource.rootUrl}; repeated failures exceeded threshold for this root source.`
    : `Retry crawl for ${rootSource.rootUrl} and verify remote availability or access controls.`;
}

async function recordReviewQueueFailure(params: {
  rootSource: RootSourceRow;
  pipelineRunId: string;
  itemType: ReviewQueueItemType;
  lastError: string;
  crawlArtifactId?: string | null;
}) {
  const sql = getSql();
  const nowIso = new Date().toISOString();
  const existingRows = await sql`
    SELECT id, retry_count as "retryCount"
    FROM dictionary.dictionary_review_queue
    WHERE item_type = ${params.itemType}
      AND root_source_id = ${params.rootSource.id}::uuid
      AND affected_record_type = 'root_source'
      AND affected_record_id = ${params.rootSource.id}::uuid
      AND resolved_at IS NULL
    ORDER BY last_failed_at DESC, created_at DESC
    LIMIT 1
  `;

  const existing = existingRows[0] as { id: string; retryCount: number } | undefined;
  const failureCount = existing ? Number(existing.retryCount || 0) + 1 : 1;
  const severity = deriveFailureSeverity(
    failureCount,
    params.rootSource.failureThreshold,
    params.rootSource.trustTier
  );
  const suggestedAction = buildSuggestedAction(params.itemType, severity, params.rootSource);

  if (existing?.id) {
    await sql`
      UPDATE dictionary.dictionary_review_queue
      SET
        severity = ${severity},
        crawl_artifact_id = COALESCE(${params.crawlArtifactId || null}::uuid, crawl_artifact_id),
        pipeline_run_id = ${params.pipelineRunId}::uuid,
        retry_count = ${failureCount},
        last_error = ${params.lastError},
        suggested_action = ${suggestedAction},
        last_failed_at = ${nowIso}::timestamptz,
        updated_at = NOW()
      WHERE id = ${existing.id}::uuid
    `;
    return { reviewQueueId: existing.id, severity, failureCount };
  }

  const inserted = await sql`
    INSERT INTO dictionary.dictionary_review_queue (
      item_type,
      severity,
      root_source_id,
      crawl_artifact_id,
      pipeline_run_id,
      affected_record_type,
      affected_record_id,
      retry_count,
      last_error,
      suggested_action,
      first_failed_at,
      last_failed_at,
      created_at,
      updated_at
    )
    VALUES (
      ${params.itemType},
      ${severity},
      ${params.rootSource.id}::uuid,
      ${params.crawlArtifactId || null}::uuid,
      ${params.pipelineRunId}::uuid,
      'root_source',
      ${params.rootSource.id}::uuid,
      ${failureCount},
      ${params.lastError},
      ${suggestedAction},
      ${nowIso}::timestamptz,
      ${nowIso}::timestamptz,
      NOW(),
      NOW()
    )
    RETURNING id
  `;

  return {
    reviewQueueId: cleanText(inserted[0]?.id || "", 80),
    severity,
    failureCount
  };
}

async function resolveOpenReviewQueueFailures(rootSourceId: string, pipelineRunId: string) {
  const sql = getSql();
  await sql`
    UPDATE dictionary.dictionary_review_queue
    SET
      resolved_at = NOW(),
      pipeline_run_id = ${pipelineRunId}::uuid,
      updated_at = NOW()
    WHERE root_source_id = ${rootSourceId}::uuid
      AND item_type IN ('fetch_failure', 'artifact_parse_failure')
      AND resolved_at IS NULL
  `;
}

function buildPhaseCReviewSuggestedAction(rootSource: RootSourceRow): string {
  return rootSource.trustTier === "authoritative"
    ? `Inspect Phase C extraction output for ${rootSource.rootUrl}; the artifact was handoff-ready but did not satisfy the structured extraction contract.`
    : `Inspect Phase C extraction output for ${rootSource.rootUrl} and confirm the source still yields usable structured civic data.`;
}

async function recordPhaseCExtractionContractFailure(params: {
  rootSource: RootSourceRow;
  artifactId: string;
  pipelineRunId: string;
  lastError: string;
}) {
  const sql = getSql();
  const nowIso = new Date().toISOString();
  const existingRows = await sql`
    SELECT id, retry_count as "retryCount"
    FROM dictionary.dictionary_review_queue
    WHERE item_type = 'extraction_contract_failure'
      AND root_source_id = ${params.rootSource.id}::uuid
      AND affected_record_type = 'root_source'
      AND affected_record_id = ${params.rootSource.id}::uuid
      AND resolved_at IS NULL
    ORDER BY last_failed_at DESC, created_at DESC
    LIMIT 1
  `;

  const existing = existingRows[0] as { id: string; retryCount: number } | undefined;
  const failureCount = existing ? Number(existing.retryCount || 0) + 1 : 1;
  const severity = deriveFailureSeverity(
    failureCount,
    params.rootSource.failureThreshold,
    params.rootSource.trustTier
  );
  const suggestedAction = buildPhaseCReviewSuggestedAction(params.rootSource);

  if (existing?.id) {
    await sql`
      UPDATE dictionary.dictionary_review_queue
      SET
        severity = ${severity},
        crawl_artifact_id = ${params.artifactId}::uuid,
        pipeline_run_id = ${params.pipelineRunId}::uuid,
        retry_count = ${failureCount},
        last_error = ${params.lastError},
        suggested_action = ${suggestedAction},
        last_failed_at = ${nowIso}::timestamptz,
        updated_at = NOW()
      WHERE id = ${existing.id}::uuid
    `;
    return { reviewQueueId: existing.id, severity, failureCount };
  }

  const inserted = await sql`
    INSERT INTO dictionary.dictionary_review_queue (
      item_type,
      severity,
      root_source_id,
      crawl_artifact_id,
      pipeline_run_id,
      affected_record_type,
      affected_record_id,
      retry_count,
      last_error,
      suggested_action,
      first_failed_at,
      last_failed_at,
      created_at,
      updated_at
    )
    VALUES (
      'extraction_contract_failure',
      ${severity},
      ${params.rootSource.id}::uuid,
      ${params.artifactId}::uuid,
      ${params.pipelineRunId}::uuid,
      'root_source',
      ${params.rootSource.id}::uuid,
      ${failureCount},
      ${params.lastError},
      ${suggestedAction},
      ${nowIso}::timestamptz,
      ${nowIso}::timestamptz,
      NOW(),
      NOW()
    )
    RETURNING id
  `;

  return {
    reviewQueueId: cleanText(inserted[0]?.id || "", 80),
    severity,
    failureCount
  };
}

async function resolveOpenPhaseCExtractionFailures(rootSourceId: string, pipelineRunId: string) {
  const sql = getSql();
  await sql`
    UPDATE dictionary.dictionary_review_queue
    SET
      resolved_at = NOW(),
      pipeline_run_id = ${pipelineRunId}::uuid,
      updated_at = NOW()
    WHERE root_source_id = ${rootSourceId}::uuid
      AND item_type = 'extraction_contract_failure'
      AND resolved_at IS NULL
  `;
}

async function persistCrawlArtifact(rootSource: RootSourceRow, runId: string, artifact: FetchArtifactResult) {
  const sql = getSql();
  const rows = await sql`
    INSERT INTO dictionary.dictionary_crawl_artifacts (
      root_source_id,
      substrate_run_id,
      prior_artifact_id,
      source_url,
      source_domain,
      content_hash,
      fetched_at,
      http_status,
      content_type,
      raw_html,
      extracted_text,
      metadata,
      created_at
    )
    VALUES (
      ${rootSource.id}::uuid,
      ${runId}::uuid,
      ${artifact.priorArtifactId || null}::uuid,
      ${artifact.sourceUrl},
      ${artifact.sourceDomain},
      ${artifact.contentHash},
      ${artifact.fetchedAt}::timestamptz,
      ${artifact.httpStatus},
      ${artifact.contentType},
      ${artifact.rawHtml},
      ${artifact.extractedText},
      ${JSON.stringify(artifact.metadata)}::jsonb,
      NOW()
    )
    RETURNING id
  `;

  await sql`
    UPDATE dictionary.dictionary_root_sources
    SET
      last_crawled_at = ${artifact.fetchedAt}::timestamptz,
      updated_at = NOW()
    WHERE id = ${rootSource.id}::uuid
  `;

  return cleanText(rows[0]?.id || "", 80);
}

async function fetchAndBuildArtifact(rootSource: RootSourceRow, trigger: string, parentRunId: string | null): Promise<FetchArtifactResult> {
  const startedAt = Date.now();
  const response = await fetch(rootSource.rootUrl, {
    method: "GET",
    headers: {
      "User-Agent": "DaytonEnquirerDictionarySubstrateBot/1.0 (+https://thedaytonenquirer.com)",
      Accept: "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8"
    }
  });

  const body = await response.text();
  if (!response.ok) {
    throw createIngestionFailure(
      "fetch_failure",
      `Fetch failed for ${rootSource.rootUrl} with status ${response.status}`,
      {
        rootUrl: rootSource.rootUrl,
        httpStatus: response.status
      }
    );
  }
  const contentType = cleanText(response.headers.get("content-type") || "", 255) || null;
  const normalized = buildNormalizedArtifact(body, contentType);
  const priorArtifactId = rootSource.latestArtifactId || null;
  const previousHash = cleanText(rootSource.latestArtifactContentHash || "", 128) || null;
  const changeState: "initial" | "unchanged" | "changed" =
    !priorArtifactId ? "initial" : previousHash === normalized.contentHash ? "unchanged" : "changed";
  const fetchedAt = new Date().toISOString();

  return {
    sourceUrl: rootSource.rootUrl,
    sourceDomain: rootSource.sourceDomain,
    fetchedAt,
    httpStatus: response.status,
    contentType,
    rawHtml: normalized.rawHtml,
    extractedText: normalized.extractedText,
    contentHash: normalized.contentHash,
    changeState,
    priorArtifactId,
    metadata: {
      change_state: changeState,
      hash_algorithm: normalized.hashAlgorithm,
      fetch: {
        requested_url: rootSource.rootUrl,
        final_url: cleanText(response.url || rootSource.rootUrl, 2000),
        http_status: response.status,
        ok: response.ok,
        duration_ms: Date.now() - startedAt,
        content_length_bytes: Buffer.byteLength(body, "utf8")
      },
      parser: {
        content_type: contentType,
        extracted_text_length: normalized.extractedText ? normalized.extractedText.length : 0,
        raw_html_length: normalized.rawHtml ? normalized.rawHtml.length : 0
      },
      source_policy: {
        trust_tier: rootSource.trustTier,
        crawl_cadence_days: rootSource.crawlCadenceDays,
        freshness_sla_days: rootSource.freshnessSlaDays,
        failure_threshold: rootSource.failureThreshold,
        supported_entity_classes: rootSource.supportedEntityClasses
      },
      handoff: {
        extraction_ready: response.ok && Boolean(normalized.extractedText),
        trigger,
        parent_run_id: parentRunId
      }
    }
  };
}

async function runRootSourceIngestion(input: {
  rootSourceId: string;
  trigger: DispatchTrigger;
  parentRunId?: string | null;
}) {
  const rootSource = await loadRootSourceById(input.rootSourceId);
  if (!rootSource) {
    throw new Error(`Unknown root source ${input.rootSourceId}`);
  }
  if (!rootSource.enabled) {
    throw new Error(`Root source ${input.rootSourceId} is disabled`);
  }

  const run = await createPipelineRun({
    parentRunId: input.parentRunId || null,
    rootSourceId: rootSource.id,
    stageName: "phase_b_root_ingestion",
    triggerType: input.trigger,
    inputPayload: {
      rootSourceId: rootSource.id,
      rootUrl: rootSource.rootUrl,
      trigger: input.trigger
    }
  });

  try {
    const artifact = await fetchAndBuildArtifact(rootSource, input.trigger, input.parentRunId || null);
    const artifactId = await persistCrawlArtifact(rootSource, run.id, artifact);
    if (!isArtifactHandoffReady(artifact)) {
      const parseFailure = createIngestionFailure(
        "artifact_parse_failure",
        `Artifact capture for ${rootSource.rootUrl} did not yield handoff-ready text`,
        {
          rootUrl: rootSource.rootUrl,
          fetchedAt: artifact.fetchedAt,
          contentType: artifact.contentType,
          changeState: artifact.changeState,
          priorArtifactId: artifact.priorArtifactId
        }
      );
      parseFailure.crawlArtifactId = artifactId;
      throw parseFailure;
    }

    await resolveOpenReviewQueueFailures(rootSource.id, run.id);
    await finalizePipelineRun({
      runId: run.id,
      status: "succeeded",
      crawlArtifactId: artifactId,
      outputPayload: buildHandoffOutputPayload({
        artifactId,
        rootSourceId: rootSource.id,
        fetchedAt: artifact.fetchedAt,
        changeState: artifact.changeState,
        handoffReady: isArtifactHandoffReady(artifact),
        priorArtifactId: artifact.priorArtifactId
      }),
      metrics: {
        httpStatus: artifact.httpStatus,
        contentHash: artifact.contentHash
      }
    });
    return {
      ok: true,
      runId: run.id,
      rootSourceId: rootSource.id,
      artifactId,
      changeState: artifact.changeState,
      handoffReady: true,
      nextStage: PHASE_C_STAGE_NAME,
      phaseCEventEmitted: false
    };
  } catch (error: any) {
    const itemType: ReviewQueueItemType =
      error?.itemType === "artifact_parse_failure" ? "artifact_parse_failure" : "fetch_failure";
    const reviewState = await recordReviewQueueFailure({
      rootSource,
      pipelineRunId: run.id,
      itemType,
      lastError: cleanText(error?.message || "root_source_ingestion_failed", 2000),
      crawlArtifactId: cleanText(error?.crawlArtifactId || "", 80) || null
    });
    const isParseFailure = itemType === "artifact_parse_failure";

    await finalizePipelineRun({
      runId: run.id,
      status: isParseFailure ? "succeeded" : "failed",
      crawlArtifactId: cleanText(error?.crawlArtifactId || "", 80) || null,
      outputPayload: isParseFailure
        ? buildHandoffOutputPayload({
            artifactId: cleanText(error?.crawlArtifactId || "", 80) || null,
            rootSourceId: rootSource.id,
            fetchedAt: cleanText(error?.details?.fetchedAt || "", 80) || null,
            changeState: (error?.details?.changeState as FetchArtifactResult["changeState"] | null) || null,
            handoffReady: false,
            priorArtifactId: cleanText(error?.details?.priorArtifactId || "", 80) || null,
            reviewQueueId: reviewState.reviewQueueId,
            reviewSeverity: reviewState.severity,
            failureCount: reviewState.failureCount
          })
        : {},
      errorPayload: {
        message: cleanText(error?.message || "root_source_ingestion_failed", 2000),
        rootSourceId: rootSource.id,
        itemType,
        reviewQueueId: reviewState.reviewQueueId,
        reviewSeverity: reviewState.severity,
        failureCount: reviewState.failureCount
      }
    });
    if (isParseFailure) {
      return {
        ok: true,
        runId: run.id,
        rootSourceId: rootSource.id,
        artifactId: cleanText(error?.crawlArtifactId || "", 80) || null,
        fetchedAt: cleanText(error?.details?.fetchedAt || "", 80) || null,
        changeState: (error?.details?.changeState as FetchArtifactResult["changeState"] | null) || null,
        handoffReady: false,
        nextStage: null,
        phaseCEventEmitted: false,
        reviewQueueId: reviewState.reviewQueueId,
        reviewSeverity: reviewState.severity,
        failureCount: reviewState.failureCount
      };
    }
    throw error;
  }
}

export function createDictionarySubstrateDispatchSchedulerFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-dispatch-scheduler", concurrency: { limit: 1 } },
    { cron: "0 14 * * 1" },
    async ({ step }: any) => {
      await step.sendEvent("emit-dictionary-substrate-scheduled-dispatch", {
        name: "dictionary.substrate.ingestion.dispatch",
        data: {
          trigger: "scheduled",
          limit: 25
        }
      });
      return {
        ok: true,
        trigger: "scheduled",
        scheduledAt: new Date().toISOString()
      };
    }
  );
}

export function createDictionarySubstrateDispatchFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-dispatch" },
    { event: "dictionary.substrate.ingestion.dispatch" },
    async ({ event, step }: any) => {
      const trigger = cleanText(event?.data?.trigger || "manual", 20) === "scheduled" ? "scheduled" : "manual";
      const limit = parsePositiveInt(event?.data?.limit, 25, 1, 200);
      const rootSourceId = cleanText(event?.data?.rootSourceId || "", 80) || null;
      if (rootSourceId && !UUID_RE.test(rootSourceId)) {
        throw new Error("Invalid rootSourceId");
      }

      const dispatchRun = await step.run("create-dispatch-run", async () =>
        createPipelineRun({
          stageName: "phase_b_ingestion_dispatch",
          triggerType: trigger,
          inputPayload: {
            rootSourceId,
            limit,
            trigger
          }
        })
      );

      try {
        const rootSources = await step.run("select-root-sources", async () => {
          if (rootSourceId) {
            const row = await loadRootSourceById(rootSourceId);
            if (!row) {
              throw new Error(`Unknown root source ${rootSourceId}`);
            }
            return [row];
          }
          return loadDueRootSources(limit);
        });

        for (const rootSource of rootSources) {
          await step.sendEvent(`emit-root-ingestion-${rootSource.id}`, {
            name: "dictionary.substrate.ingestion.root",
            data: {
              rootSourceId: rootSource.id,
              trigger,
              parentRunId: dispatchRun.id
            }
          });
        }

        await step.run("finalize-dispatch-run", async () =>
          finalizePipelineRun({
            runId: dispatchRun.id,
            status: "succeeded",
            outputPayload: {
              trigger,
              rootSourceId,
              selectedRootSourceCount: rootSources.length,
              selectedRootSourceIds: rootSources.map((row) => row.id)
            }
          })
        );

        return {
          ok: true,
          trigger,
          dispatchRunId: dispatchRun.id,
          selectedRootSourceCount: rootSources.length,
          selectedRootSourceIds: rootSources.map((row) => row.id)
        };
      } catch (error: any) {
        await step.run("finalize-dispatch-run-failed", async () =>
          finalizePipelineRun({
            runId: dispatchRun.id,
            status: "failed",
            errorPayload: {
              message: cleanText(error?.message || "dictionary_dispatch_failed", 2000),
              trigger,
              rootSourceId
            }
          })
        );
        throw error;
      }
    }
  );
}

export function createDictionarySubstrateRootIngestionFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-root-ingestion" },
    { event: "dictionary.substrate.ingestion.root" },
    async ({ event, step }: any) => {
      const rootSourceId = cleanText(event?.data?.rootSourceId || "", 80);
      if (!rootSourceId) throw new Error("Missing rootSourceId");
      if (!UUID_RE.test(rootSourceId)) throw new Error("Invalid rootSourceId");

      const trigger = cleanText(event?.data?.trigger || "manual", 20) === "scheduled" ? "scheduled" : "manual";
      const parentRunId = cleanText(event?.data?.parentRunId || "", 80) || null;

      const result = await step.run("root-source-ingestion", async () =>
        runRootSourceIngestion({
          rootSourceId,
          trigger,
          parentRunId
        })
      );

      let phaseCEventEmitted = false;
      if (result?.handoffReady && result?.artifactId) {
        try {
          await step.sendEvent(`emit-phase-c-extraction-${result.artifactId}`, {
            name: "dictionary.substrate.extraction.artifact",
            data: {
              rootSourceId,
              artifactId: result.artifactId,
              trigger,
              parentRunId: result.runId
            }
          });
          phaseCEventEmitted = true;
          await step.run("mark-phase-c-event-emitted", async () =>
            markPhaseCEventEmission({
              runId: result.runId,
              phaseCDispatchAttempted: true,
              phaseCEventEmitted: true,
              phaseCDispatchEventName: "dictionary.substrate.extraction.artifact",
              phaseCDispatchArtifactId: result.artifactId,
              phaseCDispatchError: null
            })
          );
        } catch (error: any) {
          await step.run("mark-phase-c-event-failed", async () =>
            markPhaseCEventEmission({
              runId: result.runId,
              phaseCDispatchAttempted: true,
              phaseCEventEmitted: false,
              phaseCDispatchEventName: "dictionary.substrate.extraction.artifact",
              phaseCDispatchArtifactId: result.artifactId,
              phaseCDispatchError: cleanText(
                error?.message || "phase_c_dispatch_emit_failed",
                2000
              )
            })
          );
          throw error;
        }
      }

      return {
        ok: true,
        ...result,
        phaseCEventEmitted
      };
    }
  );
}

export function createDictionarySubstrateExtractionArtifactFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-extraction-artifact" },
    { event: "dictionary.substrate.extraction.artifact" },
    async ({ event, step }: any) => {
      const rootSourceId = cleanText(event?.data?.rootSourceId || "", 80);
      const artifactId = cleanText(event?.data?.artifactId || "", 80);
      const trigger = cleanText(event?.data?.trigger || "manual", 20) === "scheduled" ? "scheduled" : "manual";
      const parentRunId = cleanText(event?.data?.parentRunId || "", 80) || null;

      if (!rootSourceId) throw new Error("Missing rootSourceId");
      if (!artifactId) throw new Error("Missing artifactId");
      if (!UUID_RE.test(rootSourceId)) throw new Error("Invalid rootSourceId");
      if (!UUID_RE.test(artifactId)) throw new Error("Invalid artifactId");
      if (parentRunId && !UUID_RE.test(parentRunId)) throw new Error("Invalid parentRunId");

      const phaseCRun = await step.run("create-phase-c-run", async () =>
        createPipelineRun({
          parentRunId,
          rootSourceId,
          stageName: PHASE_C_STAGE_NAME,
          triggerType: trigger,
          inputPayload: {
            rootSourceId,
            artifactId,
            trigger,
            phaseBoundary: "phase_b_ingestion_complete",
            handoffContract: "phase_c_event_stub"
          }
        })
      );

      let extractionAttempted = false;
      try {
        const rootSource = await step.run("load-phase-c-root-source", async () => {
          const row = await loadRootSourceById(rootSourceId);
          if (!row) throw new Error(`Unknown root source ${rootSourceId}`);
          if (!row.enabled) {
            throw new Error(`Root source ${rootSourceId} is disabled`);
          }
          return row;
        });

        const artifact = await step.run("load-phase-c-artifact", async () => {
          const row = await loadExtractionArtifactForPhaseC(rootSourceId, artifactId);
          if (!row) throw new Error(`Unknown extraction artifact ${artifactId}`);
          return row;
        });

        extractionAttempted = true;
        const extraction = await step.run("run-phase-c-extraction", async () =>
          runPhaseCExtractionExecution({
            rootSource,
            artifact
          })
        );
        const persistence = await step.run("persist-phase-c-candidates", async () =>
          persistPhaseCExtractionCandidates({
            artifact,
            candidates: extraction.normalizedCandidates,
            extractionVersion: extraction.extractionVersion
          })
        );

        const needsReview = shouldEscalateZeroOutputToReview({
          rootSource,
          extraction
        });
        let reviewState: { reviewQueueId?: string; severity?: string; failureCount?: number } | null = null;
        if (needsReview) {
          reviewState = await step.run("record-phase-c-contract-review", async () =>
            recordPhaseCExtractionContractFailure({
              rootSource,
              artifactId,
              pipelineRunId: phaseCRun.id,
              lastError: "phase_c_zero_contract_valid_output"
            })
          );
        } else {
          await step.run("resolve-phase-c-contract-review", async () =>
            resolveOpenPhaseCExtractionFailures(rootSource.id, phaseCRun.id)
          );
        }

        await step.run("finalize-phase-c-run", async () =>
          finalizePipelineRun({
            runId: phaseCRun.id,
            status: needsReview ? "needs_review" : "succeeded",
            crawlArtifactId: artifactId,
            outputPayload: {
              rootSourceId,
              artifactId,
              trigger,
              phaseBoundary: needsReview ? "phase_c_extraction_needs_review" : "phase_c_extraction_executed",
              extractionAttempted: true,
              extractionCompleted: true,
              extractionVersion: extraction.extractionVersion,
              provider: extraction.provider,
              model: extraction.model,
              zeroExtractedCandidates: extraction.metrics.extractedCount === 0,
              benignZeroOutput: extraction.metrics.extractedCount === 0 && !needsReview,
              needsReview,
              reviewQueueId: reviewState?.reviewQueueId || null,
              reviewSeverity: reviewState?.severity || null,
              reviewFailureCount: reviewState?.failureCount ?? null,
              counts: {
                extractedCount: extraction.metrics.extractedCount,
                rejectedCount: extraction.metrics.rejectedCount,
                diagnosticCount: extraction.metrics.diagnosticCount,
                persistedCount: persistence.persistedCount,
                persistedExtractedCount: persistence.extractedCount,
                persistedRejectedCount: persistence.rejectedCount,
                persistedDiagnosticCount: persistence.diagnosticCount
              },
              candidateTypeCounts: extraction.normalizedCandidates.reduce((acc: Record<string, number>, item) => {
                const key = `${item.status}:${item.candidateType}`;
                acc[key] = (acc[key] || 0) + 1;
                return acc;
              }, {}),
              sampleCandidateKeys: extraction.normalizedCandidates.slice(0, 10).map((item) => item.candidateKey),
              persistedCandidateIds: persistence.persistedRows.slice(0, 20).map((row: any) => row.id),
              candidatePersistenceSubstrateRunId: artifact.substrateRunId
            }
          })
        );

        return {
          ok: true,
          rootSourceId,
          artifactId,
          trigger,
          runId: phaseCRun.id,
          parentRunId,
          needsReview,
          reviewQueueId: reviewState?.reviewQueueId || null,
          extractionVersion: extraction.extractionVersion,
          provider: extraction.provider,
          model: extraction.model,
          counts: {
            extractedCount: extraction.metrics.extractedCount,
            rejectedCount: extraction.metrics.rejectedCount,
            diagnosticCount: extraction.metrics.diagnosticCount,
            persistedCount: persistence.persistedCount,
            persistedExtractedCount: persistence.extractedCount,
            persistedRejectedCount: persistence.rejectedCount,
            persistedDiagnosticCount: persistence.diagnosticCount
          }
        };
      } catch (error: any) {
        const isContractFailure = error?.failureKind === "contract_failure";
        let reviewState: { reviewQueueId?: string; severity?: string; failureCount?: number } | null = null;
        let reviewQueueWriteFailed = false;
        let reviewQueueWriteError: string | null = null;
        if (isContractFailure) {
          try {
            reviewState = await step.run("record-phase-c-contract-review-failed", async () => {
              const rootSource = await loadRootSourceById(rootSourceId);
              if (!rootSource) {
                throw new Error(`Unknown root source ${rootSourceId}`);
              }
              return recordPhaseCExtractionContractFailure({
                rootSource,
                artifactId,
                pipelineRunId: phaseCRun.id,
                lastError: cleanText(error?.message || "phase_c_extraction_contract_failure", 2000)
              });
            });
          } catch (reviewError: any) {
            reviewState = null;
            reviewQueueWriteFailed = true;
            reviewQueueWriteError = cleanText(
              reviewError?.message || "phase_c_review_queue_write_failed",
              2000
            );
          }
        }
        await step.run("finalize-phase-c-run-failed", async () =>
          finalizePipelineRun({
            runId: phaseCRun.id,
            status: isContractFailure ? "needs_review" : "failed",
            crawlArtifactId: artifactId,
            errorPayload: {
              rootSourceId,
              artifactId,
              trigger,
              phaseBoundary: isContractFailure ? "phase_c_extraction_contract_failure" : "phase_c_extraction_failed",
              extractionAttempted,
              extractionCompleted: false,
              needsReview: isContractFailure,
              reviewQueueWriteFailed,
              reviewQueueWriteError,
              reviewQueueId: reviewState?.reviewQueueId || null,
              reviewSeverity: reviewState?.severity || null,
              reviewFailureCount: reviewState?.failureCount ?? null,
              message: cleanText(error?.message || "phase_c_extraction_failed", 2000)
            }
          })
        );
        throw error;
      }
    }
  );
}
