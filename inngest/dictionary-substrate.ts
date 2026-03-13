import { createHash } from "node:crypto";
import { neon } from "@neondatabase/serverless";
import type { Inngest } from "inngest";
import {
  PHASE_C_EXTRACTION_VERSION,
  deriveCandidateKey,
  normalizeCandidatePayload,
  type CandidateType,
  type ExtractionCandidatePayloadByType
} from "./dictionary-substrate-phase-c-contract";

type DispatchTrigger = "scheduled" | "manual";
type PipelineRunStatus = "running" | "succeeded" | "failed" | "needs_review";
type ReviewQueueItemType =
  | "fetch_failure"
  | "artifact_parse_failure"
  | "extraction_contract_failure"
  | "merge_ambiguity"
  | "validation_failure"
  | "freshness_overdue"
  | "expired_high_impact_assertion";
type ReviewQueueSeverity = "low" | "medium" | "high" | "critical";
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const PHASE_C_STAGE_NAME = "phase_c_extraction_candidates";
const PHASE_C_EXTRACTION_EVENT = "dictionary.substrate.extraction.artifact";
const PHASE_D_STAGE_NAME = "phase_d_merge_proposals";
const PHASE_D_MERGE_EVENT = "dictionary.substrate.merge.artifact";
const PHASE_E_STAGE_NAME = "phase_e_promotion_snapshot_publish";
const PHASE_E_PROMOTION_EVENT = "dictionary.substrate.promote.artifact";
const PHASE_F_STAGE_NAME = "phase_f_freshness_review";
const PHASE_F_SCAN_EVENT = "dictionary.substrate.freshness.scan";
const PHASE_F_REFRESH_LIMIT_DEFAULT = 10;
const PHASE_F_REFRESH_COOLDOWN_HOURS_DEFAULT = 24;

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

type PhaseDCandidateRow = {
  id: string;
  substrateRunId: string;
  candidateType: CandidateType;
  status: "pending" | "extracted" | "rejected" | "needs_review" | "failed";
  candidateKey: string;
  candidatePayload: Record<string, unknown>;
  extractionVersion: string;
  rejectionReason: string | null;
};

type MergeProposalType =
  | "create_entity"
  | "add_alias"
  | "create_role"
  | "create_assertion"
  | "create_jurisdiction"
  | "supersede_assertion"
  | "retire_alias"
  | "merge_duplicate";

type CanonicalEntityRow = {
  id: string;
  canonicalName: string;
  entityType: string;
  primaryJurisdictionId: string | null;
};

type CanonicalAliasRow = {
  id: string;
  alias: string;
  aliasType: string;
  entityId: string;
  entityCanonicalName: string;
  entityType: string;
};

type CanonicalRoleRow = {
  id: string;
  roleName: string;
  roleType: string;
  jurisdictionId: string | null;
};

type CanonicalJurisdictionRow = {
  id: string;
  name: string;
  jurisdictionType: string;
  parentJurisdictionId: string | null;
};

type CanonicalAssertionRow = {
  id: string;
  assertionType: string;
  subjectEntityId: string;
  objectEntityId: string | null;
  roleId: string | null;
  effectiveStartAt: string | null;
  effectiveEndAt: string | null;
  termEndAt: string | null;
  validityStatus: string;
};

type EntityResolution = {
  strategy: string;
  matches: CanonicalEntityRow[];
  matchedAliasId: string | null;
};

type JurisdictionResolution = {
  strategy: string;
  matches: CanonicalJurisdictionRow[];
};

type PhaseDPreparedProposal = {
  extractionCandidateId: string;
  proposalKey: string;
  proposalType: MergeProposalType;
  targetRecordType: "entity" | "alias" | "role" | "assertion" | "jurisdiction" | null;
  targetRecordId: string | null;
  proposalConfidence: number;
  rationale: string;
  proposalPayload: Record<string, unknown>;
};

type ValidationOutcome = "approved" | "rejected" | "needs_review" | "retryable_failure";

type PhaseDPreparedValidation = {
  mergeProposalId: string;
  outcome: ValidationOutcome;
  validatorName: string;
  details: Record<string, unknown>;
};

type ActiveSnapshotMetadataRow = {
  id: string;
  version: number;
  status: string;
  substrateRunId: string;
  entityCount: number;
  assertionCount: number;
  aliasCount: number;
  changeSummary: Record<string, unknown>;
  createdAt: string;
  publishedAt: string;
  activatedAt: string;
};

type PhaseEPromotableProposalRow = {
  mergeProposalId: string;
  substrateRunId: string;
  validationSubstrateRunId: string;
  phaseDPipelineRunId: string | null;
  extractionCandidateId: string;
  proposalKey: string;
  proposalType: MergeProposalType;
  targetRecordType: "entity" | "alias" | "role" | "assertion" | "jurisdiction" | null;
  targetRecordId: string | null;
  proposalConfidence: number | null;
  rationale: string | null;
  proposalPayload: Record<string, unknown>;
  validationResultId: string;
  validationCreatedAt: string;
  rootSourceId: string;
  crawlArtifactId: string;
  extractionVersion: string;
  candidateType: CandidateType;
  candidatePayload: Record<string, unknown>;
};

type PhaseEPromotionExecutionResult = {
  mergeProposalId: string;
  validationResultId: string;
  proposalType: MergeProposalType;
  promotionOutcome: "promoted" | "no_op";
  createdRecordType: "entity" | "alias" | "role" | "assertion" | "jurisdiction" | null;
  createdRecordId: string | null;
  affectedRecordType: "entity" | "alias" | "role" | "assertion" | "jurisdiction" | null;
  affectedRecordId: string | null;
};

type PhaseFAssertionReviewRow = {
  assertionId: string;
  rootSourceId: string | null;
  crawlArtifactId: string | null;
  sourceUrl: string | null;
  sourceDomain: string | null;
  trustTier: RootSourceRow["trustTier"] | null;
  failureThreshold: number | null;
  blockingFailureRetryCount: number;
  blockingItemTypes: string[];
  subjectEntityId: string;
  objectEntityId: string | null;
  roleId: string | null;
  assertionType: string;
  validityStatus: string;
  reviewStatus: string;
  computedValidityStatus: string;
  computedReviewStatus: string;
  lastVerifiedAt: string | null;
  freshnessSlaDays: number | null;
  nextReviewAt: string | null;
  reviewDueAt: string | null;
  pendingRefreshAt: string | null;
  effectiveEndAt: string | null;
  termEndAt: string | null;
  supersededByAssertionId: string | null;
  latestProvenanceCapturedAt: string | null;
  isHighImpact: boolean;
  isPendingRefresh: boolean;
  isOverdue: boolean;
  isBlocked: boolean;
  expiresWithoutSuccessor: boolean;
  overdueBy: string | null;
};

type PhaseFRootSourceAttentionRow = {
  rootSourceId: string;
  sourceName: string;
  sourceType: string;
  sourceDomain: string;
  rootUrl: string;
  trustTier: RootSourceRow["trustTier"];
  crawlCadenceDays: number | null;
  freshnessSlaDays: number | null;
  failureThreshold: number;
  lastSuccessfulCrawlAt: string | null;
  daysSinceLastSuccess: number | null;
  dueByCadence: boolean;
  overdueByFreshnessSla: boolean;
  openBlockingFailureCount: number;
  blockingFailureRetryCount: number;
  blockingItemTypes: string[];
  openExtractionFailureCount: number;
  extractionFailureRetryCount: number;
  isBlocked: boolean;
  attentionReason: string;
  shouldDispatchRefresh: boolean;
};

type PhaseFDispatchCandidateRow = PhaseFRootSourceAttentionRow & {
  recentIngestionRunId: string | null;
  recentIngestionRunStatus: string | null;
  recentIngestionCreatedAt: string | null;
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

type PhaseCExtractionStepResult =
  | {
      ok: true;
      extraction: ExtractionExecutionResult;
    }
  | {
      ok: false;
      failureKind: "contract_failure" | "runtime_failure";
      message: string;
      details?: Record<string, unknown>;
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

async function markPhaseDEventEmission(params: {
  runId: string;
  phaseDDispatchAttempted?: boolean;
  phaseDEventEmitted: boolean;
  phaseDDispatchEventName?: string | null;
  phaseDDispatchArtifactId?: string | null;
  phaseDDispatchError?: string | null;
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
    phaseDDispatchAttempted: params.phaseDDispatchAttempted ?? true,
    phaseDEventEmitted: params.phaseDEventEmitted,
    phaseDDispatchEventName: params.phaseDDispatchEventName || null,
    phaseDDispatchArtifactId: params.phaseDDispatchArtifactId || null,
    phaseDDispatchError: params.phaseDDispatchError || null
  };

  await sql`
    UPDATE dictionary.dictionary_pipeline_runs
    SET
      output_payload = ${JSON.stringify(nextOutput)}::jsonb,
      updated_at = NOW()
    WHERE id = ${params.runId}::uuid
  `;
}

async function markPhaseEEventEmission(params: {
  runId: string;
  phaseEDispatchAttempted?: boolean;
  phaseEEventEmitted: boolean;
  phaseEDispatchEventName?: string | null;
  phaseEDispatchArtifactId?: string | null;
  phaseEDispatchError?: string | null;
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
    phaseEDispatchAttempted: params.phaseEDispatchAttempted ?? true,
    phaseEEventEmitted: params.phaseEEventEmitted,
    phaseEDispatchEventName: params.phaseEDispatchEventName || null,
    phaseEDispatchArtifactId: params.phaseEDispatchArtifactId || null,
    phaseEDispatchError: params.phaseEDispatchError || null
  };

  await sql`
    UPDATE dictionary.dictionary_pipeline_runs
    SET
      output_payload = ${JSON.stringify(nextOutput)}::jsonb,
      updated_at = NOW()
    WHERE id = ${params.runId}::uuid
  `;
}

async function markPhaseFRefreshDispatch(params: {
  runId: string;
  phaseFRefreshDispatchAttempted?: boolean;
  phaseFRefreshDispatchCompleted?: boolean;
  refreshDispatchLimit?: number | null;
  refreshDispatchCooldownHours?: number | null;
  refreshDispatchRequested?: boolean | null;
  refreshDispatchCandidateCount?: number | null;
  refreshDispatchSelectedCount?: number | null;
  refreshDispatchSkippedRecentCount?: number | null;
  refreshDispatchEmittedCount?: number | null;
  refreshDispatchRootSourceIds?: string[];
  refreshDispatchRecentRootSourceIds?: string[];
  refreshDispatchError?: string | null;
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
    phaseFRefreshDispatchAttempted: params.phaseFRefreshDispatchAttempted ?? true,
    phaseFRefreshDispatchCompleted: params.phaseFRefreshDispatchCompleted ?? false,
    refreshDispatchRequested: params.refreshDispatchRequested ?? false,
    refreshDispatchLimit: params.refreshDispatchLimit ?? null,
    refreshDispatchCooldownHours: params.refreshDispatchCooldownHours ?? null,
    refreshDispatchCandidateCount: params.refreshDispatchCandidateCount ?? null,
    refreshDispatchSelectedCount: params.refreshDispatchSelectedCount ?? null,
    refreshDispatchSkippedRecentCount: params.refreshDispatchSkippedRecentCount ?? null,
    refreshDispatchEmittedCount: params.refreshDispatchEmittedCount ?? null,
    refreshDispatchRootSourceIds: params.refreshDispatchRootSourceIds || [],
    refreshDispatchRecentRootSourceIds: params.refreshDispatchRecentRootSourceIds || [],
    refreshDispatchError: params.refreshDispatchError || null
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

function parseBoolean(value: unknown, fallback = false): boolean {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
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

async function runPhaseCExtractionExecutionSafely(params: {
  rootSource: RootSourceRow;
  artifact: ExtractionArtifactRow;
}): Promise<PhaseCExtractionStepResult> {
  try {
    const extraction = await runPhaseCExtractionExecution(params);
    return {
      ok: true,
      extraction
    };
  } catch (error: any) {
    return {
      ok: false,
      failureKind: error?.failureKind === "contract_failure" ? "contract_failure" : "runtime_failure",
      message: cleanText(error?.message || "phase_c_extraction_failed", 2000),
      details:
        error?.details && typeof error.details === "object" && !Array.isArray(error.details)
          ? (error.details as Record<string, unknown>)
          : {}
    };
  }
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

async function loadPhaseDCandidatesForArtifact(
  artifactId: string,
  extractionVersion: string
): Promise<PhaseDCandidateRow[]> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      ec.id,
      ec.substrate_run_id as "substrateRunId",
      ec.candidate_type as "candidateType",
      ec.status,
      ec.candidate_key as "candidateKey",
      ec.candidate_payload as "candidatePayload",
      ec.extraction_version as "extractionVersion",
      ec.rejection_reason as "rejectionReason"
    FROM dictionary.dictionary_extraction_candidates ec
    WHERE ec.crawl_artifact_id = ${artifactId}::uuid
      AND ec.extraction_version = ${extractionVersion}
    ORDER BY ec.created_at ASC, ec.id ASC
  `;
  return rows as PhaseDCandidateRow[];
}

async function loadActiveSnapshotMetadata(): Promise<ActiveSnapshotMetadataRow | null> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      id,
      version,
      status,
      substrate_run_id as "substrateRunId",
      entity_count as "entityCount",
      assertion_count as "assertionCount",
      alias_count as "aliasCount",
      change_summary as "changeSummary",
      created_at as "createdAt",
      published_at as "publishedAt",
      activated_at as "activatedAt"
    FROM dictionary.active_snapshot_metadata
    LIMIT 1
  `;
  return (rows[0] as ActiveSnapshotMetadataRow | undefined) || null;
}

async function loadPhaseEPromotableProposals(params: {
  rootSourceId: string;
  artifactId: string;
  phaseDRunId: string;
}): Promise<PhaseEPromotableProposalRow[]> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      merge_proposal_id as "mergeProposalId",
      substrate_run_id as "substrateRunId",
      validation_substrate_run_id as "validationSubstrateRunId",
      phase_d_pipeline_run_id as "phaseDPipelineRunId",
      extraction_candidate_id as "extractionCandidateId",
      proposal_key as "proposalKey",
      proposal_type as "proposalType",
      target_record_type as "targetRecordType",
      target_record_id as "targetRecordId",
      proposal_confidence as "proposalConfidence",
      rationale,
      proposal_payload as "proposalPayload",
      validation_result_id as "validationResultId",
      validation_created_at as "validationCreatedAt",
      root_source_id as "rootSourceId",
      crawl_artifact_id as "crawlArtifactId",
      extraction_version as "extractionVersion",
      candidate_type as "candidateType",
      candidate_payload as "candidatePayload"
    FROM dictionary.phase_e_promotable_merge_proposals
    WHERE root_source_id = ${params.rootSourceId}::uuid
      AND crawl_artifact_id = ${params.artifactId}::uuid
      AND phase_d_pipeline_run_id = ${params.phaseDRunId}::uuid
    ORDER BY validation_created_at ASC, merge_proposal_id ASC
  `;
  return rows as PhaseEPromotableProposalRow[];
}

async function promoteAndPublishPhaseEArtifactRun(params: {
  phaseERunId: string;
  rootSourceId: string;
  artifactId: string;
  phaseDRunId: string;
}): Promise<{
  snapshotId: string | null;
  snapshotVersion: number | null;
  promotedCount: number;
  noOpCount: number;
}> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      snapshot_id as "snapshotId",
      snapshot_version as "snapshotVersion",
      promoted_count as "promotedCount",
      no_op_count as "noOpCount"
    FROM dictionary.phase_e_promote_and_publish_artifact_run(
      ${params.phaseERunId}::uuid,
      ${params.rootSourceId}::uuid,
      ${params.artifactId}::uuid,
      ${params.phaseDRunId}::uuid
    )
  `;

  const row = rows[0] as
    | { snapshotId: string | null; snapshotVersion: number | null; promotedCount: number; noOpCount: number }
    | undefined;
  if (!row) {
    throw new Error(`Phase E publish helper returned no result for run ${params.phaseERunId}`);
  }
  return row;
}

async function loadPhaseEPromotionResultsForRun(
  phaseERunId: string
): Promise<PhaseEPromotionExecutionResult[]> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      merge_proposal_id as "mergeProposalId",
      validation_result_id as "validationResultId",
      (details->>'proposal_type')::dictionary.merge_proposal_type as "proposalType",
      promotion_outcome as "promotionOutcome",
      created_record_type as "createdRecordType",
      created_record_id as "createdRecordId",
      affected_record_type as "affectedRecordType",
      affected_record_id as "affectedRecordId"
    FROM dictionary.dictionary_promotion_results
    WHERE substrate_run_id = ${phaseERunId}::uuid
    ORDER BY created_at ASC, merge_proposal_id ASC
  `;
  return rows as PhaseEPromotionExecutionResult[];
}

async function loadPhaseFAssertionsDueForReview(
  asOf: string,
  sqlClient?: any
): Promise<PhaseFAssertionReviewRow[]> {
  const sql = sqlClient || getSql();
  const rows = await sql`
    SELECT
      assertion_id as "assertionId",
      root_source_id as "rootSourceId",
      crawl_artifact_id as "crawlArtifactId",
      source_url as "sourceUrl",
      source_domain as "sourceDomain",
      trust_tier as "trustTier",
      failure_threshold as "failureThreshold",
      blocking_failure_retry_count as "blockingFailureRetryCount",
      blocking_item_types as "blockingItemTypes",
      subject_entity_id as "subjectEntityId",
      object_entity_id as "objectEntityId",
      role_id as "roleId",
      assertion_type as "assertionType",
      validity_status as "validityStatus",
      review_status as "reviewStatus",
      computed_validity_status as "computedValidityStatus",
      computed_review_status as "computedReviewStatus",
      last_verified_at as "lastVerifiedAt",
      freshness_sla_days as "freshnessSlaDays",
      next_review_at as "nextReviewAt",
      review_due_at as "reviewDueAt",
      pending_refresh_at as "pendingRefreshAt",
      effective_end_at as "effectiveEndAt",
      term_end_at as "termEndAt",
      superseded_by_assertion_id as "supersededByAssertionId",
      latest_provenance_captured_at as "latestProvenanceCapturedAt",
      is_high_impact as "isHighImpact",
      is_pending_refresh as "isPendingRefresh",
      is_overdue as "isOverdue",
      is_blocked as "isBlocked",
      expires_without_successor as "expiresWithoutSuccessor",
      overdue_by as "overdueBy"
    FROM dictionary.phase_f_assertions_due_for_review(${asOf}::timestamptz)
  `;
  return rows as PhaseFAssertionReviewRow[];
}

async function loadPhaseFRootSourcesRequiringAttention(
  asOf: string,
  sqlClient?: any
): Promise<PhaseFRootSourceAttentionRow[]> {
  const sql = sqlClient || getSql();
  const rows = await sql`
    SELECT
      root_source_id as "rootSourceId",
      source_name as "sourceName",
      source_type as "sourceType",
      source_domain as "sourceDomain",
      root_url as "rootUrl",
      trust_tier as "trustTier",
      crawl_cadence_days as "crawlCadenceDays",
      freshness_sla_days as "freshnessSlaDays",
      failure_threshold as "failureThreshold",
      last_successful_crawl_at as "lastSuccessfulCrawlAt",
      days_since_last_success as "daysSinceLastSuccess",
      due_by_cadence as "dueByCadence",
      overdue_by_freshness_sla as "overdueByFreshnessSla",
      open_blocking_failure_count as "openBlockingFailureCount",
      blocking_failure_retry_count as "blockingFailureRetryCount",
      blocking_item_types as "blockingItemTypes",
      open_extraction_failure_count as "openExtractionFailureCount",
      extraction_failure_retry_count as "extractionFailureRetryCount",
      is_blocked as "isBlocked",
      attention_reason as "attentionReason",
      should_dispatch_refresh as "shouldDispatchRefresh"
    FROM dictionary.phase_f_root_sources_requiring_attention(${asOf}::timestamptz)
  `;
  return rows as PhaseFRootSourceAttentionRow[];
}

async function loadPhaseFRefreshDispatchCandidates(params: {
  asOf: string;
  limit: number;
  cooldownHours: number;
}): Promise<{
  candidates: PhaseFDispatchCandidateRow[];
  skippedRecentRootSourceIds: string[];
}> {
  const sql = getSql();
  const rows = await sql`
    WITH attention AS (
      SELECT *
      FROM dictionary.phase_f_root_sources_requiring_attention(${params.asOf}::timestamptz)
      WHERE should_dispatch_refresh = true
    ),
    recent_runs AS (
      SELECT DISTINCT ON (pr.root_source_id)
        pr.root_source_id,
        pr.id,
        pr.status,
        pr.created_at
      FROM dictionary.dictionary_pipeline_runs pr
      JOIN attention a
        ON a.root_source_id = pr.root_source_id
      WHERE lower(pr.stage_name) = 'phase_b_root_ingestion'
        AND pr.created_at >= ${params.asOf}::timestamptz - make_interval(hours => ${params.cooldownHours})
      ORDER BY pr.root_source_id, pr.created_at DESC, pr.id DESC
    )
    SELECT
      a.root_source_id as "rootSourceId",
      a.source_name as "sourceName",
      a.source_type as "sourceType",
      a.source_domain as "sourceDomain",
      a.root_url as "rootUrl",
      a.trust_tier as "trustTier",
      a.crawl_cadence_days as "crawlCadenceDays",
      a.freshness_sla_days as "freshnessSlaDays",
      a.failure_threshold as "failureThreshold",
      a.last_successful_crawl_at as "lastSuccessfulCrawlAt",
      a.days_since_last_success as "daysSinceLastSuccess",
      a.due_by_cadence as "dueByCadence",
      a.overdue_by_freshness_sla as "overdueByFreshnessSla",
      a.open_blocking_failure_count as "openBlockingFailureCount",
      a.blocking_failure_retry_count as "blockingFailureRetryCount",
      a.blocking_item_types as "blockingItemTypes",
      a.open_extraction_failure_count as "openExtractionFailureCount",
      a.extraction_failure_retry_count as "extractionFailureRetryCount",
      a.is_blocked as "isBlocked",
      a.attention_reason as "attentionReason",
      a.should_dispatch_refresh as "shouldDispatchRefresh",
      rr.id as "recentIngestionRunId",
      rr.status as "recentIngestionRunStatus",
      rr.created_at as "recentIngestionCreatedAt"
    FROM attention a
    LEFT JOIN recent_runs rr
      ON rr.root_source_id = a.root_source_id
    ORDER BY
      a.overdue_by_freshness_sla DESC,
      CASE a.trust_tier
        WHEN 'authoritative' THEN 0
        WHEN 'corroborative' THEN 1
        ELSE 2
      END,
      a.days_since_last_success DESC NULLS FIRST,
      a.source_name ASC
  `;

  const allRows = rows as PhaseFDispatchCandidateRow[];
  const skippedRecentRootSourceIds = allRows
    .filter((row) => Boolean(row.recentIngestionRunId))
    .map((row) => row.rootSourceId);
  const candidates = allRows
    .filter((row) => !row.recentIngestionRunId)
    .slice(0, params.limit);

  return {
    candidates,
    skippedRecentRootSourceIds
  };
}

async function findEntitiesByCanonicalName(name: string, entityType?: string | null): Promise<CanonicalEntityRow[]> {
  const sql = getSql();
  const normalizedName = cleanText(name, 240);
  const normalizedEntityType = cleanText(entityType || "", 120) || null;
  const rows = await sql`
    SELECT
      e.id,
      e.canonical_name as "canonicalName",
      e.entity_type as "entityType",
      e.primary_jurisdiction_id as "primaryJurisdictionId"
    FROM dictionary.dictionary_entities e
    WHERE lower(e.canonical_name) = lower(${normalizedName})
      AND e.status = 'active'
      AND (${normalizedEntityType}::text IS NULL OR lower(e.entity_type) = lower(${normalizedEntityType}))
    ORDER BY e.updated_at DESC, e.created_at DESC, e.id ASC
  `;
  return rows as CanonicalEntityRow[];
}

async function findEntitiesByAlias(alias: string): Promise<CanonicalAliasRow[]> {
  const sql = getSql();
  const normalizedAlias = cleanText(alias, 240);
  const rows = await sql`
    SELECT
      a.id,
      a.alias,
      a.alias_type as "aliasType",
      e.id as "entityId",
      e.canonical_name as "entityCanonicalName",
      e.entity_type as "entityType"
    FROM dictionary.dictionary_aliases a
    JOIN dictionary.dictionary_entities e
      ON e.id = a.entity_id
    WHERE lower(a.alias) = lower(${normalizedAlias})
      AND a.status = 'active'
      AND e.status = 'active'
    ORDER BY a.updated_at DESC, a.created_at DESC, a.id ASC
  `;
  return rows as CanonicalAliasRow[];
}

async function resolveEntityReference(name: string, entityType?: string | null): Promise<EntityResolution> {
  const canonicalMatches = await findEntitiesByCanonicalName(name, entityType);
  if (canonicalMatches.length) {
    return {
      strategy: canonicalMatches.length === 1 ? "exact_canonical_name" : "exact_canonical_name_ambiguous",
      matches: canonicalMatches,
      matchedAliasId: null
    };
  }

  const aliasMatches = await findEntitiesByAlias(name);
  if (!aliasMatches.length) {
    return {
      strategy: "no_entity_match",
      matches: [],
      matchedAliasId: null
    };
  }

  const normalizedEntityType = cleanText(entityType || "", 120) || null;
  const filteredAliasMatches = normalizedEntityType
    ? aliasMatches.filter((row) => cleanText(row.entityType, 120).toLowerCase() === normalizedEntityType.toLowerCase())
    : aliasMatches;

  if (!filteredAliasMatches.length) {
    return {
      strategy: "no_entity_match",
      matches: [],
      matchedAliasId: null
    };
  }

  const entityMatches = Array.from(
    new Map(
      filteredAliasMatches.map((row) => [
        row.entityId,
        {
          id: row.entityId,
          canonicalName: row.entityCanonicalName,
          entityType: row.entityType,
          primaryJurisdictionId: null
        }
      ])
    ).values()
  );

  return {
    strategy: entityMatches.length === 1 ? "exact_alias_match" : "exact_alias_match_ambiguous",
    matches: entityMatches,
    matchedAliasId:
      entityMatches.length === 1
        ? filteredAliasMatches.find((row) => row.entityId === entityMatches[0].id)?.id || null
        : null
  };
}

async function findJurisdictionsByName(name: string, jurisdictionType?: string | null): Promise<CanonicalJurisdictionRow[]> {
  const sql = getSql();
  const normalizedName = cleanText(name, 240);
  const normalizedType = cleanText(jurisdictionType || "", 120) || null;
  const rows = await sql`
    SELECT
      j.id,
      j.name,
      j.jurisdiction_type as "jurisdictionType",
      j.parent_jurisdiction_id as "parentJurisdictionId"
    FROM dictionary.dictionary_jurisdictions j
    WHERE lower(j.name) = lower(${normalizedName})
      AND j.status = 'active'
      AND (${normalizedType}::text IS NULL OR lower(j.jurisdiction_type) = lower(${normalizedType}))
    ORDER BY j.updated_at DESC, j.created_at DESC, j.id ASC
  `;
  return rows as CanonicalJurisdictionRow[];
}

async function resolveJurisdictionReference(name: string, jurisdictionType?: string | null): Promise<JurisdictionResolution> {
  const matches = await findJurisdictionsByName(name, jurisdictionType);
  return {
    strategy: !matches.length ? "no_jurisdiction_match" : matches.length === 1 ? "exact_jurisdiction_name" : "exact_jurisdiction_name_ambiguous",
    matches
  };
}

async function findRolesByName(params: {
  roleName: string;
  roleType?: string | null;
  jurisdictionId?: string | null;
}): Promise<CanonicalRoleRow[]> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      r.id,
      r.role_name as "roleName",
      r.role_type as "roleType",
      r.jurisdiction_id as "jurisdictionId"
    FROM dictionary.dictionary_roles r
    WHERE lower(r.role_name) = lower(${cleanText(params.roleName, 240)})
      AND (${cleanText(params.roleType || "", 120) || null}::text IS NULL OR lower(r.role_type) = lower(${cleanText(params.roleType || "", 120) || null}))
      AND r.status = 'active'
      AND (
        ${params.jurisdictionId || null}::uuid IS NULL
        OR r.jurisdiction_id = ${params.jurisdictionId || null}::uuid
      )
    ORDER BY r.updated_at DESC, r.created_at DESC, r.id ASC
  `;
  return rows as CanonicalRoleRow[];
}

async function findAssertionExactMatches(params: {
  assertionType: string;
  subjectEntityId: string;
  objectEntityId?: string | null;
  roleId?: string | null;
  effectiveStartAt?: string | null;
  effectiveEndAt?: string | null;
  termEndAt?: string | null;
}): Promise<CanonicalAssertionRow[]> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      a.id,
      a.assertion_type as "assertionType",
      a.subject_entity_id as "subjectEntityId",
      a.object_entity_id as "objectEntityId",
      a.role_id as "roleId",
      a.effective_start_at as "effectiveStartAt",
      a.effective_end_at as "effectiveEndAt",
      a.term_end_at as "termEndAt",
      a.validity_status as "validityStatus"
    FROM dictionary.dictionary_assertions a
    WHERE lower(a.assertion_type) = lower(${cleanText(params.assertionType, 160)})
      AND a.subject_entity_id = ${params.subjectEntityId}::uuid
      AND (
        (${params.objectEntityId || null}::uuid IS NULL AND a.object_entity_id IS NULL)
        OR a.object_entity_id = ${params.objectEntityId || null}::uuid
      )
      AND (
        (${params.roleId || null}::uuid IS NULL AND a.role_id IS NULL)
        OR a.role_id = ${params.roleId || null}::uuid
      )
      AND a.effective_start_at IS NOT DISTINCT FROM ${params.effectiveStartAt || null}::timestamptz
      AND a.effective_end_at IS NOT DISTINCT FROM ${params.effectiveEndAt || null}::timestamptz
      AND a.term_end_at IS NOT DISTINCT FROM ${params.termEndAt || null}::timestamptz
    ORDER BY a.updated_at DESC, a.created_at DESC, a.id ASC
  `;
  return rows as CanonicalAssertionRow[];
}

async function findAssertionSupersessionCandidates(params: {
  assertionType: string;
  subjectEntityId: string;
  roleId?: string | null;
}): Promise<CanonicalAssertionRow[]> {
  if (!params.roleId) return [];
  const sql = getSql();
  const rows = await sql`
    SELECT
      a.id,
      a.assertion_type as "assertionType",
      a.subject_entity_id as "subjectEntityId",
      a.object_entity_id as "objectEntityId",
      a.role_id as "roleId",
      a.effective_start_at as "effectiveStartAt",
      a.effective_end_at as "effectiveEndAt",
      a.term_end_at as "termEndAt",
      a.validity_status as "validityStatus"
    FROM dictionary.dictionary_assertions a
    WHERE lower(a.assertion_type) = lower(${cleanText(params.assertionType, 160)})
      AND a.subject_entity_id = ${params.subjectEntityId}::uuid
      AND a.role_id = ${params.roleId}::uuid
      AND a.superseded_by_assertion_id IS NULL
      AND a.validity_status IN ('current', 'scheduled', 'unknown')
    ORDER BY a.updated_at DESC, a.created_at DESC, a.id ASC
  `;
  return rows as CanonicalAssertionRow[];
}

function buildPhaseDProposalKey(params: {
  artifactId: string;
  extractionVersion: string;
  candidateKey: string;
  proposalType: MergeProposalType;
  targetRecordType: string | null;
  targetRecordId: string | null;
  matchStrategy: string;
}) {
  const seed = [
    params.artifactId,
    params.extractionVersion,
    params.candidateKey,
    params.proposalType,
    params.targetRecordType || null,
    params.targetRecordId || null,
    params.matchStrategy
  ];
  return `proposal:${createHash("sha256").update(JSON.stringify(seed)).digest("hex").slice(0, 24)}`;
}

function clampConfidence(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function toAlternativeRecord(recordType: string, row: any, matchedBy: string) {
  return {
    record_type: recordType,
    record_id: row.id || row.entityId || null,
    label: row.canonicalName || row.entityCanonicalName || row.name || row.roleName || null,
    matched_by: matchedBy,
    alias_id: row.id && recordType === "alias" ? row.id : row.matchedAliasId || null,
    entity_type: row.entityType || null,
    role_type: row.roleType || null,
    jurisdiction_type: row.jurisdictionType || null,
    jurisdiction_id: row.primaryJurisdictionId || row.jurisdictionId || row.parentJurisdictionId || null
  };
}

function buildBaseProposalPayload(params: {
  candidate: PhaseDCandidateRow;
  matchStrategy: string;
  matchedRecordType?: string | null;
  matchedRecordId?: string | null;
  matchedAliasId?: string | null;
  candidateAlternatives?: Record<string, unknown>[];
  extra?: Record<string, unknown>;
}) {
  const alternatives = (params.candidateAlternatives || []).slice(0, 10);
  return {
    candidate_key: params.candidate.candidateKey,
    candidate_type: params.candidate.candidateType,
    extraction_version: params.candidate.extractionVersion,
    match_strategy: params.matchStrategy,
    matched_record_type: params.matchedRecordType || null,
    matched_record_id: params.matchedRecordId || null,
    matched_alias_id: params.matchedAliasId || null,
    ambiguity_count: alternatives.length,
    candidate_alternatives: alternatives,
    candidate_payload: params.candidate.candidatePayload,
    ...(params.extra || {})
  };
}

function isGenericReferenceLabel(value: unknown): boolean {
  const normalized = cleanText(value, 240).toLowerCase();
  if (!normalized) return false;
  return [
    "the mayor",
    "mayor",
    "police",
    "city hall",
    "downtown"
  ].includes(normalized);
}

async function buildPhaseDProposalForCandidate(params: {
  artifactId: string;
  candidate: PhaseDCandidateRow;
}): Promise<PhaseDPreparedProposal> {
  const { artifactId, candidate } = params;

  if (candidate.candidateType === "entity") {
    const payload = normalizeCandidatePayload("entity", candidate.candidatePayload) as ExtractionCandidatePayloadByType["entity"];
    const canonicalResolution = await resolveEntityReference(payload.canonical_name, payload.entity_kind);
    if (canonicalResolution.matches.length === 1) {
      const match = canonicalResolution.matches[0];
      const matchStrategy = canonicalResolution.strategy;
      return {
        extractionCandidateId: candidate.id,
        proposalKey: buildPhaseDProposalKey({
          artifactId,
          extractionVersion: candidate.extractionVersion,
          candidateKey: candidate.candidateKey,
          proposalType: "merge_duplicate",
          targetRecordType: "entity",
          targetRecordId: match.id,
          matchStrategy
        }),
        proposalType: "merge_duplicate",
        targetRecordType: "entity",
        targetRecordId: match.id,
        proposalConfidence: clampConfidence(matchStrategy === "exact_canonical_name" ? 0.99 : 0.93),
        rationale:
          matchStrategy === "exact_canonical_name"
            ? `Entity candidate exactly matches canonical entity ${match.canonicalName}.`
            : `Entity candidate exactly matches existing alias for canonical entity ${match.canonicalName}.`,
        proposalPayload: buildBaseProposalPayload({
          candidate,
          matchStrategy,
          matchedRecordType: "entity",
          matchedRecordId: match.id,
          matchedAliasId: canonicalResolution.matchedAliasId,
          candidateAlternatives: [],
          extra: {
            resolution_basis: "entity_identity_match"
          }
        })
      };
    }

    const alternatives = canonicalResolution.matches.map((row) =>
      toAlternativeRecord("entity", row, canonicalResolution.strategy)
    );
    const proposalType: MergeProposalType = canonicalResolution.matches.length ? "merge_duplicate" : "create_entity";
    const matchStrategy = canonicalResolution.matches.length ? canonicalResolution.strategy : "no_entity_match_create";
    return {
      extractionCandidateId: candidate.id,
      proposalKey: buildPhaseDProposalKey({
        artifactId,
        extractionVersion: candidate.extractionVersion,
        candidateKey: candidate.candidateKey,
        proposalType,
        targetRecordType: null,
        targetRecordId: null,
        matchStrategy
      }),
      proposalType,
      targetRecordType: null,
      targetRecordId: null,
      proposalConfidence: clampConfidence(canonicalResolution.matches.length ? 0.52 : 0.72),
      rationale: canonicalResolution.matches.length
        ? `Entity candidate has multiple deterministic matches and requires validation review before merge selection.`
        : `Entity candidate did not match canonical names or aliases and is proposed as a new entity.`,
      proposalPayload: buildBaseProposalPayload({
        candidate,
        matchStrategy,
        matchedAliasId: canonicalResolution.matchedAliasId,
        candidateAlternatives: alternatives,
        extra: {
          resolution_basis: canonicalResolution.matches.length ? "entity_match_ambiguous" : "entity_create_no_match"
        }
      })
    };
  }

  if (candidate.candidateType === "alias") {
    const payload = normalizeCandidatePayload("alias", candidate.candidatePayload) as ExtractionCandidatePayloadByType["alias"];
    const targetResolution = await resolveEntityReference(payload.target_canonical_name);
    const aliasMatches = await findEntitiesByAlias(payload.alias_name);
    const sameEntityAlias = targetResolution.matches.length === 1
      ? aliasMatches.find((row) => row.entityId === targetResolution.matches[0].id)
      : null;

    if (targetResolution.matches.length === 1 && sameEntityAlias) {
      return {
        extractionCandidateId: candidate.id,
        proposalKey: buildPhaseDProposalKey({
          artifactId,
          extractionVersion: candidate.extractionVersion,
          candidateKey: candidate.candidateKey,
          proposalType: "merge_duplicate",
          targetRecordType: "alias",
          targetRecordId: sameEntityAlias.id,
          matchStrategy: "alias_exact_duplicate_same_entity"
        }),
        proposalType: "merge_duplicate",
        targetRecordType: "alias",
        targetRecordId: sameEntityAlias.id,
        proposalConfidence: 0.98,
        rationale: `Alias candidate exactly matches an existing alias on ${sameEntityAlias.entityCanonicalName}.`,
        proposalPayload: buildBaseProposalPayload({
          candidate,
          matchStrategy: "alias_exact_duplicate_same_entity",
          matchedRecordType: "alias",
          matchedRecordId: sameEntityAlias.id,
          candidateAlternatives: [],
          extra: {
            resolution_basis: "alias_duplicate_match",
            target_entity_id: sameEntityAlias.entityId
          }
        })
      };
    }

    const alternatives = [
      ...targetResolution.matches.map((row) => toAlternativeRecord("entity", row, targetResolution.strategy)),
      ...aliasMatches.map((row) => ({
        record_type: "alias",
        record_id: row.id,
        label: row.alias,
        matched_by: "existing_alias_name",
        alias_id: row.id,
        entity_id: row.entityId,
        entity_canonical_name: row.entityCanonicalName,
        entity_type: row.entityType
      }))
    ].slice(0, 10);

    const targetEntityId = targetResolution.matches.length === 1 ? targetResolution.matches[0].id : null;
    return {
      extractionCandidateId: candidate.id,
      proposalKey: buildPhaseDProposalKey({
        artifactId,
        extractionVersion: candidate.extractionVersion,
        candidateKey: candidate.candidateKey,
        proposalType: "add_alias",
        targetRecordType: targetEntityId ? "entity" : null,
        targetRecordId: targetEntityId,
        matchStrategy:
          targetResolution.matches.length === 1
            ? aliasMatches.length
              ? "alias_conflict_on_other_entity"
              : "target_entity_resolved_add_alias"
            : targetResolution.matches.length > 1
              ? "target_entity_ambiguous"
              : "target_entity_unresolved"
      }),
      proposalType: "add_alias",
      targetRecordType: targetEntityId ? "entity" : null,
      targetRecordId: targetEntityId,
      proposalConfidence: clampConfidence(targetEntityId && !aliasMatches.length ? 0.9 : 0.55),
      rationale:
        targetEntityId && !aliasMatches.length
          ? `Alias candidate resolves to canonical entity ${targetResolution.matches[0].canonicalName} and no duplicate alias exists.`
          : `Alias candidate requires validation because the target entity or alias ownership is ambiguous.`,
      proposalPayload: buildBaseProposalPayload({
        candidate,
        matchStrategy:
          targetResolution.matches.length === 1
            ? aliasMatches.length
              ? "alias_conflict_on_other_entity"
              : "target_entity_resolved_add_alias"
            : targetResolution.matches.length > 1
              ? "target_entity_ambiguous"
              : "target_entity_unresolved",
        matchedRecordType: targetEntityId ? "entity" : null,
        matchedRecordId: targetEntityId,
        candidateAlternatives: alternatives,
        extra: {
          resolution_basis: "alias_target_resolution",
          target_resolution_strategy: targetResolution.strategy
        }
      })
    };
  }

  if (candidate.candidateType === "role") {
    const payload = normalizeCandidatePayload("role", candidate.candidatePayload) as ExtractionCandidatePayloadByType["role"];
    const jurisdictionResolution = payload.jurisdiction_hint?.label
      ? await resolveJurisdictionReference(payload.jurisdiction_hint.label)
      : { strategy: "no_jurisdiction_hint", matches: [] as CanonicalJurisdictionRow[] };
    const jurisdictionId = jurisdictionResolution.matches.length === 1 ? jurisdictionResolution.matches[0].id : null;
    const roleMatches = await findRolesByName({
      roleName: payload.role_name,
      roleType: payload.role_kind,
      jurisdictionId
    });

    if (roleMatches.length === 1) {
      const match = roleMatches[0];
      return {
        extractionCandidateId: candidate.id,
        proposalKey: buildPhaseDProposalKey({
          artifactId,
          extractionVersion: candidate.extractionVersion,
          candidateKey: candidate.candidateKey,
          proposalType: "merge_duplicate",
          targetRecordType: "role",
          targetRecordId: match.id,
          matchStrategy: "exact_role_name_type_match"
        }),
        proposalType: "merge_duplicate",
        targetRecordType: "role",
        targetRecordId: match.id,
        proposalConfidence: 0.96,
        rationale: `Role candidate exactly matches canonical role ${match.roleName}.`,
        proposalPayload: buildBaseProposalPayload({
          candidate,
          matchStrategy: "exact_role_name_type_match",
          matchedRecordType: "role",
          matchedRecordId: match.id,
          candidateAlternatives: [],
          extra: {
            jurisdiction_resolution_strategy: jurisdictionResolution.strategy
          }
        })
      };
    }

    const alternatives = [
      ...roleMatches.map((row) => toAlternativeRecord("role", row, "role_name_type_lookup")),
      ...jurisdictionResolution.matches.map((row) => toAlternativeRecord("jurisdiction", row, jurisdictionResolution.strategy))
    ].slice(0, 10);

    return {
      extractionCandidateId: candidate.id,
      proposalKey: buildPhaseDProposalKey({
        artifactId,
        extractionVersion: candidate.extractionVersion,
        candidateKey: candidate.candidateKey,
        proposalType: "create_role",
        targetRecordType: null,
        targetRecordId: null,
        matchStrategy: roleMatches.length ? "role_match_ambiguous" : "create_role_no_match"
      }),
      proposalType: "create_role",
      targetRecordType: null,
      targetRecordId: null,
      proposalConfidence: clampConfidence(roleMatches.length ? 0.5 : 0.74),
      rationale: roleMatches.length
        ? `Role candidate has multiple deterministic matches and requires validation review.`
        : `Role candidate did not match an existing canonical role and is proposed as a new role.`,
      proposalPayload: buildBaseProposalPayload({
        candidate,
        matchStrategy: roleMatches.length ? "role_match_ambiguous" : "create_role_no_match",
        candidateAlternatives: alternatives,
        extra: {
          jurisdiction_resolution_strategy: jurisdictionResolution.strategy,
          resolved_jurisdiction_id: jurisdictionId
        }
      })
    };
  }

  if (candidate.candidateType === "jurisdiction") {
    const payload = normalizeCandidatePayload("jurisdiction", candidate.candidatePayload) as ExtractionCandidatePayloadByType["jurisdiction"];
    const parentResolution = payload.parent_jurisdiction_name
      ? await resolveJurisdictionReference(payload.parent_jurisdiction_name)
      : { strategy: "no_parent_jurisdiction_name", matches: [] as CanonicalJurisdictionRow[] };
    const jurisdictionMatches = await findJurisdictionsByName(payload.canonical_name, payload.jurisdiction_type);
    const filteredMatches = parentResolution.matches.length === 1
      ? jurisdictionMatches.filter((row) => row.parentJurisdictionId === parentResolution.matches[0].id)
      : jurisdictionMatches;

    if (filteredMatches.length === 1) {
      const match = filteredMatches[0];
      return {
        extractionCandidateId: candidate.id,
        proposalKey: buildPhaseDProposalKey({
          artifactId,
          extractionVersion: candidate.extractionVersion,
          candidateKey: candidate.candidateKey,
          proposalType: "merge_duplicate",
          targetRecordType: "jurisdiction",
          targetRecordId: match.id,
          matchStrategy: "exact_jurisdiction_name_type_match"
        }),
        proposalType: "merge_duplicate",
        targetRecordType: "jurisdiction",
        targetRecordId: match.id,
        proposalConfidence: 0.97,
        rationale: `Jurisdiction candidate exactly matches canonical jurisdiction ${match.name}.`,
        proposalPayload: buildBaseProposalPayload({
          candidate,
          matchStrategy: "exact_jurisdiction_name_type_match",
          matchedRecordType: "jurisdiction",
          matchedRecordId: match.id,
          candidateAlternatives: [],
          extra: {
            parent_resolution_strategy: parentResolution.strategy
          }
        })
      };
    }

    const alternatives = [
      ...filteredMatches.map((row) => toAlternativeRecord("jurisdiction", row, "jurisdiction_name_type_lookup")),
      ...parentResolution.matches.map((row) => toAlternativeRecord("jurisdiction", row, parentResolution.strategy))
    ].slice(0, 10);

    return {
      extractionCandidateId: candidate.id,
      proposalKey: buildPhaseDProposalKey({
        artifactId,
        extractionVersion: candidate.extractionVersion,
        candidateKey: candidate.candidateKey,
        proposalType: "create_jurisdiction",
        targetRecordType: null,
        targetRecordId: null,
        matchStrategy: filteredMatches.length ? "jurisdiction_match_ambiguous" : "create_jurisdiction_no_match"
      }),
      proposalType: "create_jurisdiction",
      targetRecordType: null,
      targetRecordId: null,
      proposalConfidence: clampConfidence(filteredMatches.length ? 0.5 : 0.78),
      rationale: filteredMatches.length
        ? `Jurisdiction candidate has multiple deterministic matches and requires validation review.`
        : `Jurisdiction candidate did not match an existing canonical jurisdiction and is proposed as a new jurisdiction.`,
      proposalPayload: buildBaseProposalPayload({
        candidate,
        matchStrategy: filteredMatches.length ? "jurisdiction_match_ambiguous" : "create_jurisdiction_no_match",
        candidateAlternatives: alternatives,
        extra: {
          parent_resolution_strategy: parentResolution.strategy,
          resolved_parent_jurisdiction_id: parentResolution.matches.length === 1 ? parentResolution.matches[0].id : null
        }
      })
    };
  }

  if (candidate.candidateType === "assertion") {
    const payload = normalizeCandidatePayload("assertion", candidate.candidatePayload) as ExtractionCandidatePayloadByType["assertion"];
    const subjectResolution = await resolveEntityReference(payload.subject_canonical_name);
    const objectResolution = payload.object_canonical_name
      ? await resolveEntityReference(payload.object_canonical_name)
      : { strategy: "no_object_entity", matches: [] as CanonicalEntityRow[], matchedAliasId: null };
    const roleMatches = payload.role_name
      ? await findRolesByName({
          roleName: payload.role_name
        })
      : [];

    const subjectId = subjectResolution.matches.length === 1 ? subjectResolution.matches[0].id : null;
    const objectId = objectResolution.matches.length === 1 ? objectResolution.matches[0].id : null;
    const roleId = roleMatches.length === 1 ? roleMatches[0].id : null;

    let exactMatches: CanonicalAssertionRow[] = [];
    let supersessionMatches: CanonicalAssertionRow[] = [];
    if (subjectId) {
      exactMatches = await findAssertionExactMatches({
        assertionType: payload.assertion_type,
        subjectEntityId: subjectId,
        objectEntityId: objectId,
        roleId,
        effectiveStartAt: payload.effective_start_at,
        effectiveEndAt: payload.effective_end_at,
        termEndAt: payload.term_end_at
      });
      if (!exactMatches.length) {
        supersessionMatches = await findAssertionSupersessionCandidates({
          assertionType: payload.assertion_type,
          subjectEntityId: subjectId,
          roleId
        });
      }
    }

    if (exactMatches.length === 1) {
      const match = exactMatches[0];
      return {
        extractionCandidateId: candidate.id,
        proposalKey: buildPhaseDProposalKey({
          artifactId,
          extractionVersion: candidate.extractionVersion,
          candidateKey: candidate.candidateKey,
          proposalType: "merge_duplicate",
          targetRecordType: "assertion",
          targetRecordId: match.id,
          matchStrategy: "exact_assertion_match"
        }),
        proposalType: "merge_duplicate",
        targetRecordType: "assertion",
        targetRecordId: match.id,
        proposalConfidence: 0.95,
        rationale: `Assertion candidate exactly matches an existing canonical assertion.`,
        proposalPayload: buildBaseProposalPayload({
          candidate,
          matchStrategy: "exact_assertion_match",
          matchedRecordType: "assertion",
          matchedRecordId: match.id,
          candidateAlternatives: [],
          extra: {
            assertion_policy_hints: {
              always_needs_review: true,
              officeholder_like: Boolean(payload.role_name),
              time_sensitive: Boolean(payload.is_time_sensitive),
              supersession_capable: Boolean(payload.role_name || payload.effective_end_at || payload.term_end_at)
            }
          }
        })
      };
    }

    if (supersessionMatches.length === 1) {
      const match = supersessionMatches[0];
      return {
        extractionCandidateId: candidate.id,
        proposalKey: buildPhaseDProposalKey({
          artifactId,
          extractionVersion: candidate.extractionVersion,
          candidateKey: candidate.candidateKey,
          proposalType: "supersede_assertion",
          targetRecordType: "assertion",
          targetRecordId: match.id,
          matchStrategy: "subject_role_supersession_candidate"
        }),
        proposalType: "supersede_assertion",
        targetRecordType: "assertion",
        targetRecordId: match.id,
        proposalConfidence: 0.78,
        rationale: `Assertion candidate appears to supersede an existing canonical assertion for the same subject/role pair.`,
        proposalPayload: buildBaseProposalPayload({
          candidate,
          matchStrategy: "subject_role_supersession_candidate",
          matchedRecordType: "assertion",
          matchedRecordId: match.id,
          candidateAlternatives: [],
          extra: {
            assertion_policy_hints: {
              always_needs_review: true,
              officeholder_like: Boolean(payload.role_name),
              time_sensitive: Boolean(payload.is_time_sensitive),
              supersession_capable: true
            }
          }
        })
      };
    }

    const alternatives = [
      ...subjectResolution.matches.map((row) => toAlternativeRecord("entity", row, subjectResolution.strategy)),
      ...objectResolution.matches.map((row) => toAlternativeRecord("entity", row, objectResolution.strategy)),
      ...roleMatches.map((row) => toAlternativeRecord("role", row, "role_name_lookup")),
      ...exactMatches.map((row) => toAlternativeRecord("assertion", row, "exact_assertion_match_candidate")),
      ...supersessionMatches.map((row) => toAlternativeRecord("assertion", row, "supersession_candidate"))
    ].slice(0, 10);

    const matchStrategy = exactMatches.length
      ? "assertion_match_ambiguous"
      : supersessionMatches.length > 1
        ? "assertion_supersession_ambiguous"
        : "create_assertion_candidate";

    return {
      extractionCandidateId: candidate.id,
      proposalKey: buildPhaseDProposalKey({
        artifactId,
        extractionVersion: candidate.extractionVersion,
        candidateKey: candidate.candidateKey,
        proposalType: "create_assertion",
        targetRecordType: null,
        targetRecordId: null,
        matchStrategy
      }),
      proposalType: "create_assertion",
      targetRecordType: null,
      targetRecordId: null,
      proposalConfidence: clampConfidence(subjectId && (objectId || roleId) ? 0.72 : 0.45),
      rationale: `Assertion candidate is proposed for validation with deterministic reference resolution context attached.`,
      proposalPayload: buildBaseProposalPayload({
        candidate,
        matchStrategy,
        candidateAlternatives: alternatives,
        extra: {
          resolved_subject_entity_id: subjectId,
          resolved_object_entity_id: objectId,
          resolved_role_id: roleId,
          subject_resolution_strategy: subjectResolution.strategy,
          object_resolution_strategy: objectResolution.strategy,
          assertion_policy_hints: {
            always_needs_review: true,
            officeholder_like: Boolean(payload.role_name),
            time_sensitive: Boolean(payload.is_time_sensitive),
            supersession_capable: Boolean(payload.role_name || supersessionMatches.length)
          }
        }
      })
    };
  }

  throw new Error(`Unsupported Phase D candidate type: ${candidate.candidateType}`);
}

async function persistPhaseDMergeProposals(params: {
  lineageRunId: string;
  phaseDRunId: string;
  proposals: PhaseDPreparedProposal[];
}) {
  const sql = getSql();
  const transactionResults = await sql.transaction((tx) => {
    const queries: any[] = [];

    for (const proposal of params.proposals) {
      queries.push(tx`
        DELETE FROM dictionary.dictionary_merge_proposals
        WHERE extraction_candidate_id = ${proposal.extractionCandidateId}::uuid
          AND proposal_key <> ${proposal.proposalKey}
      `);

      queries.push(tx`
        INSERT INTO dictionary.dictionary_merge_proposals (
          substrate_run_id,
          extraction_candidate_id,
          proposal_key,
          proposal_type,
          target_record_type,
          target_record_id,
          proposal_confidence,
          rationale,
          proposal_payload,
          created_at
        )
        VALUES (
          ${params.lineageRunId}::uuid,
          ${proposal.extractionCandidateId}::uuid,
          ${proposal.proposalKey},
          ${proposal.proposalType},
          ${proposal.targetRecordType},
          ${proposal.targetRecordId || null}::uuid,
          ${proposal.proposalConfidence},
          ${proposal.rationale},
          ${JSON.stringify({
            ...proposal.proposalPayload,
            phase_d_pipeline_run_id: params.phaseDRunId
          })}::jsonb,
          NOW()
        )
        ON CONFLICT (proposal_key)
        DO UPDATE SET
          substrate_run_id = EXCLUDED.substrate_run_id,
          extraction_candidate_id = EXCLUDED.extraction_candidate_id,
          proposal_type = EXCLUDED.proposal_type,
          target_record_type = EXCLUDED.target_record_type,
          target_record_id = EXCLUDED.target_record_id,
          proposal_confidence = EXCLUDED.proposal_confidence,
          rationale = EXCLUDED.rationale,
          proposal_payload = EXCLUDED.proposal_payload
        RETURNING
          id,
          proposal_key as "proposalKey",
          extraction_candidate_id as "extractionCandidateId",
          proposal_type as "proposalType"
      `);
    }

    return queries;
  });

  const persistedRows = transactionResults
    .filter((_: any, index: number) => index % 2 === 1)
    .map((rows: any) => rows?.[0])
    .filter(Boolean) as Array<{ id: string; proposalKey: string; extractionCandidateId: string; proposalType: string }>;

  return {
    persistedRows,
    persistedCount: persistedRows.length,
    proposalTypeCounts: persistedRows.reduce((acc: Record<string, number>, row) => {
      acc[row.proposalType] = (acc[row.proposalType] || 0) + 1;
      return acc;
    }, {})
  };
}

function summarizeValidationIssueCodes(issueCodes: string[]): string {
  return issueCodes.length ? issueCodes.join(", ") : "validation_policy_block";
}

function buildPhaseDReviewSuggestedAction(params: {
  itemType: "merge_ambiguity" | "validation_failure";
  rootSource: RootSourceRow;
  proposalType: string;
}) {
  if (params.itemType === "merge_ambiguity") {
    return `Inspect Phase D merge ambiguity for ${params.rootSource.rootUrl} and choose the correct deterministic target before promotion.`;
  }
  return `Inspect Phase D validation failure for ${params.rootSource.rootUrl} and confirm whether the proposal should be corrected, retried, or left blocked.`;
}

async function recordPhaseDProposalReviewItem(params: {
  itemType: "merge_ambiguity" | "validation_failure";
  rootSource: RootSourceRow;
  artifactId: string;
  pipelineRunId: string;
  proposalId: string;
  proposalType: string;
  lastError: string;
}) {
  const sql = getSql();
  const nowIso = new Date().toISOString();
  const existingRows = await sql`
    SELECT id, retry_count as "retryCount"
    FROM dictionary.dictionary_review_queue
    WHERE item_type = ${params.itemType}
      AND root_source_id = ${params.rootSource.id}::uuid
      AND affected_record_type = 'merge_proposal'
      AND affected_record_id = ${params.proposalId}::uuid
      AND resolved_at IS NULL
    ORDER BY last_failed_at DESC, created_at DESC
    LIMIT 1
  `;

  const existing = existingRows[0] as { id: string; retryCount: number } | undefined;
  const failureCount = existing ? Number(existing.retryCount || 0) + 1 : 1;
  const severity: ReviewQueueSeverity =
    params.itemType === "merge_ambiguity"
      ? params.rootSource.trustTier === "authoritative"
        ? "high"
        : "medium"
      : params.rootSource.trustTier === "authoritative"
        ? "critical"
        : "high";
  const suggestedAction = buildPhaseDReviewSuggestedAction({
    itemType: params.itemType,
    rootSource: params.rootSource,
    proposalType: params.proposalType
  });

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
      ${params.itemType},
      ${severity},
      ${params.rootSource.id}::uuid,
      ${params.artifactId}::uuid,
      ${params.pipelineRunId}::uuid,
      'merge_proposal',
      ${params.proposalId}::uuid,
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

async function resolvePhaseDProposalReviewItems(params: {
  proposalId: string;
  pipelineRunId: string;
  itemTypes?: Array<"merge_ambiguity" | "validation_failure">;
}) {
  const sql = getSql();
  const itemTypes = params.itemTypes || ["merge_ambiguity", "validation_failure"];
  await sql`
    UPDATE dictionary.dictionary_review_queue
    SET
      resolved_at = NOW(),
      pipeline_run_id = ${params.pipelineRunId}::uuid,
      updated_at = NOW()
    WHERE affected_record_type = 'merge_proposal'
      AND affected_record_id = ${params.proposalId}::uuid
      AND item_type = ANY(${itemTypes}::dictionary.review_queue_item_type[])
      AND resolved_at IS NULL
  `;
}

function buildPhaseDValidationDecision(params: {
  proposal: PhaseDPreparedProposal;
  persistedProposalId: string;
  rootSource: RootSourceRow;
}): PhaseDPreparedValidation {
  const validatorName = "phase_d_first_pass_validator_v1";
  const payload = params.proposal.proposalPayload || {};
  const ambiguityCount = Number(payload.ambiguity_count || 0);
  const issueCodes: string[] = [];
  let outcome: ValidationOutcome = "approved";

  const candidatePayload = payload.candidate_payload as Record<string, unknown> | undefined;
  const assertionPolicyHints =
    payload.assertion_policy_hints && typeof payload.assertion_policy_hints === "object" && !Array.isArray(payload.assertion_policy_hints)
      ? (payload.assertion_policy_hints as Record<string, unknown>)
      : {};

  if (ambiguityCount > 0) {
    outcome = "needs_review";
    issueCodes.push("ambiguity_detected");
  }

  if (params.rootSource.trustTier === "contextual" && outcome === "approved") {
    outcome = "needs_review";
    issueCodes.push("contextual_source_requires_review");
  }

  if (
    params.rootSource.trustTier === "corroborative" &&
    outcome === "approved" &&
    ["create_entity", "create_role", "create_jurisdiction"].includes(params.proposal.proposalType)
  ) {
    outcome = "needs_review";
    issueCodes.push("corroborative_create_requires_review");
  }

  if (
    ["create_assertion", "supersede_assertion"].includes(params.proposal.proposalType) ||
    assertionPolicyHints.always_needs_review === true
  ) {
    outcome = "needs_review";
    issueCodes.push("assertion_requires_review");
  }

  if (
    ["merge_duplicate", "supersede_assertion"].includes(params.proposal.proposalType) &&
    (!params.proposal.targetRecordType || !params.proposal.targetRecordId)
  ) {
    outcome = "rejected";
    issueCodes.push("missing_target_reference");
  }

  if (
    params.proposal.proposalType === "add_alias" &&
    (params.proposal.targetRecordType !== "entity" || !params.proposal.targetRecordId)
  ) {
    outcome = "rejected";
    issueCodes.push("alias_target_entity_required");
  }

  if (
    ["create_entity", "create_role", "create_jurisdiction", "create_assertion"].includes(params.proposal.proposalType) &&
    (params.proposal.targetRecordType !== null || params.proposal.targetRecordId !== null)
  ) {
    outcome = "rejected";
    issueCodes.push("unexpected_create_target");
  }

  if (params.proposal.proposalConfidence < 0.5 && outcome === "approved") {
    outcome = "needs_review";
    issueCodes.push("low_confidence_requires_review");
  }

  if (
    params.proposal.proposalType === "create_entity" &&
    candidatePayload &&
    isGenericReferenceLabel(candidatePayload.canonical_name)
  ) {
    outcome = "rejected";
    issueCodes.push("generic_entity_name_blocked");
  }

  const details = {
    issue_codes: issueCodes,
    proposal_type: params.proposal.proposalType,
    proposal_key: params.proposal.proposalKey,
    proposal_confidence: params.proposal.proposalConfidence,
    match_strategy: payload.match_strategy || null,
    ambiguity_count: ambiguityCount,
    root_source_trust_tier: params.rootSource.trustTier,
    assertion_policy_hints: assertionPolicyHints,
    validated_at: new Date().toISOString()
  };

  return {
    mergeProposalId: params.persistedProposalId,
    outcome,
    validatorName,
    details
  };
}

async function persistPhaseDValidationResults(params: {
  lineageRunId: string;
  phaseDRunId: string;
  pipelineRunId: string;
  artifactId: string;
  rootSource: RootSourceRow;
  validations: PhaseDPreparedValidation[];
  proposalTypeById: Record<string, string>;
}) {
  const sql = getSql();
  const reviewQueueStates: Array<{
    mergeProposalId: string;
    outcome: ValidationOutcome;
    reviewQueueId: string | null;
    reviewItemType: "merge_ambiguity" | "validation_failure" | null;
  }> = [];
  const validationRows: Array<{ id: string; mergeProposalId: string; outcome: ValidationOutcome }> = [];
  const queryKinds: Array<
    | { kind: "deleteValidation" }
    | { kind: "insertValidation" }
    | { kind: "resolveReview"; mergeProposalId: string; outcome: ValidationOutcome }
    | { kind: "upsertReview"; mergeProposalId: string; outcome: ValidationOutcome; itemType: "merge_ambiguity" | "validation_failure" }
  > = [];

  const transactionResults = await sql.transaction((tx) => {
    const queries: any[] = [];

    for (const validation of params.validations) {
      queries.push(tx`
        DELETE FROM dictionary.dictionary_validation_results
        WHERE merge_proposal_id = ${validation.mergeProposalId}::uuid
          AND validator_name IS NOT DISTINCT FROM ${validation.validatorName}
      `);
      queryKinds.push({ kind: "deleteValidation" });

      queries.push(tx`
        INSERT INTO dictionary.dictionary_validation_results (
          substrate_run_id,
          merge_proposal_id,
          outcome,
          validator_name,
          details,
          created_at
        )
        VALUES (
          ${params.lineageRunId}::uuid,
          ${validation.mergeProposalId}::uuid,
          ${validation.outcome},
          ${validation.validatorName},
          ${JSON.stringify({
            ...validation.details,
            phase_d_pipeline_run_id: params.phaseDRunId
          })}::jsonb,
          NOW()
        )
        RETURNING
          id,
          merge_proposal_id as "mergeProposalId",
          outcome
      `);
      queryKinds.push({ kind: "insertValidation" });

      const issueCodes = Array.isArray(validation.details.issue_codes)
        ? (validation.details.issue_codes as string[])
        : [];

      if (validation.outcome === "approved") {
        queries.push(tx`
          UPDATE dictionary.dictionary_review_queue
          SET
            resolved_at = NOW(),
            pipeline_run_id = ${params.pipelineRunId}::uuid,
            updated_at = NOW()
          WHERE affected_record_type = 'merge_proposal'
            AND affected_record_id = ${validation.mergeProposalId}::uuid
            AND item_type = ANY(ARRAY['merge_ambiguity', 'validation_failure']::dictionary.review_queue_item_type[])
            AND resolved_at IS NULL
          RETURNING id
        `);
        queryKinds.push({
          kind: "resolveReview",
          mergeProposalId: validation.mergeProposalId,
          outcome: validation.outcome
        });
        continue;
      }

      const itemType: "merge_ambiguity" | "validation_failure" =
        validation.outcome === "needs_review" && issueCodes.includes("ambiguity_detected")
          ? "merge_ambiguity"
          : "validation_failure";
      const otherItemType = itemType === "merge_ambiguity" ? "validation_failure" : "merge_ambiguity";
      const severity: ReviewQueueSeverity =
        itemType === "merge_ambiguity"
          ? params.rootSource.trustTier === "authoritative"
            ? "high"
            : "medium"
          : params.rootSource.trustTier === "authoritative"
            ? "critical"
            : "high";
      const suggestedAction = buildPhaseDReviewSuggestedAction({
        itemType,
        rootSource: params.rootSource,
        proposalType: params.proposalTypeById[validation.mergeProposalId] || "unknown"
      });
      const lastError = summarizeValidationIssueCodes(issueCodes);

      queries.push(tx`
        WITH updated AS (
          UPDATE dictionary.dictionary_review_queue
          SET
            severity = ${severity},
            crawl_artifact_id = ${params.artifactId}::uuid,
            pipeline_run_id = ${params.pipelineRunId}::uuid,
            retry_count = retry_count + 1,
            last_error = ${lastError},
            suggested_action = ${suggestedAction},
            last_failed_at = NOW(),
            updated_at = NOW()
          WHERE item_type = ${itemType}
            AND root_source_id = ${params.rootSource.id}::uuid
            AND affected_record_type = 'merge_proposal'
            AND affected_record_id = ${validation.mergeProposalId}::uuid
            AND resolved_at IS NULL
          RETURNING id
        ),
        inserted AS (
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
          SELECT
            ${itemType},
            ${severity},
            ${params.rootSource.id}::uuid,
            ${params.artifactId}::uuid,
            ${params.pipelineRunId}::uuid,
            'merge_proposal',
            ${validation.mergeProposalId}::uuid,
            1,
            ${lastError},
            ${suggestedAction},
            NOW(),
            NOW(),
            NOW(),
            NOW()
          WHERE NOT EXISTS (SELECT 1 FROM updated)
          RETURNING id
        )
        SELECT id FROM updated
        UNION ALL
        SELECT id FROM inserted
      `);
      queryKinds.push({
        kind: "upsertReview",
        mergeProposalId: validation.mergeProposalId,
        outcome: validation.outcome,
        itemType
      });

      queries.push(tx`
        UPDATE dictionary.dictionary_review_queue
        SET
          resolved_at = NOW(),
          pipeline_run_id = ${params.pipelineRunId}::uuid,
          updated_at = NOW()
        WHERE affected_record_type = 'merge_proposal'
          AND affected_record_id = ${validation.mergeProposalId}::uuid
          AND item_type = ${otherItemType}
          AND resolved_at IS NULL
        RETURNING id
      `);
      queryKinds.push({
        kind: "resolveReview",
        mergeProposalId: validation.mergeProposalId,
        outcome: validation.outcome
      });
    }

    return queries;
  });

  transactionResults.forEach((rows: any, index: number) => {
    const kind = queryKinds[index];
    if (!kind) return;
    if (kind.kind === "insertValidation") {
      const row = rows?.[0];
      if (row) {
        validationRows.push(row as { id: string; mergeProposalId: string; outcome: ValidationOutcome });
      }
      return;
    }
    if (kind.kind === "upsertReview") {
      const row = rows?.[0];
      reviewQueueStates.push({
        mergeProposalId: kind.mergeProposalId,
        outcome: kind.outcome,
        reviewQueueId: cleanText(row?.id || "", 80) || null,
        reviewItemType: kind.itemType
      });
      return;
    }
    if (kind.kind === "resolveReview" && kind.outcome === "approved") {
      reviewQueueStates.push({
        mergeProposalId: kind.mergeProposalId,
        outcome: kind.outcome,
        reviewQueueId: null,
        reviewItemType: null
      });
    }
  });

  return {
    persistedRows: validationRows,
    persistedCount: validationRows.length,
    outcomeCounts: validationRows.reduce((acc: Record<string, number>, row) => {
      acc[row.outcome] = (acc[row.outcome] || 0) + 1;
      return acc;
    }, {})
    ,
    reviewQueueStates
  };
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
  snapshotId?: string | null;
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
      snapshot_id = COALESCE(${params.snapshotId || null}::uuid, snapshot_id),
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

function derivePhaseFRootFreshnessSeverity(
  rootSource: PhaseFRootSourceAttentionRow
): ReviewQueueSeverity {
  if (rootSource.isBlocked) {
    return rootSource.trustTier === "authoritative" ? "critical" : "high";
  }
  return rootSource.trustTier === "authoritative" ? "high" : "medium";
}

function derivePhaseFAssertionFreshnessSeverity(
  assertion: PhaseFAssertionReviewRow
): ReviewQueueSeverity {
  if (assertion.isBlocked && (assertion.isHighImpact || assertion.trustTier === "authoritative")) {
    return "critical";
  }
  if (assertion.isHighImpact || assertion.isBlocked || assertion.trustTier === "authoritative") {
    return "high";
  }
  return "medium";
}

function derivePhaseFExpiredAssertionSeverity(
  assertion: PhaseFAssertionReviewRow
): ReviewQueueSeverity {
  if (assertion.trustTier === "authoritative") return "critical";
  return "high";
}

function buildPhaseFRootFreshnessSuggestedAction(rootSource: PhaseFRootSourceAttentionRow): string {
  if (rootSource.isBlocked) {
    return `Inspect unresolved source failures for ${rootSource.rootUrl} before attempting another freshness refresh.`;
  }
  return `Refresh overdue root source ${rootSource.rootUrl} through the existing substrate ingestion flow and verify the source still yields handoff-ready content.`;
}

function buildPhaseFAssertionFreshnessSuggestedAction(assertion: PhaseFAssertionReviewRow): string {
  const sourceLabel = assertion.sourceDomain || assertion.sourceUrl || "the linked source";
  if (assertion.isBlocked) {
    return `Inspect unresolved source failures blocking revalidation for assertion ${assertion.assertionId} from ${sourceLabel}.`;
  }
  if (assertion.isHighImpact) {
    return `Revalidate overdue high-impact assertion ${assertion.assertionId} against ${sourceLabel} and confirm whether a successor change should be promoted later.`;
  }
  return `Revalidate overdue assertion ${assertion.assertionId} against ${sourceLabel} and confirm the canonical review state.`;
}

function buildPhaseFExpiredHighImpactSuggestedAction(assertion: PhaseFAssertionReviewRow): string {
  return `Review expired high-impact assertion ${assertion.assertionId} and determine whether a successor assertion should be promoted or the record should remain expired without replacement.`;
}

function buildPhaseFReviewItemUpsertQuery(tx: any, params: {
  itemType: "freshness_overdue" | "expired_high_impact_assertion";
  severity: ReviewQueueSeverity;
  rootSourceId?: string | null;
  crawlArtifactId?: string | null;
  pipelineRunId: string;
  affectedRecordType: "root_source" | "assertion";
  affectedRecordId: string;
  lastError: string;
  suggestedAction: string;
}) {
  return tx`
    WITH updated AS (
      UPDATE dictionary.dictionary_review_queue
      SET
        severity = ${params.severity},
        root_source_id = COALESCE(${params.rootSourceId || null}::uuid, root_source_id),
        crawl_artifact_id = COALESCE(${params.crawlArtifactId || null}::uuid, crawl_artifact_id),
        pipeline_run_id = ${params.pipelineRunId}::uuid,
        last_error = ${params.lastError},
        suggested_action = ${params.suggestedAction},
        last_failed_at = NOW(),
        updated_at = NOW()
      WHERE item_type = ${params.itemType}
        AND affected_record_type = ${params.affectedRecordType}
        AND affected_record_id = ${params.affectedRecordId}::uuid
        AND resolved_at IS NULL
      RETURNING id, 'updated'::text as action
    ),
    inserted AS (
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
      SELECT
        ${params.itemType},
        ${params.severity},
        ${params.rootSourceId || null}::uuid,
        ${params.crawlArtifactId || null}::uuid,
        ${params.pipelineRunId}::uuid,
        ${params.affectedRecordType},
        ${params.affectedRecordId}::uuid,
        0,
        ${params.lastError},
        ${params.suggestedAction},
        NOW(),
        NOW(),
        NOW(),
        NOW()
      WHERE NOT EXISTS (SELECT 1 FROM updated)
      RETURNING id, 'inserted'::text as action
    )
    SELECT id, action FROM updated
    UNION ALL
    SELECT id, action FROM inserted
  `;
}

function buildPhaseFReviewItemResolveQuery(tx: any, params: {
  pipelineRunId: string;
  itemType: "freshness_overdue" | "expired_high_impact_assertion";
  affectedRecordType: "root_source" | "assertion";
  keepRecordIds: string[];
}) {
  if (!params.keepRecordIds.length) {
    return tx`
      UPDATE dictionary.dictionary_review_queue
      SET
        resolved_at = NOW(),
        pipeline_run_id = ${params.pipelineRunId}::uuid,
        updated_at = NOW()
      WHERE item_type = ${params.itemType}
        AND affected_record_type = ${params.affectedRecordType}
        AND resolved_at IS NULL
      RETURNING id
    `;
  }

  return tx`
    UPDATE dictionary.dictionary_review_queue
    SET
      resolved_at = NOW(),
      pipeline_run_id = ${params.pipelineRunId}::uuid,
      updated_at = NOW()
    WHERE item_type = ${params.itemType}
      AND affected_record_type = ${params.affectedRecordType}
      AND resolved_at IS NULL
      AND NOT (affected_record_id = ANY(${params.keepRecordIds}::uuid[]))
    RETURNING id
  `;
}

function buildPhaseFAssertionStateUpdateQuery(tx: any, params: {
  assertionId: string;
  computedValidityStatus: string;
  computedReviewStatus: string;
}) {
  return tx`
    UPDATE dictionary.dictionary_assertions
    SET
      validity_status = ${params.computedValidityStatus}::dictionary.assertion_validity_status,
      review_status = ${params.computedReviewStatus}::dictionary.assertion_review_status,
      updated_at = NOW()
    WHERE id = ${params.assertionId}::uuid
      AND (
        validity_status IS DISTINCT FROM ${params.computedValidityStatus}::dictionary.assertion_validity_status
        OR review_status IS DISTINCT FROM ${params.computedReviewStatus}::dictionary.assertion_review_status
      )
    RETURNING id
  `;
}

async function runPhaseFFreshnessScan(input: {
  trigger: DispatchTrigger;
  parentRunId?: string | null;
}) {
  const phaseFRun = await createPipelineRun({
    parentRunId: input.parentRunId || null,
    stageName: PHASE_F_STAGE_NAME,
    triggerType: input.trigger,
    inputPayload: {
      trigger: input.trigger,
      phaseBoundary: "phase_f_canonical_head_maintenance_only",
      reviewStateBoundary: "canonical_head_only_no_snapshot_publish"
    }
  });

  try {
    const asOf = new Date().toISOString();
    const assertions = await loadPhaseFAssertionsDueForReview(asOf);
    const rootSources = await loadPhaseFRootSourcesRequiringAttention(asOf);
    const sql = getSql();
    let assertionStateWriteCount = 0;
    let rootFreshnessItemWriteCount = 0;
    let assertionFreshnessItemWriteCount = 0;
    let expiredHighImpactItemWriteCount = 0;

    const overdueRootSourceIds: string[] = [];
    const overdueAssertionIds: string[] = [];
    const expiredHighImpactAssertionIds: string[] = [];

    const transactionResults = await sql.transaction((tx: any) => {
      const queries: any[] = [];

      for (const rootSource of rootSources) {
        if (!rootSource.overdueByFreshnessSla) continue;
        overdueRootSourceIds.push(rootSource.rootSourceId);
        queries.push(
          buildPhaseFReviewItemUpsertQuery(tx, {
            itemType: "freshness_overdue",
            severity: derivePhaseFRootFreshnessSeverity(rootSource),
            rootSourceId: rootSource.rootSourceId,
            pipelineRunId: phaseFRun.id,
            affectedRecordType: "root_source",
            affectedRecordId: rootSource.rootSourceId,
            lastError: rootSource.isBlocked
              ? "root_source_freshness_overdue_blocked_by_source_failures"
              : "root_source_freshness_sla_exceeded",
            suggestedAction: buildPhaseFRootFreshnessSuggestedAction(rootSource)
          })
        );
        rootFreshnessItemWriteCount += 1;
      }

      queries.push(
        buildPhaseFReviewItemResolveQuery(tx, {
          pipelineRunId: phaseFRun.id,
          itemType: "freshness_overdue",
          affectedRecordType: "root_source",
          keepRecordIds: overdueRootSourceIds
        })
      );

      for (const assertion of assertions) {
        queries.push(
          buildPhaseFAssertionStateUpdateQuery(tx, {
            assertionId: assertion.assertionId,
            computedValidityStatus: assertion.computedValidityStatus,
            computedReviewStatus: assertion.computedReviewStatus
          })
        );

        if (assertion.isOverdue) {
          overdueAssertionIds.push(assertion.assertionId);
          queries.push(
            buildPhaseFReviewItemUpsertQuery(tx, {
              itemType: "freshness_overdue",
              severity: derivePhaseFAssertionFreshnessSeverity(assertion),
              rootSourceId: assertion.rootSourceId,
              crawlArtifactId: assertion.crawlArtifactId,
              pipelineRunId: phaseFRun.id,
              affectedRecordType: "assertion",
              affectedRecordId: assertion.assertionId,
              lastError: assertion.isBlocked
                ? "assertion_review_overdue_blocked_by_source_failures"
                : "assertion_review_overdue",
              suggestedAction: buildPhaseFAssertionFreshnessSuggestedAction(assertion)
            })
          );
          assertionFreshnessItemWriteCount += 1;
        }

        if (assertion.expiresWithoutSuccessor) {
          expiredHighImpactAssertionIds.push(assertion.assertionId);
          queries.push(
            buildPhaseFReviewItemUpsertQuery(tx, {
              itemType: "expired_high_impact_assertion",
              severity: derivePhaseFExpiredAssertionSeverity(assertion),
              rootSourceId: assertion.rootSourceId,
              crawlArtifactId: assertion.crawlArtifactId,
              pipelineRunId: phaseFRun.id,
              affectedRecordType: "assertion",
              affectedRecordId: assertion.assertionId,
              lastError: "high_impact_assertion_expired_without_successor",
              suggestedAction: buildPhaseFExpiredHighImpactSuggestedAction(assertion)
            })
          );
          expiredHighImpactItemWriteCount += 1;
        }
      }

      queries.push(
        buildPhaseFReviewItemResolveQuery(tx, {
          pipelineRunId: phaseFRun.id,
          itemType: "freshness_overdue",
          affectedRecordType: "assertion",
          keepRecordIds: overdueAssertionIds
        })
      );

      queries.push(
        buildPhaseFReviewItemResolveQuery(tx, {
          pipelineRunId: phaseFRun.id,
          itemType: "expired_high_impact_assertion",
          affectedRecordType: "assertion",
          keepRecordIds: expiredHighImpactAssertionIds
        })
      );

      return queries;
    });

    let resultIndex = 0;
    resultIndex += rootFreshnessItemWriteCount;
    const resolvedRootFreshnessCount = (transactionResults[resultIndex++] || []).length;

    for (let index = 0; index < assertions.length; index += 1) {
      const rows = transactionResults[resultIndex++] || [];
      if (rows.length) assertionStateWriteCount += 1;

      if (assertions[index].isOverdue) {
        resultIndex += 1;
      }
      if (assertions[index].expiresWithoutSuccessor) {
        resultIndex += 1;
      }
    }

    const resolvedAssertionFreshnessCount = (transactionResults[resultIndex++] || []).length;
    const resolvedExpiredHighImpactCount = (transactionResults[resultIndex++] || []).length;

    const syncSummary = {
      assertions,
      rootSources,
      overdueRootSourceIds,
      overdueAssertionIds,
      expiredHighImpactAssertionIds,
      assertionStateWriteCount,
      rootFreshnessItemWriteCount,
      assertionFreshnessItemWriteCount,
      expiredHighImpactItemWriteCount,
      resolvedRootFreshnessCount,
      resolvedAssertionFreshnessCount,
      resolvedExpiredHighImpactCount
    };

    const needsReview =
      syncSummary.overdueRootSourceIds.length > 0 ||
      syncSummary.overdueAssertionIds.length > 0 ||
      syncSummary.expiredHighImpactAssertionIds.length > 0;

    await finalizePipelineRun({
      runId: phaseFRun.id,
      status: needsReview ? "needs_review" : "succeeded",
      outputPayload: {
        trigger: input.trigger,
        asOf,
        phaseBoundary: "phase_f_canonical_head_maintenance_only",
        snapshotMutationAttempted: false,
        transactionalSync: true,
        counts: {
          evaluatedAssertionCount: syncSummary.assertions.length,
          attentionRootSourceCount: syncSummary.rootSources.length,
          overdueRootSourceCount: syncSummary.overdueRootSourceIds.length,
          overdueAssertionCount: syncSummary.overdueAssertionIds.length,
          blockedAssertionCount: syncSummary.assertions.filter((row) => row.isBlocked).length,
          pendingRefreshAssertionCount: syncSummary.assertions.filter((row) => row.isPendingRefresh).length,
          expiredHighImpactAssertionCount: syncSummary.expiredHighImpactAssertionIds.length,
          assertionStateWriteCount: syncSummary.assertionStateWriteCount,
          rootFreshnessItemWriteCount: syncSummary.rootFreshnessItemWriteCount,
          assertionFreshnessItemWriteCount: syncSummary.assertionFreshnessItemWriteCount,
          expiredHighImpactItemWriteCount: syncSummary.expiredHighImpactItemWriteCount,
          resolvedRootFreshnessCount: syncSummary.resolvedRootFreshnessCount,
          resolvedAssertionFreshnessCount: syncSummary.resolvedAssertionFreshnessCount,
          resolvedExpiredHighImpactCount: syncSummary.resolvedExpiredHighImpactCount
        },
        overdueRootSourceIds: syncSummary.overdueRootSourceIds.slice(0, 20),
        overdueAssertionIds: syncSummary.overdueAssertionIds.slice(0, 20),
        expiredHighImpactAssertionIds: syncSummary.expiredHighImpactAssertionIds.slice(0, 20)
      },
      metrics: {
        overdueRootSourceCount: syncSummary.overdueRootSourceIds.length,
        overdueAssertionCount: syncSummary.overdueAssertionIds.length,
        expiredHighImpactAssertionCount: syncSummary.expiredHighImpactAssertionIds.length,
        assertionStateWriteCount: syncSummary.assertionStateWriteCount
      }
    });

    return {
      ok: true,
      runId: phaseFRun.id,
      trigger: input.trigger,
      asOf,
      needsReview,
      overdueRootSourceCount: syncSummary.overdueRootSourceIds.length,
      overdueAssertionCount: syncSummary.overdueAssertionIds.length,
      expiredHighImpactAssertionCount: syncSummary.expiredHighImpactAssertionIds.length,
      assertionStateWriteCount: syncSummary.assertionStateWriteCount
    };
  } catch (error: any) {
    await finalizePipelineRun({
      runId: phaseFRun.id,
      status: "failed",
      errorPayload: {
        trigger: input.trigger,
        phaseBoundary: "phase_f_freshness_scan_failed",
        message: cleanText(error?.message || "phase_f_freshness_scan_failed", 2000)
      }
    });
    throw error;
  }
}

async function markPhaseFRefreshDispatchAttempts(params: {
  pipelineRunId: string;
  rootSourceIds: string[];
}) {
  if (!params.rootSourceIds.length) return 0;
  const sql = getSql();
  const rows = await sql`
    UPDATE dictionary.dictionary_review_queue
    SET
      retry_count = retry_count + 1,
      pipeline_run_id = ${params.pipelineRunId}::uuid,
      updated_at = NOW()
    WHERE item_type = 'freshness_overdue'
      AND affected_record_type = 'root_source'
      AND affected_record_id = ANY(${params.rootSourceIds}::uuid[])
      AND resolved_at IS NULL
    RETURNING id
  `;
  return rows.length;
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
        const extractionResult = await step.run("run-phase-c-extraction", async () =>
          runPhaseCExtractionExecutionSafely({
            rootSource,
            artifact
          })
        );
        if (!extractionResult.ok) {
          let reviewState: { reviewQueueId?: string; severity?: string; failureCount?: number } | null = null;
          let reviewQueueWriteFailed = false;
          let reviewQueueWriteError: string | null = null;

          if (extractionResult.failureKind === "contract_failure") {
            try {
              reviewState = await step.run("record-phase-c-contract-review-failed", async () =>
                recordPhaseCExtractionContractFailure({
                  rootSource,
                  artifactId,
                  pipelineRunId: phaseCRun.id,
                  lastError: extractionResult.message
                })
              );
            } catch (reviewError: any) {
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
              status: extractionResult.failureKind === "contract_failure" ? "needs_review" : "failed",
              crawlArtifactId: artifactId,
              errorPayload: {
                rootSourceId,
                artifactId,
                trigger,
                phaseBoundary:
                  extractionResult.failureKind === "contract_failure"
                    ? "phase_c_extraction_contract_failure"
                    : "phase_c_extraction_failed",
                extractionAttempted: true,
                extractionCompleted: false,
                needsReview: extractionResult.failureKind === "contract_failure",
                reviewQueueWriteFailed,
                reviewQueueWriteError,
                reviewQueueId: reviewState?.reviewQueueId || null,
                reviewSeverity: reviewState?.severity || null,
                reviewFailureCount: reviewState?.failureCount ?? null,
                message: extractionResult.message,
                details: extractionResult.details || {}
              }
            })
          );

          return {
            ok: extractionResult.failureKind !== "contract_failure",
            rootSourceId,
            artifactId,
            trigger,
            runId: phaseCRun.id,
            parentRunId,
            needsReview: extractionResult.failureKind === "contract_failure",
            reviewQueueId: reviewState?.reviewQueueId || null,
            error: extractionResult.message
          };
        }

        const extraction = extractionResult.extraction;
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
              resolutionReady: !needsReview,
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
              candidatePersistenceSubstrateRunId: artifact.substrateRunId,
              nextStage: needsReview ? null : PHASE_D_STAGE_NAME,
              phaseDEventEmitted: false
            }
          })
        );

        let phaseDEventEmitted = false;
        let phaseDDispatchError: string | null = null;
        if (!needsReview) {
          try {
            await step.sendEvent(`emit-phase-d-merge-${artifactId}`, {
              name: PHASE_D_MERGE_EVENT,
              data: {
                rootSourceId,
                artifactId,
                extractionVersion: extraction.extractionVersion,
                trigger,
                parentRunId: phaseCRun.id
              }
            });
            phaseDEventEmitted = true;
            await step.run("mark-phase-d-event-emitted", async () =>
              markPhaseDEventEmission({
                runId: phaseCRun.id,
                phaseDDispatchAttempted: true,
                phaseDEventEmitted: true,
                phaseDDispatchEventName: PHASE_D_MERGE_EVENT,
                phaseDDispatchArtifactId: artifactId,
                phaseDDispatchError: null
              })
            );
          } catch (error: any) {
            await step.run("mark-phase-d-event-failed", async () =>
              markPhaseDEventEmission({
                runId: phaseCRun.id,
                phaseDDispatchAttempted: true,
                phaseDEventEmitted: false,
                phaseDDispatchEventName: PHASE_D_MERGE_EVENT,
                phaseDDispatchArtifactId: artifactId,
                phaseDDispatchError: cleanText(error?.message || "phase_d_dispatch_emit_failed", 2000)
              })
            );
            phaseDDispatchError = cleanText(error?.message || "phase_d_dispatch_emit_failed", 2000);
          }
        }

        return {
          ok: true,
          rootSourceId,
          artifactId,
          trigger,
          runId: phaseCRun.id,
          parentRunId,
          needsReview,
          phaseDEventEmitted,
          phaseDDispatchError,
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
        await step.run("finalize-phase-c-run-failed", async () =>
          finalizePipelineRun({
            runId: phaseCRun.id,
            status: "failed",
            crawlArtifactId: artifactId,
            errorPayload: {
              rootSourceId,
              artifactId,
              trigger,
              phaseBoundary: "phase_c_extraction_failed",
              extractionAttempted,
              extractionCompleted: false,
              message: cleanText(error?.message || "phase_c_extraction_failed", 2000)
            }
          })
        );
        return {
          ok: false,
          rootSourceId,
          artifactId,
          trigger,
          runId: phaseCRun.id,
          parentRunId,
          needsReview: false,
          error: cleanText(error?.message || "phase_c_extraction_failed", 2000)
        };
      }
    }
  );
}

export function createDictionarySubstrateMergeArtifactFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-merge-artifact" },
    { event: PHASE_D_MERGE_EVENT },
    async ({ event, step }: any) => {
      const rootSourceId = cleanText(event?.data?.rootSourceId || "", 80);
      const artifactId = cleanText(event?.data?.artifactId || "", 80);
      const extractionVersion = cleanText(event?.data?.extractionVersion || "", 120);
      const trigger = cleanText(event?.data?.trigger || "manual", 20) === "scheduled" ? "scheduled" : "manual";
      const parentRunId = cleanText(event?.data?.parentRunId || "", 80) || null;

      if (!rootSourceId) throw new Error("Missing rootSourceId");
      if (!artifactId) throw new Error("Missing artifactId");
      if (!extractionVersion) throw new Error("Missing extractionVersion");
      if (!UUID_RE.test(rootSourceId)) throw new Error("Invalid rootSourceId");
      if (!UUID_RE.test(artifactId)) throw new Error("Invalid artifactId");
      if (parentRunId && !UUID_RE.test(parentRunId)) throw new Error("Invalid parentRunId");

      const phaseDRun = await step.run("create-phase-d-run", async () =>
        createPipelineRun({
          parentRunId,
          rootSourceId,
          stageName: PHASE_D_STAGE_NAME,
          triggerType: trigger,
          inputPayload: {
            rootSourceId,
            artifactId,
            extractionVersion,
            trigger,
            phaseBoundary: "phase_c_extraction_complete",
            handoffContract: "phase_d_event_stub"
          }
        })
      );

      try {
        const rootSource = await step.run("load-phase-d-root-source", async () => {
          const row = await loadRootSourceById(rootSourceId);
          if (!row) throw new Error(`Unknown root source ${rootSourceId}`);
          if (!row.enabled) {
            throw new Error(`Root source ${rootSourceId} is disabled`);
          }
          return row;
        });

        const artifact = await step.run("load-phase-d-artifact", async () => {
          const row = await loadExtractionArtifactForPhaseC(rootSourceId, artifactId);
          if (!row) throw new Error(`Unknown extraction artifact ${artifactId}`);
          return row;
        });

        const candidates = await step.run("load-phase-d-candidates", async () =>
          loadPhaseDCandidatesForArtifact(artifactId, extractionVersion)
        );

        const extractedCandidates = candidates.filter(
          (candidate) => candidate.status === "extracted" && candidate.candidateType !== "diagnostic"
        );
        const diagnosticCandidates = candidates.filter((candidate) => candidate.candidateType === "diagnostic");
        const rejectedCandidates = candidates.filter((candidate) => candidate.status === "rejected");
        const proposals = await step.run("build-phase-d-proposals", async () => {
          const built: PhaseDPreparedProposal[] = [];
          for (const candidate of extractedCandidates) {
            built.push(
              await buildPhaseDProposalForCandidate({
                artifactId,
                candidate
              })
            );
          }
          return built;
        });

        const persistence = await step.run("persist-phase-d-proposals", async () =>
          persistPhaseDMergeProposals({
            lineageRunId: artifact.substrateRunId,
            phaseDRunId: phaseDRun.id,
            proposals
          })
        );
        const proposalIdByKey = new Map<string, string>(
          persistence.persistedRows.map((proposal) => [proposal.proposalKey, proposal.id])
        );
        const validations = await step.run("build-phase-d-validations", async () =>
          proposals.map((proposal) => {
            const persistedProposalId = proposalIdByKey.get(proposal.proposalKey);
            if (!persistedProposalId) {
              throw new Error(`Missing persisted merge proposal for proposal key ${proposal.proposalKey}`);
            }
            return buildPhaseDValidationDecision({
              proposal,
              persistedProposalId,
              rootSource
            });
          })
        );

        const validationPersistence = await step.run("persist-phase-d-validations", async () =>
          persistPhaseDValidationResults({
            lineageRunId: artifact.substrateRunId,
            phaseDRunId: phaseDRun.id,
            pipelineRunId: phaseDRun.id,
            artifactId,
            rootSource,
            validations
            ,
            proposalTypeById: Object.fromEntries(
              proposals.map((proposal) => [proposalIdByKey.get(proposal.proposalKey) || "", proposal.proposalType])
            )
          })
        );

        const reviewOutcomeCount = validations.filter((validation) => validation.outcome !== "approved").length;
        const approvedValidationCount = validations.filter((validation) => validation.outcome === "approved").length;

        await step.run("finalize-phase-d-run", async () =>
          finalizePipelineRun({
            runId: phaseDRun.id,
            status: reviewOutcomeCount > 0 ? "needs_review" : "succeeded",
            crawlArtifactId: artifactId,
            outputPayload: {
              rootSourceId,
              artifactId,
              trigger,
              phaseBoundary: "phase_c_extraction_complete",
              resolutionAttempted: true,
              resolutionCompleted: true,
              resolutionReady: true,
              mergeProposalGenerationImplemented: true,
              rootSourceTrustTier: rootSource.trustTier,
              phaseCExtractionRunId: parentRunId,
              artifactSubstrateRunId: artifact.substrateRunId,
              extractionVersion,
              counts: {
                totalCandidates: candidates.length,
                extractedCandidateCount: extractedCandidates.length,
                diagnosticCandidateCount: diagnosticCandidates.length,
                rejectedCandidateCount: rejectedCandidates.length,
                proposalCount: persistence.persistedCount,
                validationCount: validationPersistence.persistedCount
              },
              candidateTypeCounts: candidates.reduce((acc: Record<string, number>, candidate) => {
                const key = `${candidate.status}:${candidate.candidateType}`;
                acc[key] = (acc[key] || 0) + 1;
                return acc;
              }, {}),
              proposalTypeCounts: persistence.proposalTypeCounts,
              validationOutcomeCounts: validationPersistence.outcomeCounts,
              extractedCandidateIds: extractedCandidates.slice(0, 20).map((candidate) => candidate.id),
              extractedCandidateKeys: extractedCandidates.slice(0, 20).map((candidate) => candidate.candidateKey),
              proposalIds: persistence.persistedRows.slice(0, 20).map((proposal) => proposal.id),
              proposalKeys: persistence.persistedRows.slice(0, 20).map((proposal) => proposal.proposalKey),
              validationResultIds: validationPersistence.persistedRows.slice(0, 20).map((row) => row.id),
              reviewQueueIds: validationPersistence.reviewQueueStates
                .map((row) => row.reviewQueueId)
                .filter((value): value is string => Boolean(value))
                .slice(0, 20),
              nextStage: approvedValidationCount > 0 ? PHASE_E_STAGE_NAME : null,
              phaseEEventEmitted: false
            }
          })
        );

        let phaseEEventEmitted = false;
        let phaseEDispatchError: string | null = null;
        if (approvedValidationCount > 0) {
          try {
            await step.sendEvent(`emit-phase-e-promote-${artifactId}`, {
              name: PHASE_E_PROMOTION_EVENT,
              data: {
                rootSourceId,
                artifactId,
                extractionVersion,
                trigger,
                phaseDRunId: phaseDRun.id,
                parentRunId: phaseDRun.id
              }
            });
            phaseEEventEmitted = true;
            await step.run("mark-phase-e-event-emitted", async () =>
              markPhaseEEventEmission({
                runId: phaseDRun.id,
                phaseEDispatchAttempted: true,
                phaseEEventEmitted: true,
                phaseEDispatchEventName: PHASE_E_PROMOTION_EVENT,
                phaseEDispatchArtifactId: artifactId,
                phaseEDispatchError: null
              })
            );
          } catch (error: any) {
            await step.run("mark-phase-e-event-failed", async () =>
              markPhaseEEventEmission({
                runId: phaseDRun.id,
                phaseEDispatchAttempted: true,
                phaseEEventEmitted: false,
                phaseEDispatchEventName: PHASE_E_PROMOTION_EVENT,
                phaseEDispatchArtifactId: artifactId,
                phaseEDispatchError: cleanText(error?.message || "phase_e_dispatch_emit_failed", 2000)
              })
            );
            phaseEDispatchError = cleanText(error?.message || "phase_e_dispatch_emit_failed", 2000);
          }
        }

        return {
          ok: true,
          rootSourceId,
          artifactId,
          extractionVersion,
          trigger,
          runId: phaseDRun.id,
          parentRunId,
          counts: {
            totalCandidates: candidates.length,
            extractedCandidateCount: extractedCandidates.length,
            diagnosticCandidateCount: diagnosticCandidates.length,
            rejectedCandidateCount: rejectedCandidates.length,
            proposalCount: persistence.persistedCount,
            validationCount: validationPersistence.persistedCount,
            approvedValidationCount
          },
          phaseEEventEmitted,
          phaseEDispatchError
        };
      } catch (error: any) {
        await step.run("finalize-phase-d-run-failed", async () =>
          finalizePipelineRun({
            runId: phaseDRun.id,
            status: "failed",
            crawlArtifactId: artifactId,
            errorPayload: {
              rootSourceId,
              artifactId,
              extractionVersion,
              trigger,
              phaseBoundary: "phase_d_merge_proposals_failed",
              resolutionAttempted: true,
              resolutionCompleted: false,
              message: cleanText(error?.message || "phase_d_merge_proposals_failed", 2000)
            }
          })
        );

        return {
          ok: false,
          rootSourceId,
          artifactId,
          extractionVersion,
          trigger,
          runId: phaseDRun.id,
          parentRunId,
          error: cleanText(error?.message || "phase_d_merge_proposals_failed", 2000)
        };
      }
    }
  );
}

export function createDictionarySubstrateFreshnessSchedulerFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-freshness-scheduler", concurrency: { limit: 1 } },
    { cron: "0 16 * * 1" },
    async ({ step }: any) => {
      await step.sendEvent("emit-dictionary-substrate-freshness-scan", {
        name: PHASE_F_SCAN_EVENT,
        data: {
          trigger: "scheduled",
          dispatchRefreshes: true,
          refreshLimit: PHASE_F_REFRESH_LIMIT_DEFAULT,
          refreshCooldownHours: PHASE_F_REFRESH_COOLDOWN_HOURS_DEFAULT
        }
      });

      return {
        ok: true,
        trigger: "scheduled",
        scheduledAt: new Date().toISOString(),
        dispatchRefreshes: true,
        refreshLimit: PHASE_F_REFRESH_LIMIT_DEFAULT,
        refreshCooldownHours: PHASE_F_REFRESH_COOLDOWN_HOURS_DEFAULT
      };
    }
  );
}

export function createDictionarySubstrateFreshnessScanFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-freshness-scan", concurrency: { limit: 1 } },
    { event: PHASE_F_SCAN_EVENT },
    async ({ event, step }: any) => {
      const trigger = cleanText(event?.data?.trigger || "manual", 20) === "scheduled" ? "scheduled" : "manual";
      const parentRunId = cleanText(event?.data?.parentRunId || "", 80) || null;
      const dispatchRefreshes = parseBoolean(event?.data?.dispatchRefreshes, trigger === "scheduled");
      const refreshLimit = parsePositiveInt(
        event?.data?.refreshLimit,
        PHASE_F_REFRESH_LIMIT_DEFAULT,
        1,
        50
      );
      const refreshCooldownHours = parsePositiveInt(
        event?.data?.refreshCooldownHours,
        PHASE_F_REFRESH_COOLDOWN_HOURS_DEFAULT,
        1,
        168
      );

      if (parentRunId && !UUID_RE.test(parentRunId)) {
        throw new Error("Invalid parentRunId");
      }

      const result = await step.run("phase-f-freshness-scan", async () =>
        runPhaseFFreshnessScan({
          trigger,
          parentRunId
        })
      );

      if (!dispatchRefreshes) {
        await step.run("mark-phase-f-refresh-dispatch-skipped", async () =>
          markPhaseFRefreshDispatch({
            runId: result.runId,
            phaseFRefreshDispatchAttempted: false,
            phaseFRefreshDispatchCompleted: false,
            refreshDispatchRequested: false,
            refreshDispatchLimit: refreshLimit,
            refreshDispatchCooldownHours: refreshCooldownHours,
            refreshDispatchCandidateCount: 0,
            refreshDispatchSelectedCount: 0,
            refreshDispatchSkippedRecentCount: 0,
            refreshDispatchEmittedCount: 0,
            refreshDispatchRootSourceIds: [],
            refreshDispatchRecentRootSourceIds: []
          })
        );
        return {
          ...result,
          refreshDispatchRequested: false,
          refreshDispatchEmittedCount: 0
        };
      }

      const emittedRootSourceIds: string[] = [];
      let retryCountAdvanced = 0;
      let dispatchCandidates: {
        candidates: PhaseFDispatchCandidateRow[];
        skippedRecentRootSourceIds: string[];
      } = {
        candidates: [],
        skippedRecentRootSourceIds: []
      };
      try {
        dispatchCandidates = await step.run("load-phase-f-refresh-dispatch-candidates", async () =>
          loadPhaseFRefreshDispatchCandidates({
            asOf: result.asOf,
            limit: refreshLimit,
            cooldownHours: refreshCooldownHours
          })
        );

        for (const candidate of dispatchCandidates.candidates) {
          await step.sendEvent(`emit-phase-f-root-refresh-${candidate.rootSourceId}`, {
            name: "dictionary.substrate.ingestion.root",
            data: {
              rootSourceId: candidate.rootSourceId,
              trigger,
              parentRunId: result.runId
            }
          });
          emittedRootSourceIds.push(candidate.rootSourceId);
        }

        retryCountAdvanced = await step.run("mark-phase-f-refresh-attempts", async () =>
          markPhaseFRefreshDispatchAttempts({
            pipelineRunId: result.runId,
            rootSourceIds: emittedRootSourceIds
          })
        );

        await step.run("mark-phase-f-refresh-dispatch", async () =>
          markPhaseFRefreshDispatch({
            runId: result.runId,
            phaseFRefreshDispatchAttempted: true,
            phaseFRefreshDispatchCompleted: true,
            refreshDispatchRequested: true,
            refreshDispatchLimit: refreshLimit,
            refreshDispatchCooldownHours: refreshCooldownHours,
            refreshDispatchCandidateCount:
              dispatchCandidates.candidates.length + dispatchCandidates.skippedRecentRootSourceIds.length,
            refreshDispatchSelectedCount: dispatchCandidates.candidates.length,
            refreshDispatchSkippedRecentCount: dispatchCandidates.skippedRecentRootSourceIds.length,
            refreshDispatchEmittedCount: emittedRootSourceIds.length,
            refreshDispatchRootSourceIds: emittedRootSourceIds,
            refreshDispatchRecentRootSourceIds: dispatchCandidates.skippedRecentRootSourceIds
          })
        );

        return {
          ...result,
          refreshDispatchRequested: true,
          refreshDispatchCandidateCount:
            dispatchCandidates.candidates.length + dispatchCandidates.skippedRecentRootSourceIds.length,
          refreshDispatchSelectedCount: dispatchCandidates.candidates.length,
          refreshDispatchSkippedRecentCount: dispatchCandidates.skippedRecentRootSourceIds.length,
          refreshDispatchEmittedCount: emittedRootSourceIds.length,
          refreshDispatchRootSourceIds: emittedRootSourceIds,
          refreshDispatchRecentRootSourceIds: dispatchCandidates.skippedRecentRootSourceIds,
          refreshRetryCountAdvanced: retryCountAdvanced
        };
      } catch (error: any) {
        if (emittedRootSourceIds.length > 0 && retryCountAdvanced === 0) {
          retryCountAdvanced = await step.run("mark-phase-f-refresh-attempts-partial", async () =>
            markPhaseFRefreshDispatchAttempts({
              pipelineRunId: result.runId,
              rootSourceIds: emittedRootSourceIds
            })
          );
        }

        await step.run("mark-phase-f-refresh-dispatch-failed", async () =>
          markPhaseFRefreshDispatch({
            runId: result.runId,
            phaseFRefreshDispatchAttempted: true,
            phaseFRefreshDispatchCompleted: false,
            refreshDispatchRequested: true,
            refreshDispatchLimit: refreshLimit,
            refreshDispatchCooldownHours: refreshCooldownHours,
            refreshDispatchCandidateCount:
              dispatchCandidates.candidates.length + dispatchCandidates.skippedRecentRootSourceIds.length,
            refreshDispatchSelectedCount: dispatchCandidates.candidates.length,
            refreshDispatchSkippedRecentCount: dispatchCandidates.skippedRecentRootSourceIds.length,
            refreshDispatchEmittedCount: emittedRootSourceIds.length,
            refreshDispatchRootSourceIds: emittedRootSourceIds,
            refreshDispatchRecentRootSourceIds: dispatchCandidates.skippedRecentRootSourceIds,
            refreshDispatchError: cleanText(
              error?.message || "phase_f_refresh_dispatch_failed",
              2000
            )
          })
        );
        throw error;
      }
    }
  );
}

export function createDictionarySubstratePromotionArtifactFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-promotion-artifact" },
    { event: PHASE_E_PROMOTION_EVENT },
    async ({ event, step }: any) => {
      const rootSourceId = cleanText(event?.data?.rootSourceId || "", 80);
      const artifactId = cleanText(event?.data?.artifactId || "", 80);
      const extractionVersion = cleanText(event?.data?.extractionVersion || "", 120);
      const phaseDRunId = cleanText(event?.data?.phaseDRunId || "", 80);
      const trigger = cleanText(event?.data?.trigger || "manual", 20) === "scheduled" ? "scheduled" : "manual";
      const parentRunId = cleanText(event?.data?.parentRunId || "", 80) || null;

      if (!rootSourceId) throw new Error("Missing rootSourceId");
      if (!artifactId) throw new Error("Missing artifactId");
      if (!extractionVersion) throw new Error("Missing extractionVersion");
      if (!phaseDRunId) throw new Error("Missing phaseDRunId");
      if (!UUID_RE.test(rootSourceId)) throw new Error("Invalid rootSourceId");
      if (!UUID_RE.test(artifactId)) throw new Error("Invalid artifactId");
      if (!UUID_RE.test(phaseDRunId)) throw new Error("Invalid phaseDRunId");
      if (parentRunId && !UUID_RE.test(parentRunId)) throw new Error("Invalid parentRunId");

      const phaseERun = await step.run("create-phase-e-run", async () =>
        createPipelineRun({
          parentRunId,
          rootSourceId,
          stageName: PHASE_E_STAGE_NAME,
          triggerType: trigger,
          inputPayload: {
            rootSourceId,
            artifactId,
            extractionVersion,
            phaseDRunId,
            trigger,
            phaseBoundary: "phase_d_merge_proposals_complete",
            handoffContract: "phase_e_promote_publish_v1"
          }
        })
      );

      let promoteAndPublishAttempted = false;

      try {
        const rootSource = await step.run("load-phase-e-root-source", async () => {
          const row = await loadRootSourceById(rootSourceId);
          if (!row) throw new Error(`Unknown root source ${rootSourceId}`);
          if (!row.enabled) {
            throw new Error(`Root source ${rootSourceId} is disabled`);
          }
          return row;
        });

        const artifact = await step.run("load-phase-e-artifact", async () => {
          const row = await loadExtractionArtifactForPhaseC(rootSourceId, artifactId);
          if (!row) throw new Error(`Unknown extraction artifact ${artifactId}`);
          return row;
        });

        const activeSnapshot = await step.run("load-phase-e-active-snapshot", async () =>
          loadActiveSnapshotMetadata()
        );

        const promotableProposals = await step.run("load-phase-e-promotable-proposals", async () =>
          loadPhaseEPromotableProposals({
            rootSourceId,
            artifactId,
            phaseDRunId
          })
        );

        promoteAndPublishAttempted = true;
        const publishResult = await step.run("promote-and-publish-phase-e-artifact-run", async () =>
          promoteAndPublishPhaseEArtifactRun({
            phaseERunId: phaseERun.id,
            rootSourceId,
            artifactId,
            phaseDRunId
          })
        );

        const promotionResults = await step.run("load-phase-e-promotion-results", async () =>
          loadPhaseEPromotionResultsForRun(phaseERun.id)
        );

        await step.run("finalize-phase-e-run", async () =>
          finalizePipelineRun({
            runId: phaseERun.id,
            crawlArtifactId: artifactId,
            snapshotId: publishResult.snapshotId,
            status: "succeeded",
            outputPayload: {
              rootSourceId,
              artifactId,
              extractionVersion,
              trigger,
              phaseDRunId,
              phaseBoundary: "phase_e_snapshot_publish_complete",
              promotionPlanningAttempted: true,
              promotionPlanningCompleted: true,
              promotionReady: promotableProposals.length > 0,
              canonicalMutationAttempted: true,
              canonicalMutationCompleted: true,
              snapshotPublishAttempted: true,
              snapshotPublishCompleted: publishResult.snapshotId !== null,
              rootSourceTrustTier: rootSource.trustTier,
              artifactSubstrateRunId: artifact.substrateRunId,
              priorActiveSnapshot: activeSnapshot
                ? {
                    id: activeSnapshot.id,
                    version: activeSnapshot.version,
                    publishedAt: activeSnapshot.publishedAt,
                    activatedAt: activeSnapshot.activatedAt,
                    counts: {
                      entityCount: activeSnapshot.entityCount,
                      assertionCount: activeSnapshot.assertionCount,
                      aliasCount: activeSnapshot.aliasCount
                    }
                  }
                : null,
              publishedSnapshot: publishResult.snapshotId
                ? {
                    id: publishResult.snapshotId,
                    version: publishResult.snapshotVersion
                  }
                : null,
              counts: {
                promotableProposalCount: promotableProposals.length,
                promotedCount: publishResult.promotedCount,
                noOpCount: publishResult.noOpCount
              },
              proposalTypeCounts: promotableProposals.reduce((acc: Record<string, number>, proposal) => {
                acc[proposal.proposalType] = (acc[proposal.proposalType] || 0) + 1;
                return acc;
              }, {}),
              promotionOutcomeCounts: promotionResults.reduce((acc: Record<string, number>, row) => {
                acc[row.promotionOutcome] = (acc[row.promotionOutcome] || 0) + 1;
                return acc;
              }, {}),
              promotableProposalIds: promotableProposals.slice(0, 20).map((proposal) => proposal.mergeProposalId),
              validationResultIds: promotableProposals.slice(0, 20).map((proposal) => proposal.validationResultId),
              proposalKeys: promotableProposals.slice(0, 20).map((proposal) => proposal.proposalKey),
              createdRecordIds: promotionResults
                .map((row) => row.createdRecordId)
                .filter((value): value is string => Boolean(value))
                .slice(0, 20),
              affectedRecordIds: promotionResults
                .map((row) => row.affectedRecordId)
                .filter((value): value is string => Boolean(value))
                .slice(0, 20),
              nextStage: null
            }
          })
        );

        return {
          ok: true,
          rootSourceId,
          artifactId,
          extractionVersion,
          trigger,
          runId: phaseERun.id,
          parentRunId,
          phaseDRunId,
          promotableProposalCount: promotableProposals.length,
          promotedCount: publishResult.promotedCount,
          noOpCount: publishResult.noOpCount,
          activeSnapshotVersion: activeSnapshot?.version || null,
          publishedSnapshotVersion: publishResult.snapshotVersion
        };
      } catch (error: any) {
        await step.run("finalize-phase-e-run-failed", async () =>
          finalizePipelineRun({
            runId: phaseERun.id,
            status: "failed",
            crawlArtifactId: artifactId,
            errorPayload: {
              rootSourceId,
              artifactId,
              extractionVersion,
              phaseDRunId,
              trigger,
              phaseBoundary: "phase_e_snapshot_publish_failed",
              promotionPlanningAttempted: true,
              promotionPlanningCompleted: true,
              canonicalMutationAttempted: promoteAndPublishAttempted,
              canonicalMutationCompleted: false,
              snapshotPublishAttempted: promoteAndPublishAttempted,
              snapshotPublishCompleted: false,
              message: cleanText(error?.message || "phase_e_snapshot_publish_failed", 2000)
            }
          })
        );

        return {
          ok: false,
          rootSourceId,
          artifactId,
          extractionVersion,
          trigger,
          runId: phaseERun.id,
          parentRunId,
          phaseDRunId,
          error: cleanText(error?.message || "phase_e_snapshot_publish_failed", 2000)
        };
      }
    }
  );
}
