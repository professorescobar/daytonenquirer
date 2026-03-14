// Skeleton only: wire this into your Inngest client bootstrap when ready.
// This file defines the 7-step contract for Layer 1 Gatekeeper.

import { randomUUID } from "node:crypto";
import { neon } from "@neondatabase/serverless";
import type { Inngest } from "inngest";

type SourceType = "rss" | "webhook" | "chat_yes" | "chat_specify";
type RelationToArchive = "none" | "duplicate" | "update" | "follow_up";
type Action = "reject" | "watch" | "promote";
type NextStep = "none" | "research_discovery" | "cluster_update" | "story_planning";

type SignalRecord = {
  id: number;
  personaId: string;
  sourceType: SourceType;
  title: string;
  snippet: string;
  sectionHint: string;
  personaSection: string;
  personaBeat: string;
  beatPolicy: {
    includeKeywords: string[];
    excludeKeywords: string[];
    requiredLocalTerms: string[];
  };
  metadata: Record<string, unknown>;
  createdAt: string;
  isAutoPromoteEnabled: boolean;
  dictionaryContext: Layer1DictionaryContext;
  normalizationContext: Layer1NormalizationContext;
  localityContext: Layer1LocalityContext;
};

type ActiveDictionarySnapshot = {
  id: string;
  version: number;
  publishedAt: string;
  activatedAt: string;
};

type Layer1DictionaryEntityMatch = {
  entityId: string;
  canonicalName: string;
  entityType: string;
  slug: string;
  matchSource: "canonical_name" | "alias";
  matchedText: string;
  aliasType: string | null;
  primaryJurisdictionId: string | null;
  normalizedAddress: string | null;
  lat: number | null;
  lng: number | null;
  spatialConfidence: number | null;
};

type Layer1DictionaryJurisdiction = {
  id: string;
  name: string;
  jurisdictionType: string;
  parentJurisdictionId: string | null;
  centroidLat: number | null;
  centroidLng: number | null;
  bbox: Record<string, unknown> | null;
  geojson: Record<string, unknown> | null;
};

type Layer1EligibleAssertion = {
  id: string;
  assertionType: string;
  subjectEntityId: string;
  objectEntityId: string | null;
  roleId: string | null;
  validityStatus: "current";
  reviewStatus: "verified" | "pending_refresh";
  effectiveStartAt: string | null;
  effectiveEndAt: string | null;
  termEndAt: string | null;
  observedAt: string | null;
  lastVerifiedAt: string | null;
  nextReviewAt: string | null;
  assertionConfidence: number | null;
};

type Layer1DictionaryContext = {
  snapshot: ActiveDictionarySnapshot | null;
  entityMatches: Layer1DictionaryEntityMatch[];
  jurisdictions: Layer1DictionaryJurisdiction[];
  eligibleAssertions: Layer1EligibleAssertion[];
};

type Layer1NormalizedEntity = {
  entityId: string;
  canonicalName: string;
  entityType: string;
  slug: string;
  matchedBy: Array<"canonical_name" | "alias">;
  matchedTexts: string[];
  aliasTexts: string[];
  aliasTypes: string[];
  primaryJurisdictionId: string | null;
  primaryJurisdictionName: string | null;
  normalizedAddress: string | null;
  lat: number | null;
  lng: number | null;
  spatialConfidence: number | null;
  eligibleAssertionIds: string[];
};

type Layer1AssertionSummary = {
  assertionId: string;
  assertionType: string;
  subjectEntityId: string;
  subjectCanonicalName: string | null;
  objectEntityId: string | null;
  objectCanonicalName: string | null;
  roleId: string | null;
  reviewStatus: "verified" | "pending_refresh";
  lastVerifiedAt: string | null;
  nextReviewAt: string | null;
};

type Layer1NormalizationContext = {
  snapshotId: string | null;
  snapshotVersion: number | null;
  normalizedEntities: Layer1NormalizedEntity[];
  normalizedEntityIds: string[];
  canonicalNames: string[];
  canonicalSlugs: string[];
  aliasTexts: string[];
  jurisdictionIds: string[];
  jurisdictionNames: string[];
  assertionSummaries: Layer1AssertionSummary[];
};

type Layer1LocalityContext = {
  isLocalByDictionary: boolean;
  canBypassLocalTermGate: boolean;
  confidence: "high" | "medium" | "none";
  evidenceTypes: Array<"direct_jurisdiction" | "normalized_address" | "entity_name_heuristic">;
  matchedJurisdictionNames: string[];
  matchedEntityIds: string[];
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
  personaSection: string;
  personaBeat: string;
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

type ResearchPassType = "strict_official" | "broad_context" | "historical_background";

type SearchPlanPass = {
  passType: ResearchPassType;
  intent: string;
  optimizedQueries: string[];
};

type SearchPlan = {
  passes: SearchPlanPass[];
};

type SearchPlanGenerationResult = {
  searchPlan: SearchPlan;
  provider: "gemini" | "openai" | "test";
  model: string;
};

type ResearchRunStatus = "completed" | "degraded" | "failed";

type ResearchPassPolicy = {
  passType: ResearchPassType;
  includeDomains: string[];
  maxAgeDays: number | null;
  minUsableArtifacts: number;
  minDistinctDomains: number;
  appliedTrustTier: string | null;
  trustProfileSummary: {
    totalDomains: number;
    officialDomains: number;
    localNewsDomains: number;
    contextualDomains: number;
    sources: string[];
  };
};

type ExecutedResearchPass = {
  passType: ResearchPassType;
  intent: string;
  optimizedQueries: string[];
  appliedDomains: string[];
  appliedMaxAgeDays: number | null;
  appliedTrustTier: string | null;
  trustProfileSummary: ResearchPassPolicy["trustProfileSummary"];
  fetchedResultCount: number;
  usableResultCount: number;
  distinctDomainCount: number;
  selected: TavilyResult[];
  sufficiencyMet: boolean;
};

type ResearchDiscoveryResult = {
  queries: string[];
  saved: number;
  fetched: number;
  status: ResearchRunStatus;
  provider: string;
  model: string;
  successfulPassType: ResearchPassType | null;
  degradationReason: string | null;
  failureReason: string | null;
  passSummaries: Array<{
    passType: ResearchPassType;
    fetchedResultCount: number;
    usableResultCount: number;
    distinctDomainCount: number;
    appliedDomains: string[];
    appliedMaxAgeDays: number | null;
    appliedTrustTier?: string | null;
    sufficiencyMet: boolean;
  }>;
};

type ResearchTrustEntry = {
  personaId: string | null;
  section: string | null;
  beat: string | null;
  domain: string;
  trustTier: "official" | "local_news" | "trusted" | "contextual";
  isOfficial: boolean;
  priority: number;
  enabled: boolean;
  source: "persona" | "default" | "signal";
};

type EvidenceSource = {
  sourceUrl: string;
  title: string | null;
  content: string | null;
  score: number;
  sourceType: StoryPlanningEvidence["sourceType"];
  publishedAt: string | null;
  sourceDomain: string | null;
};

type EvidenceCandidate = {
  claim: string;
  sourceUrl: string;
  evidenceQuote: string;
  confidence: number;
  whyItMatters: string;
};

type EvidenceSupportStatus = "corroborated" | "single_source" | "contested";
type EvidenceExtractionStatus = "READY" | "NEEDS_REPORTING";
type EvidenceExecutionOutcome =
  | "validated"
  | "contract_invalid"
  | "editorial_insufficiency"
  | "repair_failed"
  | "provider_failure"
  | "persistence_failed";

type JudgedEvidenceItem = EvidenceCandidate & {
  sourceType: StoryPlanningEvidence["sourceType"];
  publishedAt: string | null;
  sourceDomain: string | null;
  supportStatus: EvidenceSupportStatus;
};

type EvidenceExtractionArtifact = {
  extractionStatus: EvidenceExtractionStatus | "";
  decisionRationale: string;
  evidenceItems: JudgedEvidenceItem[];
  editorialRisks: string[];
  missingEvidence: string[];
  followUpQueries: string[];
};

type PersistedEvidenceItem = JudgedEvidenceItem & {
  evidenceId: string;
};

type EvidenceBundlePersistenceResult = {
  saved: number;
  version: number;
  isCanonical: boolean;
  extractionStatus: EvidenceExtractionStatus | "";
  executionOutcome: EvidenceExecutionOutcome;
};

type EvidenceBundleLoadResult = {
  artifact: EvidenceExtractionArtifact;
  version: number;
  isCanonical: boolean;
  executionOutcome: EvidenceExecutionOutcome | "";
  failureReasons: string[];
};

type StoryPlanningEvidence = {
  evidenceId: string;
  claim: string;
  sourceUrl: string;
  sourceType: "official" | "local_news" | "wire" | "secondary" | "other";
  evidenceQuote: string;
  confidence: number;
  publishedAt: string | null;
  whyItMatters: string;
};

type StoryPlanningStatus = "READY" | "NEEDS_REPORTING" | "REJECTED";
type StoryPlanningExecutionOutcome =
  | "validated"
  | "contract_invalid"
  | "editorial_insufficiency"
  | "repair_failed"
  | "provider_failure"
  | "persistence_failed";
type StoryPlanSectionPurpose = "lead" | "nut_graph" | "context" | "impact" | "what_next" | "uncertainty";
type StoryPlanAssertiveness = "high" | "medium" | "low";

type StoryPlanSection = {
  sectionId: string;
  heading: string;
  summary: string;
  purpose: StoryPlanSectionPurpose | "";
  evidenceIds: string[];
  evidenceSourceUrls: string[];
  assertiveness: StoryPlanAssertiveness | "";
  tensionFlags: string[];
  qualificationNotes: string[];
  priority: number | null;
};

type StoryPlanArtifact = {
  planningStatus: StoryPlanningStatus | "";
  decisionRationale: string;
  angle: string;
  narrativeStrategy: string;
  approvedEvidenceIds: string[];
  approvedSourceUrls: string[];
  sections: StoryPlanSection[];
  uncertaintyNotes: string[];
  missingInformation: string[];
  missingInformationQueries: string[];
  editorialRisks: string[];
  primarySourceAssessment: {
    hasPrimarySource: boolean;
    primaryEvidenceIds: string[];
    notes: string[];
  };
  personaFitAssessment: {
    fit: "strong" | "medium" | "weak" | "";
    notes: string[];
  };
};

type DraftWritingArtifact = {
  headline: string;
  dek: string;
  body: string;
  sourceUrls: string[];
  uncertaintyNotes: string[];
  coverageGaps: string[];
};

type DraftWriterProvider = "anthropic" | "openai" | "gemini" | "grok";

type DraftWriterConfig = {
  provider: DraftWriterProvider;
  model: string;
  source: "persona_config" | "hardcoded_fallback";
};

type DraftWriterExecutionResult = {
  provider: DraftWriterProvider;
  model: string;
  source: "persona_config" | "hardcoded_fallback";
  rawText: string;
  parsed: unknown | null;
};

type DraftWriterFailureInfo = {
  provider: DraftWriterProvider;
  model: string;
  source: "persona_config" | "hardcoded_fallback";
  message: string;
};

type DraftWritingValidationResult = {
  repaired: boolean;
  repairReason: string | null;
  sourceUrlCountAccepted: number;
};

type PacingConfig = {
  enabled: boolean;
  postingDays: boolean[];
  postsPerActiveDay: number;
  windowStartLocal: string;
  windowEndLocal: string;
  cadenceEnabled: boolean;
  singlePostTimeLocal: string | null;
  singlePostDaypart: "morning" | "midday" | "afternoon" | "evening" | null;
  minSpacingMinutes: number;
  maxBacklog: number;
  maxRetries: number;
  adminTimezone: string;
  globalDailyCap: number;
  killSwitchEnabled: boolean;
};

type QueueDecision = {
  signalId: number;
  personaId: string;
  decision: "queued" | "released" | "deferred" | "rejected" | "pass_through";
  reasonCode: string;
  scheduledForUtc: string | null;
  scheduledDayLocal: string | null;
};

type StagePromptBundle = {
  compiledPrompt: string;
  promptHash: string;
  promptSourceVersion: string;
  warnings: string[];
};

type SharedCompiledPrompt = {
  ok: boolean;
  compiledPrompt: string;
  warnings?: string[];
  promptHash: string;
  promptSourceVersion: string;
};

const {
  compileStagePrompt,
  normalizeStageName,
  normalizeSection
}: {
  compileStagePrompt: (options?: Record<string, unknown>) => SharedCompiledPrompt;
  normalizeStageName: (value: unknown) => string;
  normalizeSection: (value: unknown) => string;
} = require("../lib/topic-engine-prompts");

const TEST_SIGNAL_ID = 12345;
const TEST_MODE_ENABLED =
  String(process.env.TOPIC_ENGINE_TEST_MODE || "").trim().toLowerCase() === "true" ||
  String(process.env.VERCEL_ENV || "").trim().toLowerCase() !== "production";
const HARD_CODED_GATEKEEPER_MODEL = "gemini-2.0-flash";
const HARD_CODED_GATEKEEPER_OPENAI_MODEL = "gpt-4o-mini";
const HARD_CODED_RESEARCH_QUERY_MODEL = "gemini-1.5-flash";
const HARD_CODED_EVIDENCE_MODEL_CANDIDATES = ["gemini-1.5-pro", "gemini-1.5-pro-002"];
const HARD_CODED_STORY_PLANNING_OPENAI_MODEL = "gpt-4o-mini";
const HARD_CODED_STORY_PLANNING_GEMINI_MODEL = "gemini-1.5-flash";
const SUPPORTED_DRAFT_WRITER_PROVIDERS: DraftWriterProvider[] = ["anthropic", "openai", "gemini", "grok"];
const DRAFT_WRITING_TIMEOUT_MS = 45000;
const LOCAL_SCOPE_TERMS = [
  "dayton",
  "montgomery county",
  "miami valley",
  "kettering",
  "beavercreek",
  "centerville",
  "huber heights",
  "vandalia",
  "fairborn",
  "trotwood",
  "xenia",
  "moraine",
  "west carrollton"
];
const RESEARCH_PLAN_PASS_ORDER: ResearchPassType[] = ["strict_official", "broad_context", "historical_background"];
const RESEARCH_PASS_TYPE_ALIASES: Record<string, ResearchPassType> = {
  strict_official: "strict_official",
  pass_1_strict: "strict_official",
  broad_context: "broad_context",
  pass_2_broad: "broad_context",
  historical_background: "historical_background",
  pass_3_historical: "historical_background"
};
const SECTION_TRUSTED_RESEARCH_DOMAINS: Record<string, Array<{
  domain: string;
  trustTier: "official" | "local_news" | "trusted" | "contextual";
  isOfficial?: boolean;
  priority?: number;
}>> = {
  local: [
    { domain: "daytondailynews.com", trustTier: "local_news", priority: 20 },
    { domain: "whio.com", trustTier: "local_news", priority: 30 },
    { domain: "wdtn.com", trustTier: "local_news", priority: 40 }
  ],
  sports: [
    { domain: "milb.com", trustTier: "official", isOfficial: true, priority: 10 },
    { domain: "daytondragons.com", trustTier: "official", isOfficial: true, priority: 20 },
    { domain: "daytondailynews.com", trustTier: "local_news", priority: 30 }
  ],
  business: [
    { domain: "daytondailynews.com", trustTier: "local_news", priority: 20 },
    { domain: "bizjournals.com", trustTier: "trusted", priority: 30 },
    { domain: "whio.com", trustTier: "local_news", priority: 40 }
  ],
  health: [
    { domain: "daytondailynews.com", trustTier: "local_news", priority: 20 },
    { domain: "whio.com", trustTier: "local_news", priority: 30 },
    { domain: "wdtn.com", trustTier: "local_news", priority: 40 }
  ],
  entertainment: [
    { domain: "daytondailynews.com", trustTier: "local_news", priority: 20 },
    { domain: "whio.com", trustTier: "local_news", priority: 30 },
    { domain: "wdtn.com", trustTier: "local_news", priority: 40 }
  ],
  technology: [
    { domain: "daytondailynews.com", trustTier: "local_news", priority: 20 },
    { domain: "whio.com", trustTier: "local_news", priority: 30 },
    { domain: "wdtn.com", trustTier: "local_news", priority: 40 }
  ],
  national: [
    { domain: "apnews.com", trustTier: "trusted", priority: 20 },
    { domain: "reuters.com", trustTier: "trusted", priority: 30 }
  ],
  world: [
    { domain: "apnews.com", trustTier: "trusted", priority: 20 },
    { domain: "reuters.com", trustTier: "trusted", priority: 30 }
  ]
};

function isTestSignalId(signalId: number): boolean {
  return TEST_MODE_ENABLED && signalId === TEST_SIGNAL_ID;
}

function cleanText(value: unknown, max = 8000): string {
  return String(value || "").trim().slice(0, max);
}

function cleanComparableText(value: unknown, max = 8000): string {
  return cleanText(value, max)
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function containsWholePhrase(haystack: string, needle: string): boolean {
  const normalizedNeedle = cleanComparableText(needle, 240);
  if (!normalizedNeedle || normalizedNeedle.length < 4) return false;
  const pattern = new RegExp(`(^|[^a-z0-9])${escapeRegExp(normalizedNeedle)}([^a-z0-9]|$)`, "i");
  return pattern.test(haystack);
}

function normalizeSqlComparableExpression(columnSql: string): string {
  return `regexp_replace(regexp_replace(lower(${columnSql}), '[^a-z0-9\\s-]+', ' ', 'g'), '\\s+', ' ', 'g')`;
}

function normalizeUuid(value: unknown): string | null {
  const normalized = cleanText(value, 80).toLowerCase();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(normalized)
    ? normalized
    : null;
}

function toFiniteNumberOrNull(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toJsonObjectOrNull(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

async function loadActiveDictionarySnapshot(sql: any): Promise<ActiveDictionarySnapshot | null> {
  const rows = await sql`
    SELECT
      id,
      version,
      published_at as "publishedAt",
      activated_at as "activatedAt"
    FROM dictionary.active_snapshot_metadata
    LIMIT 1
  `;
  const row = rows[0];
  if (!row?.id) return null;
  return {
    id: cleanText(row.id, 80),
    version: Number(row.version || 0),
    publishedAt: new Date(row.publishedAt || Date.now()).toISOString(),
    activatedAt: new Date(row.activatedAt || Date.now()).toISOString()
  };
}

async function loadLayer1DictionaryContext(
  sql: any,
  signal: Pick<SignalRecord, "title" | "snippet" | "sectionHint">
): Promise<Layer1DictionaryContext> {
  const snapshot = await loadActiveDictionarySnapshot(sql);
  if (!snapshot) {
    return {
      snapshot: null,
      entityMatches: [],
      jurisdictions: [],
      eligibleAssertions: []
    };
  }

  const comparableText = cleanComparableText(
    [signal.title, signal.snippet, signal.sectionHint].filter(Boolean).join(" "),
    12000
  );
  if (!comparableText) {
    return {
      snapshot,
      entityMatches: [],
      jurisdictions: [],
      eligibleAssertions: []
    };
  }

  const entityRows = await sql`
    WITH canonical_matches AS (
      SELECT
        se.canonical_record_id as "entityId",
        se.canonical_name as "canonicalName",
        se.entity_type as "entityType",
        se.slug,
        'canonical_name'::text as "matchSource",
        se.canonical_name as "matchedText",
        NULL::text as "aliasType",
        se.primary_jurisdiction_id as "primaryJurisdictionId",
        se.normalized_address as "normalizedAddress",
        se.lat,
        se.lng,
        se.spatial_confidence as "spatialConfidence"
      FROM dictionary.dictionary_snapshot_entities se
      WHERE se.snapshot_id = ${snapshot.id}::uuid
        AND length(trim(se.canonical_name)) >= 4
        AND position(
          ${sql.unsafe(normalizeSqlComparableExpression("se.canonical_name"))}
          in ${comparableText}
        ) > 0
    ),
    alias_matches AS (
      SELECT
        se.canonical_record_id as "entityId",
        se.canonical_name as "canonicalName",
        se.entity_type as "entityType",
        se.slug,
        'alias'::text as "matchSource",
        sa.alias as "matchedText",
        sa.alias_type as "aliasType",
        se.primary_jurisdiction_id as "primaryJurisdictionId",
        se.normalized_address as "normalizedAddress",
        se.lat,
        se.lng,
        se.spatial_confidence as "spatialConfidence"
      FROM dictionary.dictionary_snapshot_aliases sa
      JOIN dictionary.dictionary_snapshot_entities se
        ON se.snapshot_id = sa.snapshot_id
       AND se.canonical_record_id = sa.entity_id
      WHERE sa.snapshot_id = ${snapshot.id}::uuid
        AND length(trim(sa.alias)) >= 4
        AND position(
          ${sql.unsafe(normalizeSqlComparableExpression("sa.alias"))}
          in ${comparableText}
        ) > 0
    )
    SELECT *
    FROM (
      SELECT * FROM canonical_matches
      UNION ALL
      SELECT * FROM alias_matches
    ) matches
    ORDER BY
      length(matches."matchedText") DESC,
      matches."canonicalName" ASC
    LIMIT 25
  `;

  const entityMatches = entityRows
    .map((row: any): Layer1DictionaryEntityMatch | null => {
      const matchedText = cleanText(row.matchedText, 240);
      if (!containsWholePhrase(comparableText, matchedText)) return null;
      return {
        entityId: cleanText(row.entityId, 80),
        canonicalName: cleanText(row.canonicalName, 240),
        entityType: cleanText(row.entityType, 120),
        slug: cleanText(row.slug, 240),
        matchSource: row.matchSource === "alias" ? "alias" : "canonical_name",
        matchedText,
        aliasType: cleanText(row.aliasType || "", 120) || null,
        primaryJurisdictionId: normalizeUuid(row.primaryJurisdictionId),
        normalizedAddress: cleanText(row.normalizedAddress || "", 320) || null,
        lat: toFiniteNumberOrNull(row.lat),
        lng: toFiniteNumberOrNull(row.lng),
        spatialConfidence: toFiniteNumberOrNull(row.spatialConfidence)
      };
    })
    .filter((row: Layer1DictionaryEntityMatch | null): row is Layer1DictionaryEntityMatch => Boolean(row));

  if (!entityMatches.length) {
    return {
      snapshot,
      entityMatches: [],
      jurisdictions: [],
      eligibleAssertions: []
    };
  }

  const uniqueEntityMatches: Layer1DictionaryEntityMatch[] = Array.from(
    new Map<string, Layer1DictionaryEntityMatch>(
      entityMatches.map((match) => [
        `${match.entityId}:${match.matchSource}:${match.matchedText.toLowerCase()}`,
        match
      ])
    ).values()
  );
  const entityIds = Array.from(new Set(uniqueEntityMatches.map((match) => match.entityId)));
  const jurisdictionIds = Array.from(
    new Set(uniqueEntityMatches.map((match) => match.primaryJurisdictionId).filter(Boolean))
  ) as string[];

  const [jurisdictionRows, assertionRows] = await Promise.all([
    jurisdictionIds.length
      ? sql`
          SELECT
            canonical_record_id as id,
            name,
            jurisdiction_type as "jurisdictionType",
            parent_jurisdiction_id as "parentJurisdictionId",
            centroid_lat as "centroidLat",
            centroid_lng as "centroidLng",
            bbox,
            geojson
          FROM dictionary.dictionary_snapshot_jurisdictions
          WHERE snapshot_id = ${snapshot.id}::uuid
            AND canonical_record_id = ANY(${jurisdictionIds}::uuid[])
        `
      : Promise.resolve([]),
    entityIds.length
      ? sql`
          SELECT
            canonical_record_id as id,
            assertion_type as "assertionType",
            subject_entity_id as "subjectEntityId",
            object_entity_id as "objectEntityId",
            role_id as "roleId",
            validity_status as "validityStatus",
            review_status as "reviewStatus",
            effective_start_at as "effectiveStartAt",
            effective_end_at as "effectiveEndAt",
            term_end_at as "termEndAt",
            observed_at as "observedAt",
            last_verified_at as "lastVerifiedAt",
            next_review_at as "nextReviewAt",
            assertion_confidence as "assertionConfidence"
          FROM dictionary.dictionary_snapshot_assertions
          WHERE snapshot_id = ${snapshot.id}::uuid
            AND validity_status = 'current'
            AND review_status IN ('verified', 'pending_refresh')
            AND (
              subject_entity_id = ANY(${entityIds}::uuid[])
              OR object_entity_id = ANY(${entityIds}::uuid[])
            )
          ORDER BY last_verified_at DESC NULLS LAST, updated_at DESC, created_at DESC
          LIMIT 50
        `
      : Promise.resolve([])
  ]);

  return {
    snapshot,
    entityMatches: uniqueEntityMatches,
    jurisdictions: jurisdictionRows.map((row: any): Layer1DictionaryJurisdiction => ({
      id: cleanText(row.id, 80),
      name: cleanText(row.name, 240),
      jurisdictionType: cleanText(row.jurisdictionType, 120),
      parentJurisdictionId: normalizeUuid(row.parentJurisdictionId),
      centroidLat: toFiniteNumberOrNull(row.centroidLat),
      centroidLng: toFiniteNumberOrNull(row.centroidLng),
      bbox: toJsonObjectOrNull(row.bbox),
      geojson: toJsonObjectOrNull(row.geojson)
    })),
    eligibleAssertions: assertionRows.map((row: any): Layer1EligibleAssertion => ({
      id: cleanText(row.id, 80),
      assertionType: cleanText(row.assertionType, 160),
      subjectEntityId: cleanText(row.subjectEntityId, 80),
      objectEntityId: normalizeUuid(row.objectEntityId),
      roleId: normalizeUuid(row.roleId),
      validityStatus: "current",
      reviewStatus: row.reviewStatus === "pending_refresh" ? "pending_refresh" : "verified",
      effectiveStartAt: row.effectiveStartAt ? new Date(row.effectiveStartAt).toISOString() : null,
      effectiveEndAt: row.effectiveEndAt ? new Date(row.effectiveEndAt).toISOString() : null,
      termEndAt: row.termEndAt ? new Date(row.termEndAt).toISOString() : null,
      observedAt: row.observedAt ? new Date(row.observedAt).toISOString() : null,
      lastVerifiedAt: row.lastVerifiedAt ? new Date(row.lastVerifiedAt).toISOString() : null,
      nextReviewAt: row.nextReviewAt ? new Date(row.nextReviewAt).toISOString() : null,
      assertionConfidence: toFiniteNumberOrNull(row.assertionConfidence)
    }))
  };
}

function compareEntityMatchPriority(a: Layer1DictionaryEntityMatch, b: Layer1DictionaryEntityMatch): number {
  const sourceWeight = (match: Layer1DictionaryEntityMatch) => (match.matchSource === "canonical_name" ? 0 : 1);
  return (
    sourceWeight(a) - sourceWeight(b) ||
    b.matchedText.length - a.matchedText.length ||
    a.canonicalName.localeCompare(b.canonicalName)
  );
}

function buildLayer1NormalizationContext(dictionaryContext: Layer1DictionaryContext): Layer1NormalizationContext {
  const jurisdictionById = new Map(dictionaryContext.jurisdictions.map((jurisdiction) => [jurisdiction.id, jurisdiction]));
  const entityById = new Map<string, Layer1NormalizedEntity>();

  const sortedMatches = [...dictionaryContext.entityMatches].sort(compareEntityMatchPriority);
  for (const match of sortedMatches) {
    const existing = entityById.get(match.entityId);
    const jurisdictionName = match.primaryJurisdictionId
      ? cleanText(jurisdictionById.get(match.primaryJurisdictionId)?.name || "", 240) || null
      : null;

    if (!existing) {
      entityById.set(match.entityId, {
        entityId: match.entityId,
        canonicalName: match.canonicalName,
        entityType: match.entityType,
        slug: match.slug,
        matchedBy: [match.matchSource],
        matchedTexts: [match.matchedText],
        aliasTexts: match.matchSource === "alias" ? [match.matchedText] : [],
        aliasTypes: match.aliasType ? [match.aliasType] : [],
        primaryJurisdictionId: match.primaryJurisdictionId,
        primaryJurisdictionName: jurisdictionName,
        normalizedAddress: match.normalizedAddress,
        lat: match.lat,
        lng: match.lng,
        spatialConfidence: match.spatialConfidence,
        eligibleAssertionIds: []
      });
      continue;
    }

    if (!existing.matchedBy.includes(match.matchSource)) {
      existing.matchedBy.push(match.matchSource);
    }
    if (!existing.matchedTexts.includes(match.matchedText)) {
      existing.matchedTexts.push(match.matchedText);
    }
    if (match.matchSource === "alias" && !existing.aliasTexts.includes(match.matchedText)) {
      existing.aliasTexts.push(match.matchedText);
    }
    if (match.aliasType && !existing.aliasTypes.includes(match.aliasType)) {
      existing.aliasTypes.push(match.aliasType);
    }
    if (!existing.primaryJurisdictionId && match.primaryJurisdictionId) {
      existing.primaryJurisdictionId = match.primaryJurisdictionId;
      existing.primaryJurisdictionName = jurisdictionName;
    }
    if (!existing.normalizedAddress && match.normalizedAddress) {
      existing.normalizedAddress = match.normalizedAddress;
    }
    if (existing.lat === null && match.lat !== null) {
      existing.lat = match.lat;
    }
    if (existing.lng === null && match.lng !== null) {
      existing.lng = match.lng;
    }
    if (existing.spatialConfidence === null && match.spatialConfidence !== null) {
      existing.spatialConfidence = match.spatialConfidence;
    }
  }

  const normalizedEntities = Array.from(entityById.values());
  const canonicalNameById = new Map(normalizedEntities.map((entity) => [entity.entityId, entity.canonicalName]));

  const assertionSummaries = dictionaryContext.eligibleAssertions.map((assertion) => {
    const subjectEntity = entityById.get(assertion.subjectEntityId);
    if (subjectEntity && !subjectEntity.eligibleAssertionIds.includes(assertion.id)) {
      subjectEntity.eligibleAssertionIds.push(assertion.id);
    }
    const objectEntity = assertion.objectEntityId ? entityById.get(assertion.objectEntityId) : null;
    if (objectEntity && !objectEntity.eligibleAssertionIds.includes(assertion.id)) {
      objectEntity.eligibleAssertionIds.push(assertion.id);
    }
    return {
      assertionId: assertion.id,
      assertionType: assertion.assertionType,
      subjectEntityId: assertion.subjectEntityId,
      subjectCanonicalName: cleanText(
        subjectEntity?.canonicalName || canonicalNameById.get(assertion.subjectEntityId) || "",
        240
      ) || null,
      objectEntityId: assertion.objectEntityId,
      objectCanonicalName: cleanText(
        (assertion.objectEntityId ? canonicalNameById.get(assertion.objectEntityId) : "") || "",
        240
      ) || null,
      roleId: assertion.roleId,
      reviewStatus: assertion.reviewStatus,
      lastVerifiedAt: assertion.lastVerifiedAt,
      nextReviewAt: assertion.nextReviewAt
    };
  });

  return {
    snapshotId: dictionaryContext.snapshot?.id || null,
    snapshotVersion: Number.isFinite(dictionaryContext.snapshot?.version)
      ? Number(dictionaryContext.snapshot?.version)
      : null,
    normalizedEntities,
    normalizedEntityIds: normalizedEntities.map((entity) => entity.entityId),
    canonicalNames: normalizedEntities.map((entity) => entity.canonicalName),
    canonicalSlugs: normalizedEntities.map((entity) => entity.slug).filter(Boolean),
    aliasTexts: Array.from(new Set(normalizedEntities.flatMap((entity) => entity.aliasTexts))),
    jurisdictionIds: Array.from(
      new Set(normalizedEntities.map((entity) => entity.primaryJurisdictionId).filter(Boolean))
    ) as string[],
    jurisdictionNames: Array.from(
      new Set(normalizedEntities.map((entity) => entity.primaryJurisdictionName).filter(Boolean))
    ) as string[],
    assertionSummaries
  };
}

function matchesKnownLocalArea(value: unknown): boolean {
  const normalized = cleanComparableText(value, 320);
  if (!normalized) return false;
  return LOCAL_SCOPE_TERMS.some((term) => containsWholePhrase(normalized, term) || containsWholePhrase(term, normalized));
}

function buildLayer1LocalityContext(normalizationContext: Layer1NormalizationContext): Layer1LocalityContext {
  const directJurisdictionEntities = normalizationContext.normalizedEntities.filter(
    (entity) => entity.primaryJurisdictionName && matchesKnownLocalArea(entity.primaryJurisdictionName)
  );
  const addressMatchedEntities = normalizationContext.normalizedEntities.filter(
    (entity) => entity.normalizedAddress && matchesKnownLocalArea(entity.normalizedAddress)
  );
  const entityNameHeuristicMatches = normalizationContext.normalizedEntities.filter(
    (entity) =>
      matchesKnownLocalArea(entity.canonicalName) || entity.aliasTexts.some((alias) => matchesKnownLocalArea(alias))
  );

  const evidenceTypes: Layer1LocalityContext["evidenceTypes"] = [];
  if (directJurisdictionEntities.length) evidenceTypes.push("direct_jurisdiction");
  if (addressMatchedEntities.length) evidenceTypes.push("normalized_address");
  if (!directJurisdictionEntities.length && !addressMatchedEntities.length && entityNameHeuristicMatches.length) {
    evidenceTypes.push("entity_name_heuristic");
  }

  const matchedJurisdictionNames = Array.from(
    new Set(
      [
        ...directJurisdictionEntities.map((entity) => entity.primaryJurisdictionName),
        ...addressMatchedEntities
          .map((entity) => entity.primaryJurisdictionName)
          .filter((name): name is string => Boolean(name))
      ].filter(Boolean)
    )
  );
  const matchedEntityIds = Array.from(
    new Set(
      [
        ...directJurisdictionEntities.map((entity) => entity.entityId),
        ...addressMatchedEntities.map((entity) => entity.entityId),
        ...entityNameHeuristicMatches.map((entity) => entity.entityId)
      ]
    )
  );

  return {
    isLocalByDictionary: evidenceTypes.length > 0,
    canBypassLocalTermGate: directJurisdictionEntities.length > 0 || addressMatchedEntities.length > 0,
    confidence: directJurisdictionEntities.length || addressMatchedEntities.length
      ? "high"
      : entityNameHeuristicMatches.length
        ? "medium"
        : "none",
    evidenceTypes,
    matchedJurisdictionNames,
    matchedEntityIds
  };
}

function buildLayer1GroupingProbes(normalizationContext: Layer1NormalizationContext): string[] {
  return Array.from(
    new Set(
      [...normalizationContext.canonicalNames, ...normalizationContext.aliasTexts]
        .map((value) => cleanText(value, 240))
        .filter((value) => value.length >= 4)
        .map((value) => value.toLowerCase())
    )
  )
    .sort((a, b) => b.length - a.length)
    .slice(0, 10);
}

function textMatchesGroupingProbe(value: unknown, probes: string[]): boolean {
  const haystack = cleanComparableText(value, 4000);
  if (!haystack || !probes.length) return false;
  return probes.some((probe) => containsWholePhrase(haystack, probe));
}

function scorePriorArtMatch(
  title: string,
  snippet: string,
  titleProbe: string,
  snippetProbe: string,
  groupingProbes: string[]
): number {
  let score = 0;
  const normalizedTitle = cleanComparableText(title, 1000);
  const normalizedSnippet = cleanComparableText(snippet, 2000);

  if (titleProbe && containsWholePhrase(normalizedTitle, titleProbe)) score += 0.85;
  if (snippetProbe && containsWholePhrase(normalizedSnippet, snippetProbe)) score += 0.55;

  const matchedGroupingProbes = groupingProbes.filter(
    (probe) => containsWholePhrase(normalizedTitle, probe) || containsWholePhrase(normalizedSnippet, probe)
  );
  if (matchedGroupingProbes.length) {
    score += Math.min(0.75, matchedGroupingProbes.length * 0.25);
  }

  return Number(score.toFixed(4));
}

function buildGatekeeperDictionaryPromptContext(signal: SignalRecord): Record<string, unknown> {
  return {
    snapshot: signal.dictionaryContext.snapshot
      ? {
          id: signal.dictionaryContext.snapshot.id,
          version: signal.dictionaryContext.snapshot.version,
          publishedAt: signal.dictionaryContext.snapshot.publishedAt
        }
      : null,
    normalization: {
      entityCount: signal.normalizationContext.normalizedEntities.length,
      canonicalNames: signal.normalizationContext.canonicalNames.slice(0, 8),
      aliasTexts: signal.normalizationContext.aliasTexts.slice(0, 8),
      jurisdictionNames: signal.normalizationContext.jurisdictionNames.slice(0, 6),
      assertionSummaries: signal.normalizationContext.assertionSummaries.slice(0, 8)
    },
    locality: {
      isLocalByDictionary: signal.localityContext.isLocalByDictionary,
      canBypassLocalTermGate: signal.localityContext.canBypassLocalTermGate,
      confidence: signal.localityContext.confidence,
      evidenceTypes: signal.localityContext.evidenceTypes,
      matchedJurisdictionNames: signal.localityContext.matchedJurisdictionNames
    }
  };
}

function buildDecisionObservabilityMetadata(signal: SignalRecord): Record<string, unknown> {
  return {
    phaseGLayer1DictionaryRead: {
      snapshotId: signal.dictionaryContext.snapshot?.id || null,
      snapshotVersion: signal.dictionaryContext.snapshot?.version || null,
      normalization: {
        normalizedEntityIds: signal.normalizationContext.normalizedEntityIds.slice(0, 12),
        canonicalNames: signal.normalizationContext.canonicalNames.slice(0, 8),
        aliasTexts: signal.normalizationContext.aliasTexts.slice(0, 8),
        jurisdictionNames: signal.normalizationContext.jurisdictionNames.slice(0, 6),
        assertionSummaryCount: signal.normalizationContext.assertionSummaries.length
      },
      locality: {
        isLocalByDictionary: signal.localityContext.isLocalByDictionary,
        canBypassLocalTermGate: signal.localityContext.canBypassLocalTermGate,
        confidence: signal.localityContext.confidence,
        evidenceTypes: signal.localityContext.evidenceTypes,
        matchedJurisdictionNames: signal.localityContext.matchedJurisdictionNames.slice(0, 6)
      }
    }
  };
}

function buildPromptSafeSignalPayload(signal: SignalRecord): Record<string, unknown> {
  const promptSafeMetadata = toSafeJsonObject(signal.metadata);
  delete promptSafeMetadata.phaseGLayer1DictionaryRead;

  return {
    id: signal.id,
    personaId: signal.personaId,
    sourceType: signal.sourceType,
    title: signal.title,
    snippet: signal.snippet,
    sectionHint: signal.sectionHint,
    personaSection: signal.personaSection,
    personaBeat: signal.personaBeat,
    beatPolicy: signal.beatPolicy,
    metadata: promptSafeMetadata,
    createdAt: signal.createdAt,
    isAutoPromoteEnabled: signal.isAutoPromoteEnabled
  };
}

function getCuratedDraftWriterModels(): Record<DraftWriterProvider, string> {
  return {
    openai: cleanText(process.env.TOPIC_ENGINE_DRAFT_WRITING_OPENAI_MODEL || "", 160) || "gpt-4o-mini",
    anthropic: cleanText(process.env.TOPIC_ENGINE_DRAFT_WRITING_ANTHROPIC_MODEL || "", 160) || "claude-haiku-4-5",
    gemini: cleanText(process.env.TOPIC_ENGINE_DRAFT_WRITING_GEMINI_MODEL || "", 160) || "gemini-3.1-flash-lite-preview",
    grok: cleanText(process.env.TOPIC_ENGINE_DRAFT_WRITING_GROK_MODEL || "", 160) || "grok-4-1-fast-non-reasoning"
  };
}

async function loadPromptLayerGuidance(
  sql: any,
  stageName: string,
  personaId: string,
  section: string
): Promise<{
  globalPrompt: string;
  sectionPrompt: string;
  personaPrompt: string;
  sourceVersions: { global: number | null; section: number | null; persona: number | null };
}> {
  const stage = cleanText(normalizeStageName(stageName), 120);
  if (!stage || !personaId) {
    return {
      globalPrompt: "",
      sectionPrompt: "",
      personaPrompt: "",
      sourceVersions: { global: null, section: null, persona: null }
    };
  }
  const normalizedSection = cleanText(normalizeSection(section || "local"), 120);

  const tableRows = await sql`
    SELECT
      to_regclass('public.topic_engine_prompt_layers') as "layersTable",
      to_regclass('public.topic_engine_stage_configs') as "stageConfigsTable"
  `;
  const hasLayersTable = Boolean(tableRows?.[0]?.layersTable);
  const hasStageConfigsTable = Boolean(tableRows?.[0]?.stageConfigsTable);
  if (!hasLayersTable) {
    return {
      globalPrompt: "",
      sectionPrompt: "",
      personaPrompt: "",
      sourceVersions: { global: null, section: null, persona: null }
    };
  }

  const globalRows = await sql`
    SELECT
      prompt_template as "promptTemplate",
      version
    FROM topic_engine_prompt_layers
    WHERE stage_name = ${stage}
      AND scope_type = 'global'
    LIMIT 1
  `;
  const sectionRows = normalizedSection
    ? await sql`
        SELECT
          prompt_template as "promptTemplate",
          version
        FROM topic_engine_prompt_layers
        WHERE stage_name = ${stage}
          AND scope_type = 'section'
          AND section = ${normalizedSection}
        LIMIT 1
      `
    : [];
  const personaRows = hasStageConfigsTable
    ? await sql`
        SELECT
          prompt_template as "promptTemplate"
        FROM topic_engine_stage_configs
        WHERE persona_id = ${personaId}
          AND stage_name = ${stage}
        LIMIT 1
      `
    : [];

  return {
    globalPrompt: cleanText(globalRows?.[0]?.promptTemplate || "", 50000),
    sectionPrompt: cleanText(sectionRows?.[0]?.promptTemplate || "", 50000),
    personaPrompt: cleanText(personaRows?.[0]?.promptTemplate || "", 50000),
    sourceVersions: {
      global: Number.isFinite(Number(globalRows?.[0]?.version)) ? Number(globalRows[0].version) : null,
      section: Number.isFinite(Number(sectionRows?.[0]?.version)) ? Number(sectionRows[0].version) : null,
      persona: null
    }
  };
}

async function buildStageGuidanceBundle(
  sql: any,
  stageName: string,
  personaId: string,
  section: string
): Promise<StagePromptBundle> {
  try {
    const guidance = await loadPromptLayerGuidance(sql, stageName, personaId, section);
    const compiled = compileStagePrompt({
      stageName,
      section,
      globalPrompt: guidance.globalPrompt,
      sectionPrompt: guidance.sectionPrompt,
      personaPrompt: guidance.personaPrompt,
      sourceVersions: guidance.sourceVersions
    });
    if (!compiled?.ok) {
      return {
        compiledPrompt: "",
        promptHash: "",
        promptSourceVersion: "",
        warnings: Array.isArray(compiled?.warnings) ? compiled.warnings : ["invalid_stage_name"]
      };
    }
    return {
      compiledPrompt: cleanText(compiled.compiledPrompt || "", 200000),
      promptHash: cleanText(compiled.promptHash || "", 120),
      promptSourceVersion: cleanText(compiled.promptSourceVersion || "", 120),
      warnings: Array.isArray(compiled.warnings) ? compiled.warnings : []
    };
  } catch (_) {
    return {
      compiledPrompt: "",
      promptHash: "",
      promptSourceVersion: "",
      warnings: ["guidance_load_failed"]
    };
  }
}

function toSafeJsonObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function toSlugKeywordList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => cleanText(item, 80).toLowerCase())
    .map((item) => item.replace(/[^a-z0-9- ]+/g, " ").replace(/\s+/g, " ").trim())
    .filter(Boolean);
}

function parseBeatPolicy(value: unknown): SignalRecord["beatPolicy"] {
  const raw = toSafeJsonObject(value);
  return {
    includeKeywords: toSlugKeywordList(raw.includeKeywords),
    excludeKeywords: toSlugKeywordList(raw.excludeKeywords),
    requiredLocalTerms: toSlugKeywordList(raw.requiredLocalTerms)
  };
}

function tokensFromBeat(beat: string): string[] {
  const stop = new Set(["general", "local", "national", "world"]);
  return beat
    .toLowerCase()
    .split("-")
    .map((part) => part.trim())
    .filter(Boolean)
    .filter((part) => !stop.has(part) && part.length >= 4);
}

function uniqueStrings(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean)));
}

function hasAnyTerm(haystack: string, terms: string[]): boolean {
  if (!terms.length) return false;
  const text = ` ${haystack.toLowerCase()} `;
  return terms.some((term) => {
    const t = cleanText(term, 80).toLowerCase();
    if (!t) return false;
    return text.includes(` ${t} `) || text.includes(t);
  });
}

function buildDefaultBeatPolicy(signal: SignalRecord): SignalRecord["beatPolicy"] {
  const section = cleanText(signal.personaSection || signal.sectionHint || "local", 80).toLowerCase() || "local";
  const beatTokens = tokensFromBeat(signal.personaBeat || "");
  const includeBySection: Record<string, string[]> = {
    local: ["city", "county", "dayton", "ohio", "public safety", "schools", "road", "community"],
    national: ["u.s.", "federal", "senate", "house", "states", "national"],
    world: ["international", "global", "foreign", "country", "diplomacy"],
    business: ["business", "company", "market", "economy", "jobs", "investment"],
    sports: ["game", "team", "season", "coach", "player", "score"],
    health: ["health", "medical", "hospital", "disease", "wellness", "care"],
    entertainment: ["movie", "music", "show", "festival", "artist", "game"],
    technology: ["technology", "software", "ai", "device", "startup", "innovation"]
  };
  return {
    includeKeywords: uniqueStrings([...(includeBySection[section] || []), ...beatTokens]),
    excludeKeywords: [],
    requiredLocalTerms: section === "local" ? LOCAL_SCOPE_TERMS : []
  };
}

function applyBeatPolicyPreFilter(signal: SignalRecord): GatekeeperOutput | null {
  const basePolicy = buildDefaultBeatPolicy(signal);
  const configured = signal.beatPolicy || { includeKeywords: [], excludeKeywords: [], requiredLocalTerms: [] };
  const policy = {
    includeKeywords: uniqueStrings([...(configured.includeKeywords || []), ...(basePolicy.includeKeywords || [])]),
    excludeKeywords: uniqueStrings(configured.excludeKeywords || []),
    requiredLocalTerms: uniqueStrings(configured.requiredLocalTerms?.length ? configured.requiredLocalTerms : basePolicy.requiredLocalTerms)
  };
  const text = [signal.title, signal.snippet, signal.sectionHint, JSON.stringify(signal.metadata || {})]
    .map((part) => cleanText(part, 6000))
    .join(" ")
    .toLowerCase();

  const flags: string[] = [];
  if (policy.requiredLocalTerms.length && !hasAnyTerm(text, policy.requiredLocalTerms)) {
    if (signal.localityContext.canBypassLocalTermGate) {
      return null;
    }
    flags.push("local_scope_mismatch");
    return {
      is_newsworthy: 0.25,
      is_local: false,
      confidence: 0.92,
      category: "Local Scope Mismatch",
      relation_to_archive: "none",
      event_key: `scope-${signal.id}`,
      action: "reject",
      next_step: "none",
      policy_flags: flags,
      reasoning:
        "Pre-LLM beat policy rejected signal: local section requires Dayton-area locality terms and none were found."
    };
  }
  if (policy.excludeKeywords.length && hasAnyTerm(text, policy.excludeKeywords)) {
    flags.push("beat_excluded_keyword_match");
    return {
      is_newsworthy: 0.35,
      is_local: true,
      confidence: 0.88,
      category: "Beat Exclusion Match",
      relation_to_archive: "none",
      event_key: `scope-${signal.id}`,
      action: "watch",
      next_step: "none",
      policy_flags: flags,
      reasoning: "Pre-LLM beat policy placed signal in watch due to excluded keyword match."
    };
  }
  if (policy.includeKeywords.length && !hasAnyTerm(text, policy.includeKeywords)) {
    flags.push("beat_scope_mismatch");
    return {
      is_newsworthy: 0.4,
      is_local: signal.personaSection === "local",
      confidence: 0.75,
      category: "Beat Scope Mismatch",
      relation_to_archive: "none",
      event_key: `scope-${signal.id}`,
      action: "watch",
      next_step: "none",
      policy_flags: flags,
      reasoning: "Pre-LLM beat policy placed signal in watch because it did not match beat/section topical keywords."
    };
  }
  return null;
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

function toBoolArray(value: unknown): boolean[] {
  if (!Array.isArray(value) || value.length !== 7) {
    return [true, true, true, true, true, true, true];
  }
  return value.map((v) => Boolean(v));
}

function parseTimeToMinutes(value: string, fallbackMinutes: number): number {
  const raw = cleanText(value, 20);
  const match = raw.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (!match) return fallbackMinutes;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return fallbackMinutes;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return fallbackMinutes;
  return hour * 60 + minute;
}

function minutesToTimeString(minutes: number): string {
  const clamped = Math.max(0, Math.min(1439, Math.round(minutes)));
  const hours = Math.floor(clamped / 60);
  const mins = clamped % 60;
  return `${String(hours).padStart(2, "0")}:${String(mins).padStart(2, "0")}:00`;
}

function normalizeWindow(startMinutes: number, endMinutes: number): { start: number; end: number; duration: number } {
  let start = startMinutes;
  let end = endMinutes;
  if (end <= start) end += 24 * 60;
  if (end - start < 15) end = start + 15;
  return { start, end, duration: end - start };
}

function getEvenlySpacedSlots(start: number, end: number, count: number): number[] {
  if (count <= 0) return [];
  const duration = Math.max(1, end - start);
  const interval = duration / (count + 1);
  const slots: number[] = [];
  for (let i = 1; i <= count; i += 1) {
    slots.push(start + interval * i);
  }
  return slots;
}

function allocateByWeights(total: number, weights: number[]): number[] {
  if (total <= 0) return weights.map(() => 0);
  const raw = weights.map((w) => (w / weights.reduce((a, b) => a + b, 0)) * total);
  const base = raw.map((v) => Math.floor(v));
  let used = base.reduce((a, b) => a + b, 0);
  const remainderOrder = raw
    .map((value, idx) => ({ idx, frac: value - Math.floor(value) }))
    .sort((a, b) => b.frac - a.frac);
  for (const item of remainderOrder) {
    if (used >= total) break;
    base[item.idx] += 1;
    used += 1;
  }
  return base;
}

function getCadencedSlots(start: number, end: number, count: number): number[] {
  if (count <= 0) return [];
  if (count === 1) return [start + (end - start) * 0.5];
  const duration = end - start;
  const segments = [
    { startPct: 0, endPct: 0.3 },
    { startPct: 0.3, endPct: 0.5 },
    { startPct: 0.5, endPct: 0.7 },
    { startPct: 0.7, endPct: 1.0 }
  ];
  const counts = allocateByWeights(count, [30, 20, 20, 30]);
  const slots: number[] = [];
  for (let i = 0; i < segments.length; i += 1) {
    const segment = segments[i];
    const segmentStart = start + duration * segment.startPct;
    const segmentEnd = start + duration * segment.endPct;
    slots.push(...getEvenlySpacedSlots(segmentStart, segmentEnd, counts[i]));
  }
  return slots.sort((a, b) => a - b);
}

function chooseSinglePostSlot(start: number, end: number, daypart: string | null, exactTime: string | null): number {
  const duration = end - start;
  if (exactTime) {
    const minute = parseTimeToMinutes(exactTime, Math.round(start + duration * 0.5));
    let adjusted = minute;
    if (adjusted < start) adjusted += 24 * 60;
    if (adjusted > end) adjusted = Math.round(start + duration * 0.5);
    return adjusted;
  }
  const byDaypart: Record<string, number> = {
    morning: 0.15,
    midday: 0.4,
    afternoon: 0.6,
    evening: 0.85
  };
  const pct = byDaypart[String(daypart || "").toLowerCase()] ?? 0.5;
  return start + duration * pct;
}

function getIsoDowFromLocalDate(localDate: string): number {
  const d = new Date(`${localDate}T00:00:00Z`);
  const dow = d.getUTCDay();
  return dow === 0 ? 7 : dow;
}

function isPostingDay(postingDays: boolean[], isoDow: number): boolean {
  const arr = toBoolArray(postingDays);
  return arr[isoDow - 1] === true;
}

function addDays(localDate: string, days: number): string {
  const d = new Date(`${localDate}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function nextActiveLocalDate(fromDate: string, postingDays: boolean[]): string {
  for (let offset = 0; offset < 14; offset += 1) {
    const candidate = addDays(fromDate, offset);
    if (isPostingDay(postingDays, getIsoDowFromLocalDate(candidate))) return candidate;
  }
  return fromDate;
}

async function convertLocalToUtc(sql: any, localDate: string, localTime: string, timezone: string): Promise<string> {
  const rows = await sql`
    SELECT
      ((${localDate}::date + ${localTime}::time) AT TIME ZONE ${timezone}) as "utcTs"
  `;
  const utcTs = rows?.[0]?.utcTs;
  if (!utcTs) throw new Error("Failed to convert local schedule to UTC");
  return new Date(utcTs).toISOString();
}

async function loadPacingConfig(sql: any, personaId: string): Promise<PacingConfig> {
  const rows = await sql`
    SELECT
      p.enabled,
      p.posting_days as "postingDays",
      p.posts_per_active_day as "postsPerActiveDay",
      p.window_start_local::text as "windowStartLocal",
      p.window_end_local::text as "windowEndLocal",
      p.cadence_enabled as "cadenceEnabled",
      p.single_post_time_local::text as "singlePostTimeLocal",
      p.single_post_daypart as "singlePostDaypart",
      p.min_spacing_minutes as "minSpacingMinutes",
      p.max_backlog as "maxBacklog",
      p.max_retries as "maxRetries",
      COALESCE(
        (
          SELECT value #>> '{}'
          FROM system_settings
          WHERE key = 'topic_engine_admin_timezone'
          LIMIT 1
        ),
        'America/New_York'
      ) as "adminTimezone",
      COALESCE(
        (
          SELECT (value #>> '{}')::int
          FROM system_settings
          WHERE key = 'topic_engine_global_daily_cap'
          LIMIT 1
        ),
        100
      ) as "globalDailyCap",
      COALESCE(
        (
          SELECT (value #>> '{}')::boolean
          FROM system_settings
          WHERE key = 'topic_engine_kill_switch_enabled'
          LIMIT 1
        ),
        false
      ) as "killSwitchEnabled"
    FROM topic_engine_pacing p
    WHERE p.persona_id = ${personaId}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row) {
    return {
      enabled: false,
      postingDays: [true, true, true, true, true, true, true],
      postsPerActiveDay: 1,
      windowStartLocal: "06:00:00",
      windowEndLocal: "22:00:00",
      cadenceEnabled: true,
      singlePostTimeLocal: null,
      singlePostDaypart: null,
      minSpacingMinutes: 90,
      maxBacklog: 200,
      maxRetries: 3,
      adminTimezone: "America/New_York",
      globalDailyCap: 100,
      killSwitchEnabled: false
    };
  }
  return {
    enabled: Boolean(row.enabled),
    postingDays: toBoolArray(row.postingDays),
    postsPerActiveDay: Math.max(0, Number(row.postsPerActiveDay || 1)),
    windowStartLocal: cleanText(row.windowStartLocal || "06:00:00", 20),
    windowEndLocal: cleanText(row.windowEndLocal || "22:00:00", 20),
    cadenceEnabled: Boolean(row.cadenceEnabled),
    singlePostTimeLocal: cleanText(row.singlePostTimeLocal || "", 20) || null,
    singlePostDaypart: cleanText(row.singlePostDaypart || "", 20) as
      | "morning"
      | "midday"
      | "afternoon"
      | "evening"
      | null,
    minSpacingMinutes: Math.max(0, Number(row.minSpacingMinutes || 90)),
    maxBacklog: Math.max(1, Number(row.maxBacklog || 200)),
    maxRetries: Math.max(0, Number(row.maxRetries || 3)),
    adminTimezone: cleanText(row.adminTimezone || "America/New_York", 100),
    globalDailyCap: Math.max(1, Number(row.globalDailyCap || 100)),
    killSwitchEnabled: Boolean(row.killSwitchEnabled)
  };
}

async function loadLocalClock(sql: any, timezone: string): Promise<{ nowLocalDate: string; nowLocalTime: string }> {
  const rows = await sql`
    SELECT
      (NOW() AT TIME ZONE ${timezone})::date::text as "nowLocalDate",
      to_char((NOW() AT TIME ZONE ${timezone})::time, 'HH24:MI:SS') as "nowLocalTime"
  `;
  return {
    nowLocalDate: cleanText(rows?.[0]?.nowLocalDate || new Date().toISOString().slice(0, 10), 10),
    nowLocalTime: cleanText(rows?.[0]?.nowLocalTime || "00:00:00", 20)
  };
}

async function loadDailyCounts(
  sql: any,
  personaId: string,
  localDate: string
): Promise<{ personaReleasedToday: number; globalReleasedToday: number; personaBacklog: number }> {
  const rows = await sql`
    SELECT
      (
        SELECT COUNT(*)::int
        FROM topic_engine_release_queue q
        WHERE q.persona_id = ${personaId}
          AND q.status = 'released'
          AND q.released_day_local = ${localDate}::date
      ) as "personaReleasedToday",
      (
        SELECT COUNT(*)::int
        FROM topic_engine_release_queue q
        WHERE q.status = 'released'
          AND q.released_day_local = ${localDate}::date
      ) as "globalReleasedToday",
      (
        SELECT COUNT(*)::int
        FROM topic_engine_release_queue q
        WHERE q.persona_id = ${personaId}
          AND q.status IN ('queued', 'deferred')
      ) as "personaBacklog"
  `;
  const row = rows[0] || {};
  return {
    personaReleasedToday: Number(row.personaReleasedToday || 0),
    globalReleasedToday: Number(row.globalReleasedToday || 0),
    personaBacklog: Number(row.personaBacklog || 0)
  };
}

async function loadExistingSlotsForDay(
  sql: any,
  personaId: string,
  dayLocal: string,
  timezone: string,
  excludeQueueId: string | null = null
): Promise<number[]> {
  const rows = await sql`
    SELECT
      to_char((q.scheduled_for_utc AT TIME ZONE ${timezone})::time, 'HH24:MI:SS') as "scheduledTime"
    FROM topic_engine_release_queue q
    WHERE q.persona_id = ${personaId}
      AND q.status IN ('queued', 'released')
      AND q.scheduled_day_local = ${dayLocal}::date
      AND q.scheduled_for_utc IS NOT NULL
      AND (${excludeQueueId}::uuid IS NULL OR q.id <> ${excludeQueueId}::uuid)
  `;
  return rows
    .map((row: any) => parseTimeToMinutes(cleanText(row.scheduledTime, 20), -1))
    .filter((value: number) => value >= 0);
}

async function loadReleasedSlotsForDay(
  sql: any,
  personaId: string,
  dayLocal: string,
  timezone: string,
  excludeQueueId: string | null = null
): Promise<number[]> {
  const rows = await sql`
    SELECT
      to_char((q.released_at AT TIME ZONE ${timezone})::time, 'HH24:MI:SS') as "releasedTime"
    FROM topic_engine_release_queue q
    WHERE q.persona_id = ${personaId}
      AND q.status = 'released'
      AND q.released_day_local = ${dayLocal}::date
      AND q.released_at IS NOT NULL
      AND (${excludeQueueId}::uuid IS NULL OR q.id <> ${excludeQueueId}::uuid)
  `;
  return rows
    .map((row: any) => parseTimeToMinutes(cleanText(row.releasedTime, 20), -1))
    .filter((value: number) => value >= 0);
}

async function buildDeferredSchedule(
  sql: any,
  personaId: string,
  config: PacingConfig,
  nowLocalDate: string,
  nowLocalTime: string,
  excludeQueueId: string | null
): Promise<{ scheduledForUtc: string; scheduledDayLocal: string } | null> {
  let targetLocalDate = nextActiveLocalDate(nowLocalDate, config.postingDays);
  for (let attempts = 0; attempts < 5; attempts += 1) {
    const existingSlots = await loadExistingSlotsForDay(
      sql,
      personaId,
      targetLocalDate,
      config.adminTimezone,
      excludeQueueId
    );
    const slotMinute = pickScheduledMinute(config, nowLocalTime, targetLocalDate === nowLocalDate, existingSlots);
    if (slotMinute === null) {
      targetLocalDate = nextActiveLocalDate(addDays(targetLocalDate, 1), config.postingDays);
      continue;
    }
    const localTime = minutesToTimeString(slotMinute);
    const scheduledForUtc = await convertLocalToUtc(sql, targetLocalDate, localTime, config.adminTimezone);
    if (new Date(scheduledForUtc).getTime() <= Date.now()) {
      targetLocalDate = nextActiveLocalDate(addDays(targetLocalDate, 1), config.postingDays);
      continue;
    }
    return {
      scheduledForUtc,
      scheduledDayLocal: targetLocalDate
    };
  }
  return null;
}

function pickScheduledMinute(
  config: PacingConfig,
  nowLocalTime: string,
  isToday: boolean,
  existingSlots: number[]
): number | null {
  const nowMinutes = parseTimeToMinutes(nowLocalTime, 0);
  const startMinutes = parseTimeToMinutes(config.windowStartLocal, 6 * 60);
  const endMinutes = parseTimeToMinutes(config.windowEndLocal, 22 * 60);
  const window = normalizeWindow(startMinutes, endMinutes);
  const posts = Math.max(0, config.postsPerActiveDay);

  let candidateSlots: number[] = [];
  if (posts <= 0) {
    return null;
  } else if (posts === 1) {
    const slot = config.cadenceEnabled
      ? chooseSinglePostSlot(window.start, window.end, config.singlePostDaypart, null)
      : chooseSinglePostSlot(window.start, window.end, null, config.singlePostTimeLocal);
    candidateSlots = [slot];
  } else if (!config.cadenceEnabled || window.duration <= 180) {
    candidateSlots = getEvenlySpacedSlots(window.start, window.end, posts);
  } else {
    candidateSlots = getCadencedSlots(window.start, window.end, posts);
  }

  const lowerBound = isToday ? nowMinutes : 0;
  for (const slot of candidateSlots) {
    const normalized = slot >= 24 * 60 ? slot - 24 * 60 : slot;
    if (isToday && normalized < lowerBound) continue;
    const hasSpacingConflict = existingSlots.some(
      (existing) => Math.abs(existing - normalized) < config.minSpacingMinutes
    );
    if (hasSpacingConflict) continue;
    return normalized;
  }
  if (!candidateSlots.length) return null;
  const fallback = candidateSlots[0] ?? window.start + window.duration * 0.5;
  return fallback >= 24 * 60 ? fallback - 24 * 60 : fallback;
}

async function upsertQueueDecision(sql: any, payload: QueueDecision): Promise<void> {
  await sql`
    INSERT INTO topic_engine_release_queue (
      signal_id,
      persona_id,
      status,
      reason_code,
      source_event,
      scheduled_for_utc,
      scheduled_day_local,
      updated_at
    )
    VALUES (
      ${payload.signalId},
      ${payload.personaId},
      ${
        payload.decision === "released"
          ? "released"
          : payload.decision === "rejected"
            ? "rejected"
            : payload.decision === "deferred"
              ? "deferred"
              : "queued"
      },
      ${payload.reasonCode},
      'signal.received',
      ${payload.scheduledForUtc},
      ${payload.scheduledDayLocal},
      NOW()
    )
    ON CONFLICT (signal_id) DO UPDATE
    SET
      persona_id = EXCLUDED.persona_id,
      status = EXCLUDED.status,
      reason_code = EXCLUDED.reason_code,
      scheduled_for_utc = EXCLUDED.scheduled_for_utc,
      scheduled_day_local = EXCLUDED.scheduled_day_local,
      updated_at = NOW()
  `;
}

async function applyQuotaPacingGate(signalId: number, personaId: string): Promise<QueueDecision> {
  if (isTestSignalId(signalId)) {
    return {
      signalId,
      personaId,
      decision: "released",
      reasonCode: "test_mode_bypass",
      scheduledForUtc: new Date().toISOString(),
      scheduledDayLocal: new Date().toISOString().slice(0, 10)
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  let config: PacingConfig;
  try {
    config = await loadPacingConfig(sql, personaId);
  } catch (error: any) {
    if (String(error?.message || "").includes("topic_engine_pacing")) {
      return {
        signalId,
        personaId,
        decision: "pass_through",
        reasonCode: "pacing_table_missing",
        scheduledForUtc: null,
        scheduledDayLocal: null
      };
    }
    throw error;
  }

  if (!config.enabled) {
    return {
      signalId,
      personaId,
      decision: "pass_through",
      reasonCode: "pacing_disabled",
      scheduledForUtc: null,
      scheduledDayLocal: null
    };
  }

  if (config.postsPerActiveDay <= 0) {
    const decision: QueueDecision = {
      signalId,
      personaId,
      decision: "rejected",
      reasonCode: "posts_per_active_day_zero",
      scheduledForUtc: null,
      scheduledDayLocal: null
    };
    await upsertQueueDecision(sql, decision);
    return decision;
  }

  if (config.killSwitchEnabled) {
    const decision: QueueDecision = {
      signalId,
      personaId,
      decision: "rejected",
      reasonCode: "kill_switch_enabled",
      scheduledForUtc: null,
      scheduledDayLocal: null
    };
    await upsertQueueDecision(sql, decision);
    return decision;
  }

  const clock = await loadLocalClock(sql, config.adminTimezone);
  const todayLocal = clock.nowLocalDate;
  const counts = await loadDailyCounts(sql, personaId, todayLocal);

  if (counts.personaBacklog >= config.maxBacklog) {
    const decision: QueueDecision = {
      signalId,
      personaId,
      decision: "rejected",
      reasonCode: "max_backlog_reached",
      scheduledForUtc: null,
      scheduledDayLocal: null
    };
    await upsertQueueDecision(sql, decision);
    return decision;
  }

  let targetLocalDate = nextActiveLocalDate(todayLocal, config.postingDays);
  if (counts.personaReleasedToday >= config.postsPerActiveDay || counts.globalReleasedToday >= config.globalDailyCap) {
    targetLocalDate = nextActiveLocalDate(addDays(todayLocal, 1), config.postingDays);
  }

  const existingSlots = await loadExistingSlotsForDay(sql, personaId, targetLocalDate, config.adminTimezone);
  const slotMinute = pickScheduledMinute(config, clock.nowLocalTime, targetLocalDate === todayLocal, existingSlots);
  if (slotMinute === null) {
    const decision: QueueDecision = {
      signalId,
      personaId,
      decision: "rejected",
      reasonCode: "no_available_schedule_slot",
      scheduledForUtc: null,
      scheduledDayLocal: targetLocalDate
    };
    await upsertQueueDecision(sql, decision);
    return decision;
  }
  const localTime = minutesToTimeString(slotMinute);
  const scheduledForUtc = await convertLocalToUtc(sql, targetLocalDate, localTime, config.adminTimezone);
  const scheduledDateUtcMs = new Date(scheduledForUtc).getTime();
  const releaseNow = Number.isFinite(scheduledDateUtcMs) && scheduledDateUtcMs <= Date.now();

  const queueDecision: QueueDecision = {
    signalId,
    personaId,
    decision: releaseNow ? "released" : targetLocalDate === todayLocal ? "queued" : "deferred",
    reasonCode:
      targetLocalDate !== todayLocal
        ? "next_active_day"
        : counts.personaReleasedToday >= config.postsPerActiveDay
          ? "persona_daily_cap_reached"
          : counts.globalReleasedToday >= config.globalDailyCap
            ? "global_daily_cap_reached"
            : "scheduled",
    scheduledForUtc,
    scheduledDayLocal: targetLocalDate
  };

  await upsertQueueDecision(sql, queueDecision);
  if (releaseNow) {
    await sql`
      UPDATE topic_engine_release_queue
      SET
        status = 'released',
        released_at = NOW(),
        released_day_local = ${targetLocalDate}::date,
        updated_at = NOW()
      WHERE signal_id = ${signalId}
    `;
  }
  return queueDecision;
}

async function releaseDueQueuedSignals(limit = 30): Promise<Array<{ signalId: number; personaId: string }>> {
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const timezoneRows = await sql`
    SELECT
      COALESCE(
        (
          SELECT value #>> '{}'
          FROM system_settings
          WHERE key = 'topic_engine_admin_timezone'
          LIMIT 1
        ),
        'America/New_York'
      ) as "adminTimezone",
      COALESCE(
        (
          SELECT (value #>> '{}')::boolean
          FROM system_settings
          WHERE key = 'topic_engine_kill_switch_enabled'
          LIMIT 1
        ),
        false
      ) as "killSwitchEnabled"
  `;
  const adminTimezone = cleanText(timezoneRows?.[0]?.adminTimezone || "America/New_York", 100);
  const killSwitchEnabled = Boolean(timezoneRows?.[0]?.killSwitchEnabled);
  if (killSwitchEnabled) return [];

  const dueRows = await sql`
    SELECT DISTINCT ON (q.persona_id)
      q.id,
      q.signal_id as "signalId",
      q.persona_id as "personaId"
    FROM topic_engine_release_queue q
    WHERE q.status IN ('queued', 'deferred')
      AND q.scheduled_for_utc IS NOT NULL
      AND q.scheduled_for_utc <= NOW()
    ORDER BY q.persona_id ASC, q.scheduled_for_utc ASC
    LIMIT ${limit}
  `;

  const clock = await loadLocalClock(sql, adminTimezone);
  let globalReleasedToday = 0;
  const personaReleasedToday = new Map<string, number>();
  const releasedCountRows = await sql`
    SELECT
      persona_id as "personaId",
      COUNT(*)::int as "releasedCount"
    FROM topic_engine_release_queue
    WHERE status = 'released'
      AND released_day_local = ${clock.nowLocalDate}::date
    GROUP BY persona_id
  `;
  for (const row of releasedCountRows) {
    const personaId = cleanText(row.personaId, 255);
    const count = Number(row.releasedCount || 0);
    globalReleasedToday += count;
    personaReleasedToday.set(personaId, count);
  }

  const released: Array<{ signalId: number; personaId: string }> = [];
  for (const row of dueRows) {
    const personaId = cleanText(row.personaId, 255);
    const signalId = Number(row.signalId);
    const queueId = cleanText(row.id, 80) || null;
    const config = await loadPacingConfig(sql, personaId);
    const personaReleasedCount = Number(personaReleasedToday.get(personaId) || 0);

    if (config.postsPerActiveDay <= 0) {
      await sql`
        UPDATE topic_engine_release_queue
        SET
          status = 'rejected',
          reason_code = 'posts_per_active_day_zero',
          updated_at = NOW()
        WHERE id = ${queueId}
          AND status IN ('queued', 'deferred')
      `;
      continue;
    }

    if (
      globalReleasedToday >= config.globalDailyCap ||
      personaReleasedCount >= config.postsPerActiveDay
    ) {
      const deferredSchedule = await buildDeferredSchedule(
        sql,
        personaId,
        config,
        clock.nowLocalDate,
        clock.nowLocalTime,
        queueId
      );
      if (!deferredSchedule) {
        await sql`
          UPDATE topic_engine_release_queue
          SET
            status = 'rejected',
            reason_code = 'no_available_schedule_slot',
            updated_at = NOW()
          WHERE id = ${queueId}
            AND status IN ('queued', 'deferred')
        `;
        continue;
      }
      await sql`
        UPDATE topic_engine_release_queue
        SET
          status = 'deferred',
          reason_code = ${
            globalReleasedToday >= config.globalDailyCap
              ? "global_daily_cap_recheck"
              : "persona_daily_cap_recheck"
          },
          scheduled_for_utc = ${deferredSchedule.scheduledForUtc},
          scheduled_day_local = ${deferredSchedule.scheduledDayLocal}::date,
          updated_at = NOW()
        WHERE id = ${queueId}
          AND status IN ('queued', 'deferred')
      `;
      continue;
    }

    const releasedSlots = await loadReleasedSlotsForDay(
      sql,
      personaId,
      clock.nowLocalDate,
      config.adminTimezone,
      queueId
    );
    const nowMinutes = parseTimeToMinutes(clock.nowLocalTime, 0);
    const hasSpacingConflict = releasedSlots.some(
      (existing) => Math.abs(existing - nowMinutes) < config.minSpacingMinutes
    );
    if (hasSpacingConflict) {
      const deferredSchedule = await buildDeferredSchedule(
        sql,
        personaId,
        config,
        clock.nowLocalDate,
        clock.nowLocalTime,
        queueId
      );
      if (!deferredSchedule) {
        await sql`
          UPDATE topic_engine_release_queue
          SET
            status = 'rejected',
            reason_code = 'no_available_schedule_slot',
            updated_at = NOW()
          WHERE id = ${queueId}
            AND status IN ('queued', 'deferred')
        `;
        continue;
      }
      await sql`
        UPDATE topic_engine_release_queue
        SET
          status = 'deferred',
          reason_code = 'min_spacing_recheck',
          scheduled_for_utc = ${deferredSchedule.scheduledForUtc},
          scheduled_day_local = ${deferredSchedule.scheduledDayLocal}::date,
          updated_at = NOW()
        WHERE id = ${queueId}
          AND status IN ('queued', 'deferred')
      `;
      continue;
    }

    const updated = await sql`
      UPDATE topic_engine_release_queue
      SET
        status = 'released',
        released_at = NOW(),
        released_day_local = (NOW() AT TIME ZONE ${adminTimezone})::date,
        updated_at = NOW()
      WHERE id = ${row.id}
        AND status IN ('queued', 'deferred')
      RETURNING signal_id as "signalId", persona_id as "personaId"
    `;
    if (updated[0]) {
      released.push({
        signalId: Number(updated[0].signalId),
        personaId: cleanText(updated[0].personaId, 255)
      });
      globalReleasedToday += 1;
      personaReleasedToday.set(personaId, personaReleasedCount + 1);
    }
  }
  return released;
}

function getDefaultSearchPlan(signal: ResearchSignalContext): SearchPlan {
  const title = cleanText(signal.title, 220);
  const sourceName = cleanText(signal.sourceName || "", 120);
  const snippetTerms = cleanText(signal.snippet || "", 240).split(/\s+/).slice(0, 8).join(" ");
  return {
    passes: [
      {
        passType: "strict_official",
        intent: "Find official confirmation of the signal.",
        optimizedQueries: uniqueQueries([
          `${title} official announcement`,
          sourceName ? `${title} ${sourceName} official` : ""
        ]).slice(0, 2)
      },
      {
        passType: "broad_context",
        intent: "Find local community context and reporting around the signal.",
        optimizedQueries: uniqueQueries([
          `${title} local coverage`,
          snippetTerms ? `${title} ${snippetTerms}` : ""
        ]).slice(0, 2)
      },
      {
        passType: "historical_background",
        intent: "Find historical or background context relevant to the signal.",
        optimizedQueries: uniqueQueries([
          `${title} history`,
          `${title} timeline`
        ]).slice(0, 2)
      }
    ]
  };
}

async function loadResearchSignalContext(signalId: number): Promise<ResearchSignalContext> {
  if (isTestSignalId(signalId)) {
    return {
      id: TEST_SIGNAL_ID,
      personaId: "dayton-local",
      personaSection: "local",
      personaBeat: "general-local",
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
      s.id,
      s.persona_id as "personaId",
      COALESCE(NULLIF(trim(to_jsonb(p)->>'section'), ''), 'local') as "personaSection",
      COALESCE(NULLIF(trim(to_jsonb(p)->>'beat'), ''), '') as "personaBeat",
      s.title,
      s.snippet,
      s.source_name as "sourceName",
      s.source_url as "sourceUrl",
      s.event_key as "eventKey",
      s.dedupe_key as "dedupeKey"
    FROM topic_signals s
    LEFT JOIN personas p
      ON p.id = s.persona_id
    WHERE s.id = ${signalId}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row) throw new Error(`Signal ${signalId} not found`);
  return {
    id: Number(row.id),
    personaId: cleanText(row.personaId, 255),
    personaSection: cleanText(row.personaSection, 120).toLowerCase() || "local",
    personaBeat: cleanText(row.personaBeat, 120).toLowerCase() || "general-local",
    title: cleanText(row.title, 500),
    snippet: cleanText(row.snippet || "", 4000) || null,
    sourceName: cleanText(row.sourceName || "", 500) || null,
    sourceUrl: cleanText(row.sourceUrl || "", 2000) || null,
    eventKey: cleanText(row.eventKey || "", 500) || null,
    dedupeKey: cleanText(row.dedupeKey || "", 500) || null
  };
}

function normalizeSearchPlanPassType(value: unknown): ResearchPassType | null {
  const key = cleanText(value, 80).toLowerCase();
  return RESEARCH_PASS_TYPE_ALIASES[key] || null;
}

function normalizeSearchPlanPass(rawPass: any, fallbackPassType: ResearchPassType): SearchPlanPass {
  const passType = normalizeSearchPlanPassType(rawPass?.passType || rawPass?.type || rawPass?.name) || fallbackPassType;
  const queries = uniqueQueries(
    (Array.isArray(rawPass?.optimized_queries) ? rawPass.optimized_queries : rawPass?.optimizedQueries)
      ?.map((value: unknown) => cleanText(value, 220))
      || []
  ).slice(0, 2);
  return {
    passType,
    intent: cleanText(rawPass?.intent || "", 280),
    optimizedQueries: queries
  };
}

function parseAndValidateSearchPlan(raw: unknown): SearchPlan | null {
  const payload = toSafeJsonObject(raw);
  const container = toSafeJsonObject(payload.search_plan || payload.searchPlan || payload);
  const passes = RESEARCH_PLAN_PASS_ORDER.map((passType) => {
    const aliasKey = passType === "strict_official"
      ? "pass_1_strict"
      : passType === "broad_context"
        ? "pass_2_broad"
        : "pass_3_historical";
    const normalized = normalizeSearchPlanPass(container[passType] || container[aliasKey], passType);
    if (!normalized.intent) {
      normalized.intent = passType === "strict_official"
        ? "Find official confirmation of the signal."
        : passType === "broad_context"
          ? "Find local context and reporting around the signal."
          : "Find background and precedent for the signal.";
    }
    return normalized;
  });
  for (const pass of passes) {
    const seen = new Set<string>();
    pass.optimizedQueries = pass.optimizedQueries.filter((query) => {
      const key = query.toLowerCase();
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    }).slice(0, 2);
  }
  if (passes.some((pass) => pass.optimizedQueries.length < 1)) return null;
  return { passes };
}

function buildRetryableResearchError(message: string): Error & { retryable: true } {
  const error = new Error(message) as Error & { retryable: true };
  error.retryable = true;
  return error;
}

function buildResearchSearchPlanPrompt(signal: ResearchSignalContext, guidanceBundle: StagePromptBundle): string {
  return [
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion
      ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}`
      : "",
    "You are planning a newsroom research strategy for a promoted signal.",
    "Return strict JSON only.",
    "Do not include domains, dates, site: operators, or time windows in queries.",
    "Provide exactly three passes in this schema:",
    "{\"search_plan\":{\"pass_1_strict\":{\"intent\":\"...\",\"optimized_queries\":[\"...\",\"...\"]},\"pass_2_broad\":{\"intent\":\"...\",\"optimized_queries\":[\"...\",\"...\"]},\"pass_3_historical\":{\"intent\":\"...\",\"optimized_queries\":[\"...\",\"...\"]}}}",
    "Rules:",
    "- pass_1_strict: official confirmation queries.",
    "- pass_2_broad: local reporting and context queries.",
    "- pass_3_historical: background and precedent queries.",
    "- 1 to 2 optimized queries per pass.",
    "- Prefer local and persona-relevant framing.",
    "- No commentary, no markdown, no extra keys.",
    "",
    `Persona Section: ${signal.personaSection}`,
    `Persona Beat: ${signal.personaBeat}`,
    `Title: ${signal.title}`,
    `Snippet: ${signal.snippet || ""}`,
    `Source Name: ${signal.sourceName || ""}`,
    `Source URL: ${signal.sourceUrl || ""}`
  ].filter(Boolean).join("\n");
}

async function generateSearchPlan(
  signal: ResearchSignalContext,
  guidanceBundle: StagePromptBundle
): Promise<SearchPlanGenerationResult> {
  if (isTestSignalId(signal.id)) {
    return {
      searchPlan: getDefaultSearchPlan(signal),
      provider: "test",
      model: "research-search-plan-test-v1"
    };
  }

  const geminiApiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  const openAiApiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
  const prompt = buildResearchSearchPlanPrompt(signal, guidanceBundle);
  let hadRetryableProviderFailure = false;

  const tryGemini = async (): Promise<SearchPlanGenerationResult | null> => {
    if (!geminiApiKey) return null;
    const model = HARD_CODED_RESEARCH_QUERY_MODEL;
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
      model
    )}:generateContent?key=${encodeURIComponent(geminiApiKey)}`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.2,
          maxOutputTokens: 1400,
          responseMimeType: "application/json"
        }
      })
    });
    if (!response.ok) {
      hadRetryableProviderFailure = true;
      return null;
    }
    const data = await response.json();
    const text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
    const searchPlan = parseAndValidateSearchPlan(safeJsonParse(text));
    return searchPlan ? { searchPlan, provider: "gemini", model } : null;
  };

  const tryOpenAi = async (): Promise<SearchPlanGenerationResult | null> => {
    if (!openAiApiKey) return null;
    const model = HARD_CODED_GATEKEEPER_OPENAI_MODEL;
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${openAiApiKey}`
      },
      body: JSON.stringify({
        model,
        temperature: 0.2,
        response_format: { type: "json_object" },
        messages: [{ role: "user", content: prompt }]
      })
    });
    if (!response.ok) {
      hadRetryableProviderFailure = true;
      return null;
    }
    const data = await response.json();
    const text = data?.choices?.[0]?.message?.content || "";
    const searchPlan = parseAndValidateSearchPlan(safeJsonParse(text));
    return searchPlan ? { searchPlan, provider: "openai", model } : null;
  };

  const geminiResult = await tryGemini().catch(() => {
    hadRetryableProviderFailure = true;
    return null;
  });
  if (geminiResult) return geminiResult;
  const openAiResult = await tryOpenAi().catch(() => {
    hadRetryableProviderFailure = true;
    return null;
  });
  if (openAiResult) return openAiResult;
  if (hadRetryableProviderFailure || (!geminiApiKey && !openAiApiKey)) {
    throw buildRetryableResearchError("research_search_plan_provider_unavailable");
  }
  throw new Error("research_search_plan_generation_failed");
}

function normalizeResearchTrustTier(value: unknown): ResearchTrustEntry["trustTier"] {
  const normalized = cleanText(value, 40).toLowerCase();
  if (normalized === "official" || normalized === "local_news" || normalized === "contextual") return normalized;
  return "trusted";
}

function normalizeResearchTrustDomain(value: unknown): string {
  return cleanText(value, 255)
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/\/.*$/, "")
    .replace(/^\.+|\.+$/g, "");
}

function buildDefaultResearchTrustEntries(signal: ResearchSignalContext): ResearchTrustEntry[] {
  const defaults: ResearchTrustEntry[] = (SECTION_TRUSTED_RESEARCH_DOMAINS[signal.personaSection] || []).map((entry, index) => ({
    personaId: null,
    section: signal.personaSection,
    beat: null,
    domain: normalizeResearchTrustDomain(entry.domain),
    trustTier: normalizeResearchTrustTier(entry.trustTier),
    isOfficial: entry.isOfficial === true || entry.trustTier === "official",
    priority: Number.isFinite(Number(entry.priority)) ? Number(entry.priority) : (index + 1) * 10,
    enabled: true,
    source: "default" as const
  }));
  const signalDomain = normalizeResearchTrustDomain(parseSourceDomain(signal.sourceUrl || "") || "");
  if (signalDomain) {
    defaults.push({
      personaId: signal.personaId || null,
      section: signal.personaSection,
      beat: signal.personaBeat || null,
      domain: signalDomain,
      trustTier: "trusted",
      isOfficial: false,
      priority: 90,
      enabled: true,
      source: "signal"
    });
  }
  return defaults.filter((entry) => entry.domain);
}

async function loadResearchTrustEntries(sql: any, signal: ResearchSignalContext): Promise<ResearchTrustEntry[]> {
  const defaults = buildDefaultResearchTrustEntries(signal);
  try {
    const rows = await sql`
      SELECT
        persona_id as "personaId",
        NULLIF(trim(section), '') as section,
        NULLIF(trim(beat), '') as beat,
        domain,
        trust_tier as "trustTier",
        is_official as "isOfficial",
        priority,
        enabled
      FROM topic_engine_research_trust
      WHERE enabled = TRUE
        AND (
          persona_id = ${signal.personaId}
          OR persona_id IS NULL
          OR trim(persona_id) = ''
        )
        AND (
          section IS NULL
          OR trim(section) = ''
          OR lower(trim(section)) = ${signal.personaSection}
        )
        AND (
          beat IS NULL
          OR trim(beat) = ''
          OR lower(trim(beat)) = ${signal.personaBeat}
        )
      ORDER BY
        CASE
          WHEN persona_id = ${signal.personaId} THEN 0
          ELSE 1
        END,
        priority ASC,
        id ASC
    `;

    const merged = [...defaults];
    const seen = new Set(defaults.map((entry) => entry.domain));
    for (const row of rows) {
      const domain = normalizeResearchTrustDomain(row.domain);
      if (!domain) continue;
      const entry: ResearchTrustEntry = {
        personaId: cleanText(row.personaId || "", 255) || null,
        section: cleanText(row.section || "", 120).toLowerCase() || null,
        beat: cleanText(row.beat || "", 120).toLowerCase() || null,
        domain,
        trustTier: normalizeResearchTrustTier(row.trustTier),
        isOfficial: row.isOfficial === true || normalizeResearchTrustTier(row.trustTier) === "official",
        priority: Number.isFinite(Number(row.priority)) ? Number(row.priority) : 100,
        enabled: row.enabled !== false,
        source: cleanText(row.personaId || "", 255) ? "persona" : "default"
      };
      const existingIndex = merged.findIndex((item) => item.domain === domain);
      if (existingIndex >= 0) {
        const existing = merged[existingIndex];
        const existingPriorityRank = existing.source === "persona" ? 3 : existing.source === "signal" ? 2 : 1;
        const incomingPriorityRank = entry.source === "persona" ? 3 : entry.source === "signal" ? 2 : 1;
        if (incomingPriorityRank >= existingPriorityRank) {
          merged[existingIndex] = entry;
        }
      } else if (!seen.has(domain)) {
        merged.push(entry);
      }
      seen.add(domain);
    }
    return merged
      .filter((entry) => entry.enabled !== false && entry.domain)
      .sort((a, b) => a.priority - b.priority || a.domain.localeCompare(b.domain));
  } catch (error: any) {
    if (!String(error?.message || "").toLowerCase().includes("topic_engine_research_trust")) throw error;
    return defaults.sort((a, b) => a.priority - b.priority || a.domain.localeCompare(b.domain));
  }
}

function summarizeTrustProfile(entries: ResearchTrustEntry[]): ResearchPassPolicy["trustProfileSummary"] {
  return {
    totalDomains: entries.length,
    officialDomains: entries.filter((entry) => entry.isOfficial || entry.trustTier === "official").length,
    localNewsDomains: entries.filter((entry) => entry.trustTier === "local_news").length,
    contextualDomains: entries.filter((entry) => entry.trustTier === "contextual").length,
    sources: Array.from(new Set(entries.map((entry) => entry.source)))
  };
}

function resolveTrustedResearchDomains(entries: ResearchTrustEntry[], passType: ResearchPassType): string[] {
  const prioritized = [...entries].sort((a, b) => a.priority - b.priority || a.domain.localeCompare(b.domain));
  if (passType === "broad_context") return [];
  if (passType === "strict_official") {
    const strict = prioritized.filter((entry) => entry.isOfficial || entry.trustTier === "official" || entry.trustTier === "local_news");
    const selected = strict.length ? strict : prioritized.filter((entry) => entry.trustTier !== "contextual");
    return Array.from(new Set(selected.map((entry) => entry.domain)));
  }
  return Array.from(new Set(
    prioritized
      .filter((entry) => entry.trustTier !== "contextual" || entry.isOfficial)
      .map((entry) => entry.domain)
  ));
}

function buildPassExecutionPolicy(
  signal: ResearchSignalContext,
  passType: ResearchPassType,
  trustEntries: ResearchTrustEntry[]
): ResearchPassPolicy {
  const trustProfileSummary = summarizeTrustProfile(trustEntries);
  const trustedDomains = resolveTrustedResearchDomains(trustEntries, passType);
  if (passType === "strict_official") {
    return {
      passType,
      includeDomains: trustedDomains,
      maxAgeDays: 2,
      minUsableArtifacts: 2,
      minDistinctDomains: 1,
      appliedTrustTier: trustEntries.some((entry) => entry.isOfficial || entry.trustTier === "official")
        ? "official"
        : "local_news",
      trustProfileSummary
    };
  }
  if (passType === "broad_context") {
    return {
      passType,
      includeDomains: [],
      maxAgeDays: 2,
      minUsableArtifacts: 2,
      minDistinctDomains: 2,
      appliedTrustTier: "contextual",
      trustProfileSummary
    };
  }
  return {
    passType,
    includeDomains: trustedDomains,
    maxAgeDays: null,
    minUsableArtifacts: 1,
    minDistinctDomains: 1,
    appliedTrustTier: "trusted",
    trustProfileSummary
  };
}

function isWithinAgeWindow(publishedAt: string | null, maxAgeDays: number | null): boolean {
  if (maxAgeDays == null) return true;
  if (!publishedAt) return false;
  const published = new Date(publishedAt);
  if (Number.isNaN(published.getTime())) return false;
  return Date.now() - published.getTime() <= maxAgeDays * 24 * 60 * 60 * 1000;
}

function urlMatchesTrustedDomains(url: string, includeDomains: string[]): boolean {
  if (!includeDomains.length) return true;
  const hostname = cleanText(parseSourceDomain(url) || "", 255).toLowerCase();
  if (!hostname) return false;
  return includeDomains.some((domain) => hostname === domain || hostname.endsWith(`.${domain}`));
}

function isUsableResearchResult(result: TavilyResult): boolean {
  const content = cleanText(result.content || result.rawContent || "", 18000);
  return Boolean(cleanText(result.url, 2000) && content.length >= 140);
}

async function searchTavily(
  query: string,
  options?: { includeDomains?: string[]; maxAgeDays?: number | null }
): Promise<TavilyResult[]> {
  const apiKey = cleanText(process.env.TAVILY_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing TAVILY_API_KEY");
  const includeDomains = Array.isArray(options?.includeDomains)
    ? options.includeDomains.map((value) => cleanText(value, 255).toLowerCase()).filter(Boolean)
    : [];
  const maxAgeDays = Number.isFinite(Number(options?.maxAgeDays)) ? Number(options?.maxAgeDays) : null;

  const response = await fetch("https://api.tavily.com/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      api_key: apiKey,
      query,
      search_depth: "advanced",
      include_raw_content: true,
      ...(includeDomains.length ? { include_domains: includeDomains } : {}),
      ...(maxAgeDays != null ? { days: maxAgeDays } : {}),
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
  }).filter((item: TavilyResult) =>
    urlMatchesTrustedDomains(item.url, includeDomains) && isWithinAgeWindow(item.publishedAt, maxAgeDays)
  );
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
  allResultCount: number,
  context?: {
    runId?: string;
    planProvider?: string;
    planModel?: string;
    passType?: ResearchPassType;
    passIndex?: number;
    appliedDomains?: string[];
    appliedMaxAgeDays?: number | null;
    appliedTrustTier?: string | null;
    trustProfileSummary?: ResearchPassPolicy["trustProfileSummary"] | null;
    runStatus?: ResearchRunStatus;
    degradationReason?: string | null;
  }
): Promise<void> {
  if (!selected.length) return;

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const runId = cleanText(context?.runId || "", 120) || randomUUID();
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
      searchPlanProvider: cleanText(context?.planProvider || "", 80) || null,
      searchPlanModel: cleanText(context?.planModel || "", 160) || null,
      passType: context?.passType || null,
      passIndex: Number.isFinite(Number(context?.passIndex)) ? Number(context?.passIndex) : null,
      appliedDomains: Array.isArray(context?.appliedDomains) ? context?.appliedDomains.slice(0, 20) : [],
      appliedMaxAgeDays: context?.appliedMaxAgeDays ?? null,
      appliedTrustTier: cleanText(context?.appliedTrustTier || "", 80) || null,
      trustProfileSummary: context?.trustProfileSummary || null,
      runStatus: context?.runStatus || null,
      degradationReason: cleanText(context?.degradationReason || "", 240) || null,
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

async function persistResearchSearchPlanArtifact(payload: {
  signal: ResearchSignalContext;
  runId: string;
  provider: string;
  model: string;
  searchPlan: SearchPlan;
  status: ResearchRunStatus;
  successfulPassType: ResearchPassType | null;
  degradationReason: string | null;
  failureReason: string | null;
  passSummaries: ExecutedResearchPass[];
  trustProfileSummary?: ResearchPassPolicy["trustProfileSummary"] | null;
}): Promise<number> {
  if (isTestSignalId(payload.signal.id)) return 1;
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const sourceUrl = `signal://${payload.signal.id}/search-plan/${payload.runId}`;
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
    VALUES (
      ${randomUUID()},
      ${payload.runId},
      ${randomUUID()},
      ${randomUUID()},
      ${payload.signal.id},
      ${payload.signal.personaId || null},
      'research_discovery',
      'search_plan',
      ${sourceUrl},
      ${null},
      ${`Research search plan for signal ${payload.signal.id}`},
      ${null},
      ${JSON.stringify(payload.searchPlan)},
      ${toSafeJsonObject({
        signalId: payload.signal.id,
        personaId: payload.signal.personaId,
        provider: payload.provider,
        model: payload.model,
        status: payload.status,
        successfulPassType: payload.successfulPassType,
        degradationReason: payload.degradationReason,
        failureReason: payload.failureReason,
        trustProfileSummary: payload.trustProfileSummary || null,
        passSummaries: payload.passSummaries.map((pass, index) => ({
          passType: pass.passType,
          passIndex: index + 1,
          intent: pass.intent,
          optimizedQueries: pass.optimizedQueries,
          appliedDomains: pass.appliedDomains,
          appliedMaxAgeDays: pass.appliedMaxAgeDays,
          appliedTrustTier: pass.appliedTrustTier || null,
          trustProfileSummary: pass.trustProfileSummary || null,
          fetchedResultCount: pass.fetchedResultCount,
          usableResultCount: pass.usableResultCount,
          distinctDomainCount: pass.distinctDomainCount,
          sufficiencyMet: pass.sufficiencyMet
        })),
        searchPlan: payload.searchPlan
      })}::jsonb,
      NOW()
    )
    RETURNING id
  `;
  return rows.length;
}

function evaluateResearchPassSufficiency(pass: ExecutedResearchPass, policy: ResearchPassPolicy): boolean {
  return pass.usableResultCount >= policy.minUsableArtifacts && pass.distinctDomainCount >= policy.minDistinctDomains;
}

async function executeSearchPass(
  signal: ResearchSignalContext,
  pass: SearchPlanPass,
  trustEntries: ResearchTrustEntry[]
): Promise<ExecutedResearchPass> {
  const policy = buildPassExecutionPolicy(signal, pass.passType, trustEntries);
  const allResults: TavilyResult[] = [];
  for (const query of pass.optimizedQueries) {
    const results = await searchTavily(query, {
      includeDomains: policy.includeDomains,
      maxAgeDays: policy.maxAgeDays
    });
    allResults.push(...results);
  }
  const selected = selectTopResults(allResults.filter(isUsableResearchResult));
  const distinctDomainCount = new Set(
    selected.map((item) => cleanText(parseSourceDomain(item.url) || "", 255)).filter(Boolean)
  ).size;
  const executed: ExecutedResearchPass = {
    passType: pass.passType,
    intent: pass.intent,
    optimizedQueries: pass.optimizedQueries,
    appliedDomains: policy.includeDomains,
    appliedMaxAgeDays: policy.maxAgeDays,
    appliedTrustTier: policy.appliedTrustTier,
    trustProfileSummary: policy.trustProfileSummary,
    fetchedResultCount: allResults.length,
    usableResultCount: selected.length,
    distinctDomainCount,
    selected,
    sufficiencyMet: false
  };
  executed.sufficiencyMet = evaluateResearchPassSufficiency(executed, policy);
  return executed;
}

async function runResearchDiscovery(signalId: number): Promise<ResearchDiscoveryResult> {
  const signal = await loadResearchSignalContext(signalId);
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const guidanceBundle = await buildStageGuidanceBundle(
    sql,
    "research_discovery",
    signal.personaId,
    signal.personaSection
  );
  const trustEntries = await loadResearchTrustEntries(sql, signal);
  const runId = randomUUID();

  let planResult: SearchPlanGenerationResult;
  try {
    planResult = await generateSearchPlan(signal, guidanceBundle);
  } catch (error: any) {
    const failureReason = cleanText(error?.message || "research_search_plan_generation_failed", 240) || "research_search_plan_generation_failed";
    const fallbackPlan = getDefaultSearchPlan(signal);
    await persistResearchSearchPlanArtifact({
      signal,
      runId,
      provider: "none",
      model: "none",
      searchPlan: fallbackPlan,
      status: "failed",
      successfulPassType: null,
      degradationReason: null,
      failureReason,
      passSummaries: [],
      trustProfileSummary: summarizeTrustProfile(trustEntries)
    });
    if (error?.retryable === true) {
      throw error;
    }
    return {
      queries: [],
      saved: 0,
      fetched: 0,
      status: "failed",
      provider: "none",
      model: "none",
      successfulPassType: null,
      degradationReason: null,
      failureReason,
      passSummaries: []
    };
  }

  const executedPasses: ExecutedResearchPass[] = [];
  let successfulPass: ExecutedResearchPass | null = null;
  try {
    for (const pass of planResult.searchPlan.passes) {
      const executedPass = await executeSearchPass(signal, pass, trustEntries);
      executedPasses.push(executedPass);
      if (executedPass.sufficiencyMet) {
        successfulPass = executedPass;
        break;
      }
    }
  } catch (error: any) {
    const failureReason = cleanText(error?.message || "research_retrieval_failed", 240) || "research_retrieval_failed";
    await persistResearchSearchPlanArtifact({
      signal,
      runId,
      provider: planResult.provider,
      model: planResult.model,
      searchPlan: planResult.searchPlan,
      status: "failed",
      successfulPassType: null,
      degradationReason: null,
      failureReason,
      passSummaries: executedPasses,
      trustProfileSummary: summarizeTrustProfile(trustEntries)
    });
    throw buildRetryableResearchError(failureReason);
  }

  const status: ResearchRunStatus = !successfulPass
    ? "failed"
    : successfulPass.passType === "strict_official"
      ? "completed"
      : "degraded";
  const degradationReason = status === "degraded"
    ? (successfulPass?.passType === "broad_context" ? "used_broad_context_pass" : "used_historical_background_pass")
    : null;
  const failureReason = status === "failed" ? "insufficient_research_evidence" : null;
  const queries = executedPasses.flatMap((pass) => pass.optimizedQueries);
  const fetched = executedPasses.reduce((sum, pass) => sum + pass.fetchedResultCount, 0);
  const selectedResults = successfulPass?.selected || [];

  if (selectedResults.length) {
    await persistResearchArtifacts(signal, queries, selectedResults, fetched, {
      runId,
      planProvider: planResult.provider,
      planModel: planResult.model,
      passType: successfulPass?.passType || undefined,
      passIndex: successfulPass ? executedPasses.findIndex((pass) => pass.passType === successfulPass.passType) + 1 : undefined,
      appliedDomains: successfulPass?.appliedDomains || [],
      appliedMaxAgeDays: successfulPass?.appliedMaxAgeDays ?? null,
      appliedTrustTier: successfulPass?.appliedTrustTier ?? null,
      trustProfileSummary: successfulPass?.trustProfileSummary || summarizeTrustProfile(trustEntries),
      runStatus: status,
      degradationReason
    });
  }

  await persistResearchSearchPlanArtifact({
    signal,
    runId,
    provider: planResult.provider,
    model: planResult.model,
    searchPlan: planResult.searchPlan,
    status,
    successfulPassType: successfulPass?.passType || null,
    degradationReason,
    failureReason,
    passSummaries: executedPasses,
    trustProfileSummary: summarizeTrustProfile(trustEntries)
  });

  return {
    queries,
    saved: selectedResults.length,
    fetched,
    status,
    provider: planResult.provider,
    model: planResult.model,
    successfulPassType: successfulPass?.passType || null,
    degradationReason,
    failureReason,
    passSummaries: executedPasses.map((pass) => ({
      passType: pass.passType,
      fetchedResultCount: pass.fetchedResultCount,
      usableResultCount: pass.usableResultCount,
      distinctDomainCount: pass.distinctDomainCount,
      appliedDomains: pass.appliedDomains,
      appliedMaxAgeDays: pass.appliedMaxAgeDays,
      appliedTrustTier: pass.appliedTrustTier,
      sufficiencyMet: pass.sufficiencyMet
    }))
  };
}

async function loadEvidenceSources(signalId: number): Promise<EvidenceSource[]> {
  if (isTestSignalId(signalId)) {
    return [
      {
        sourceUrl: "https://example.com/mock-signal-12345",
        title: "City announces weekend downtown detours",
        content: "Dayton public works said lane closures start Saturday morning and detours will be posted.",
        score: 0.92,
        sourceType: "official",
        publishedAt: "2026-03-10T12:00:00Z",
        sourceDomain: "example.com"
      },
      {
        sourceUrl: "https://example.com/mock-signal-12345-traffic",
        title: "Transit agency updates downtown service map",
        content: "RTA said two routes will shift stops near utility work zones for safety through Sunday night.",
        score: 0.88,
        sourceType: "official",
        publishedAt: "2026-03-10T12:05:00Z",
        sourceDomain: "example.com"
      }
    ];
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const rows = await sql`
    SELECT
      source_url as "sourceUrl",
      source_domain as "sourceDomain",
      title,
      content,
      published_at as "publishedAt",
      COALESCE(NULLIF(trim(COALESCE(metadata->>'appliedTrustTier', '')), ''), '') as "sourceType",
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
      sourceDomain: cleanText(row.sourceDomain || "", 255) || parseSourceDomain(cleanText(row.sourceUrl, 2000)),
      title: cleanText(row.title || "", 600) || null,
      content: cleanText(row.content || "", 12000) || null,
      score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0,
      sourceType: normalizeStoryPlanningEvidenceSourceType(row.sourceType),
      publishedAt: cleanText(row.publishedAt || "", 120) || null
    }))
    .filter((row: EvidenceSource) => row.sourceUrl);
}

function clampConfidence(value: unknown): number {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0.5;
  return Math.max(0, Math.min(1, num));
}

function normalizeEvidenceExtractionStatus(value: unknown): EvidenceExtractionArtifact["extractionStatus"] {
  const normalized = cleanText(value, 40).toUpperCase();
  if (normalized === "READY" || normalized === "NEEDS_REPORTING") return normalized;
  return "";
}

function normalizeStringArray(value: unknown, maxItemLength = 240, maxItems = 8): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((item) => cleanText(item, maxItemLength)).filter(Boolean).slice(0, maxItems);
}

function tokenizeComparableText(value: string): string[] {
  return cleanText(value, 5000)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((token) => token.length >= 3);
}

function buildEvidenceFingerprint(value: string): string {
  const stopWords = new Set([
    "the", "and", "for", "with", "that", "this", "from", "will", "have", "has", "said", "after", "before", "their",
    "about", "into", "near", "more", "than", "when", "where", "which", "while", "over", "under", "through"
  ]);
  return Array.from(new Set(tokenizeComparableText(value).filter((token) => !stopWords.has(token)))).slice(0, 12).join(" ");
}

function computeTokenOverlapRatio(a: string, b: string): number {
  const tokensA = Array.from(new Set(tokenizeComparableText(a)));
  const tokensB = new Set(tokenizeComparableText(b));
  if (!tokensA.length || !tokensB.size) return 0;
  let matches = 0;
  for (const token of tokensA) {
    if (tokensB.has(token)) matches += 1;
  }
  return matches / tokensA.length;
}

function isQuoteInspectableAgainstSource(quote: string, sourceContent: string | null): boolean {
  if (!quote || !sourceContent) return false;
  const normalizedQuote = cleanText(quote, 5000).toLowerCase();
  const normalizedContent = cleanText(sourceContent, 20000).toLowerCase();
  if (!normalizedQuote || !normalizedContent) return false;
  if (normalizedContent.includes(normalizedQuote)) return true;
  return computeTokenOverlapRatio(quote, sourceContent) >= 0.7;
}

function hasSpecificity(value: string): boolean {
  const vaguePatterns = [/\bissue\b/i, /\bsituation\b/i, /\bdevelopment\b/i, /\bimportant\b/i, /\bongoing story\b/i];
  const tokenCount = tokenizeComparableText(value).length;
  return tokenCount >= 5 && !vaguePatterns.some((pattern) => pattern.test(value));
}

function hasPlanningValue(item: EvidenceCandidate): boolean {
  if (!item.whyItMatters) return false;
  if (tokenizeComparableText(item.whyItMatters).length < 4) return false;
  return /(affect|impact|change|close|start|end|route|resident|customer|student|driver|business|official|timeline|next|uncertain|investigat|cost|fee|service|safety|public)/i.test(
    `${item.claim} ${item.whyItMatters}`
  );
}

function classifyCoverageContribution(item: EvidenceCandidate): Set<string> {
  const text = `${item.claim} ${item.whyItMatters}`.toLowerCase();
  const out = new Set<string>();
  if (
    /(announce|close|closure|open|reopen|launch|start|begin|end|approve|reject|arrest|charge|file|vote|pass|settle|merge|cut|expand|shift|change|update|cancel|resume|suspend|investigat|report|confirm|deny)/.test(
      text
    )
  ) {
    out.add("core_event");
  }
  if (/(start|begin|end|through|until|monday|tuesday|wednesday|thursday|friday|saturday|sunday|today|tonight|morning|afternoon|evening|week|month|year)/.test(text)) out.add("timing");
  if (/(affect|impact|resident|rider|commuter|student|family|business|customer|community|driver|taxpayer)/.test(text)) out.add("impact");
  if (/(will|expected|next|plan|hearing|meeting|vote|continue|resume|follow)/.test(text)) out.add("what_next");
  if (/(official|agency|city|police|school|company|judge|court|department|board)/.test(text)) out.add("actors");
  if (/(unknown|uncertain|investigat|not clear|contested|conflict|contradict|unresolved)/.test(text)) out.add("uncertainty");
  if (/\b\d[\d,\.]*\b/.test(text)) out.add("specifics");
  return out;
}

function violatesScopeContainment(item: EvidenceCandidate): boolean {
  const claim = item.claim.toLowerCase();
  const quote = item.evidenceQuote.toLowerCase();
  const causalTerms = ["because", "due to", "as a result", "caused by", "in response to"];
  if (causalTerms.some((term) => claim.includes(term) && !quote.includes(term))) return true;
  const absoluteTerms = ["always", "never", "all", "every", "completely", "proved"];
  if (absoluteTerms.some((term) => claim.includes(term) && !quote.includes(term))) return true;
  return false;
}

function violatesTemporalGrounding(item: EvidenceCandidate, source: EvidenceSource): boolean {
  const text = `${item.claim} ${item.whyItMatters}`.toLowerCase();
  const timeSensitive = /(today|tonight|currently|now|ongoing|still|continues|will remain|expected to)/.test(text);
  return timeSensitive && !source.publishedAt;
}

function extractNumericTokens(value: string): string[] {
  return cleanText(value, 1000).match(/\b\d[\d,\.]*\b/g) || [];
}

function hasNegation(value: string): boolean {
  return /\b(no|not|never|without|denied|deny|refused)\b/i.test(value);
}

function assignSupportStatuses(items: EvidenceCandidate[]): EvidenceSupportStatus[] {
  return items.map((item, index) => {
    const currentFingerprint = buildEvidenceFingerprint(item.claim);
    let corroborated = false;
    let contested = false;
    for (let otherIndex = 0; otherIndex < items.length; otherIndex += 1) {
      if (otherIndex === index) continue;
      const other = items[otherIndex];
      if (item.sourceUrl === other.sourceUrl) continue;
      const overlap = computeTokenOverlapRatio(item.claim, other.claim);
      const sameTopic = overlap >= 0.55 || currentFingerprint === buildEvidenceFingerprint(other.claim);
      if (!sameTopic) continue;
      const currentNumbers = extractNumericTokens(item.claim);
      const otherNumbers = extractNumericTokens(other.claim);
      const conflictingNumbers = currentNumbers.length > 0 && otherNumbers.length > 0 && currentNumbers.join("|") !== otherNumbers.join("|");
      const conflictingNegation = hasNegation(item.claim) !== hasNegation(other.claim);
      if (conflictingNumbers || conflictingNegation) contested = true;
      else corroborated = true;
    }
    if (contested) return "contested";
    if (corroborated) return "corroborated";
    return "single_source";
  });
}

function normalizeEvidenceExtractionArtifact(raw: unknown, sourcesByUrl: Map<string, EvidenceSource>): EvidenceExtractionArtifact {
  const out: EvidenceCandidate[] = [];
  const seen = new Set<string>();
  const rawItems = Array.isArray((raw as any)?.evidenceItems)
    ? (raw as any).evidenceItems
    : Array.isArray((raw as any)?.claims)
      ? (raw as any).claims
      : [];

  for (const item of rawItems) {
    const normalizedItem: EvidenceCandidate = {
      claim: cleanText(item?.claim || "", 600),
      sourceUrl: cleanText(item?.sourceUrl || "", 2000),
      evidenceQuote: cleanText(item?.evidenceQuote || "", 320),
      confidence: clampConfidence(item?.confidence),
      whyItMatters: cleanText(item?.whyItMatters || "", 500)
    };
    const dedupeKey = [
      normalizedItem.sourceUrl.toLowerCase(),
      buildEvidenceFingerprint(normalizedItem.claim),
      buildEvidenceFingerprint(normalizedItem.evidenceQuote)
    ].join("|");
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    out.push(normalizedItem);
    if (out.length >= 8) break;
  }

  const supportStatuses = assignSupportStatuses(out);
  const evidenceItems: JudgedEvidenceItem[] = out.map((item, index) => {
    const source = sourcesByUrl.get(item.sourceUrl.toLowerCase()) || null;
    return {
      ...item,
      sourceType: source?.sourceType || "other",
      publishedAt: source?.publishedAt || null,
      sourceDomain: source?.sourceDomain || parseSourceDomain(item.sourceUrl),
      supportStatus: supportStatuses[index] || "single_source"
    };
  });

  return {
    extractionStatus: normalizeEvidenceExtractionStatus((raw as any)?.extractionStatus),
    decisionRationale: cleanText((raw as any)?.decisionRationale || "", 800),
    evidenceItems,
    editorialRisks: normalizeStringArray((raw as any)?.editorialRisks, 120, 8),
    missingEvidence: normalizeStringArray((raw as any)?.missingEvidence, 240, 8),
    followUpQueries: normalizeStringArray((raw as any)?.followUpQueries, 240, 8)
  };
}

function validateEvidenceExtractionArtifact(
  artifact: EvidenceExtractionArtifact,
  _signal: ResearchSignalContext,
  sources: EvidenceSource[]
): void {
  const contractFailures: string[] = [];
  const editorialFailures: string[] = [];
  const sourcesByUrl = new Map(sources.map((source) => [source.sourceUrl.toLowerCase(), source]));
  const distinctUrls = new Set<string>();
  const coverage = new Set<string>();
  let planningUsefulCount = 0;
  let nonContestedCount = 0;

  if (artifact.extractionStatus !== "READY" && artifact.extractionStatus !== "NEEDS_REPORTING") {
    contractFailures.push("extractionStatus must be READY or NEEDS_REPORTING");
  }
  if (!artifact.decisionRationale) {
    contractFailures.push("decisionRationale is required");
  }

  for (const item of artifact.evidenceItems) {
    const source = sourcesByUrl.get(item.sourceUrl.toLowerCase());
    if (!item.claim) contractFailures.push("every evidence item must include claim");
    if (!item.sourceUrl) contractFailures.push("every evidence item must include sourceUrl");
    if (!item.evidenceQuote) contractFailures.push("every evidence item must include evidenceQuote");
    if (!item.whyItMatters) contractFailures.push("every evidence item must include whyItMatters");
    if (!source) {
      contractFailures.push(`evidence item sourceUrl must match an approved Phase 2 source: ${item.sourceUrl || "missing"}`);
      continue;
    }
    distinctUrls.add(item.sourceUrl);
    if (item.sourceType !== source.sourceType) {
      contractFailures.push(`sourceType must be inherited from the matched source for ${item.sourceUrl}`);
    }
    if ((item.publishedAt || null) !== (source.publishedAt || null)) {
      contractFailures.push(`publishedAt must be inherited from the matched source for ${item.sourceUrl}`);
    }
    if (!isQuoteInspectableAgainstSource(item.evidenceQuote, source.content)) {
      contractFailures.push(`evidenceQuote is not inspectably grounded for ${item.sourceUrl}`);
    }
    if (!hasSpecificity(item.claim)) {
      editorialFailures.push(`claim is too vague to be planning-useful for ${item.sourceUrl}`);
    }
    if (!hasPlanningValue(item)) {
      editorialFailures.push(`claim lacks clear story relevance for ${item.sourceUrl}`);
    } else {
      planningUsefulCount += 1;
    }
    if (violatesScopeContainment(item)) {
      editorialFailures.push(`claim overreaches the supporting evidence for ${item.sourceUrl}`);
    }
    if (violatesTemporalGrounding(item, source)) {
      editorialFailures.push(`claim is not safely time-bounded for ${item.sourceUrl}`);
    }
    if (item.supportStatus !== "contested") nonContestedCount += 1;
    for (const dimension of classifyCoverageContribution(item)) coverage.add(dimension);
  }

  if (artifact.extractionStatus === "READY") {
    if (artifact.evidenceItems.length < 3) editorialFailures.push("READY requires at least 3 evidence items");
    if (distinctUrls.size < 2) editorialFailures.push("READY requires evidence from at least 2 distinct source URLs");
    if (planningUsefulCount < 3) editorialFailures.push("READY requires at least 3 planning-useful evidence items");
    if (!coverage.has("core_event")) editorialFailures.push("READY requires a clearly grounded core event/change");
    if (![coverage.has("impact"), coverage.has("timing"), coverage.has("what_next")].some(Boolean)) {
      editorialFailures.push("READY requires coverage beyond the core event, such as impact, timing, or what happens next");
    }
    if (nonContestedCount === 0) editorialFailures.push("READY cannot rely entirely on contested evidence");
  }

  if (artifact.extractionStatus === "NEEDS_REPORTING") {
    if (!artifact.missingEvidence.length) contractFailures.push("NEEDS_REPORTING requires missingEvidence");
    if (!artifact.followUpQueries.length) contractFailures.push("NEEDS_REPORTING requires followUpQueries");
  } else {
    if (artifact.followUpQueries.length) {
      contractFailures.push("READY must not include followUpQueries");
    }
    if (artifact.missingEvidence.length) {
      editorialFailures.push("READY must not retain unresolved missingEvidence");
    }
  }

  if (contractFailures.length) {
    throw new EvidenceExtractionValidationError("contract_invalid", Array.from(new Set(contractFailures)));
  }
  if (editorialFailures.length) {
    throw new EvidenceExtractionValidationError("editorial_insufficiency", Array.from(new Set(editorialFailures)));
  }
}

function buildEvidenceExtractionPrompt(
  signal: ResearchSignalContext,
  sources: EvidenceSource[],
  guidanceBundle: StagePromptBundle
): string {
  const sourceContext = sources
    .slice(0, 5)
    .map((source, index) =>
      [
        `Source ${index + 1}:`,
        `URL: ${source.sourceUrl}`,
        `Title: ${source.title || ""}`,
        `Source Type: ${source.sourceType}`,
        `Published At: ${source.publishedAt || "unknown"}`,
        `Score: ${source.score}`,
        `Content: ${cleanText(source.content || "", 5000)}`
      ].join("\n")
    )
    .join("\n\n");
  return [
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}` : "",
    "You are the evidence adjudication engine for an autonomous newsroom pipeline.",
    "Use only the provided sources.",
    "Your job is to preserve only facts that are source-grounded and useful for story understanding.",
    "Do not use outside knowledge. Do not generalize beyond the evidence. Do not invent causality, chronology, or certainty.",
    "Return strict JSON only in this schema:",
    "{\"extractionStatus\":\"READY|NEEDS_REPORTING\",\"decisionRationale\":\"...\",\"evidenceItems\":[{\"claim\":\"...\",\"sourceUrl\":\"...\",\"evidenceQuote\":\"...\",\"confidence\":0.0,\"whyItMatters\":\"...\"}],\"editorialRisks\":[\"...\"],\"missingEvidence\":[\"...\"],\"followUpQueries\":[\"...\"]}",
    "Rules:",
    "- sourceUrl must exactly match one provided URL.",
    "- Keep only materially distinct, planning-useful evidence items.",
    "- Preserve inspectable source language in evidenceQuote.",
    "- Do not overstate, broaden, or infer beyond the source.",
    "- READY means the evidence picture is mature enough for planning.",
    "- NEEDS_REPORTING means the story may be viable but evidence maturity is insufficient; include specific missingEvidence and followUpQueries.",
    "- Return 2 to 8 evidenceItems max.",
    "- No markdown. No extra keys.",
    "",
    `Persona ID: ${signal.personaId}`,
    `Section: ${signal.personaSection}`,
    `Beat: ${signal.personaBeat}`,
    `Signal Title: ${signal.title}`,
    `Signal Snippet: ${signal.snippet || ""}`,
    "",
    sourceContext
  ].filter(Boolean).join("\n");
}

function buildEvidenceExtractionRepairPrompt(originalPrompt: string, rawText: string, failureReasons: string[]): string {
  return [
    "Repair the evidence-adjudication response so it matches the required JSON contract exactly.",
    "Do not invent facts, source URLs, or evidence quotes.",
    "If the evidence is not strong enough for READY, explicitly change extractionStatus to NEEDS_REPORTING and provide missingEvidence plus followUpQueries.",
    "Return strict JSON only.",
    `Validation failures: ${failureReasons.map((reason) => cleanText(reason, 240)).join(" | ")}`,
    "",
    "Original instructions:",
    originalPrompt,
    "",
    "Previous model output to repair:",
    cleanText(rawText, 24000)
  ].join("\n");
}

async function extractEvidenceClaimsWithGemini(prompt: string): Promise<StoryPlannerExecutionResult> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new EvidenceExtractionProviderError("Missing GEMINI_API_KEY");

  const candidateModels = [...HARD_CODED_EVIDENCE_MODEL_CANDIDATES];
  let lastError = "Gemini evidence extraction failed";
  for (const model of candidateModels) {
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.2,
          maxOutputTokens: 2200,
          responseMimeType: "application/json"
        }
      })
    });
    if (response.ok) {
      const data = await response.json();
      const text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
      if (text) {
        return {
          provider: "gemini",
          model,
          rawText: text,
          parsed: safeJsonParse(text)
        };
      }
      lastError = `Gemini evidence extraction returned empty content for model ${model}`;
      continue;
    }
    const body = await response.text();
    lastError = `Gemini evidence extraction failed for model ${model} ${response.status}: ${body.slice(0, 200)}`;
  }
  throw new EvidenceExtractionProviderError(lastError);
}

async function executeEvidenceExtractionWithValidation(
  signal: ResearchSignalContext,
  sources: EvidenceSource[],
  guidanceBundle: StagePromptBundle
): Promise<{ execution: StoryPlannerExecutionResult; artifact: EvidenceExtractionArtifact; validation: EvidenceExtractionValidationResult }> {
  const prompt = buildEvidenceExtractionPrompt(signal, sources, guidanceBundle);
  const sourcesByUrl = new Map(sources.map((source) => [source.sourceUrl.toLowerCase(), source]));
  const firstExecution = await extractEvidenceClaimsWithGemini(prompt);
  const firstArtifact = normalizeEvidenceExtractionArtifact(firstExecution.parsed, sourcesByUrl);
  try {
    validateEvidenceExtractionArtifact(firstArtifact, signal, sources);
    return {
      execution: firstExecution,
      artifact: firstArtifact,
      validation: {
        repaired: false,
        repairReason: null,
        executionOutcome: "validated",
        failureReasons: []
      }
    };
  } catch (error: any) {
    if (!(error instanceof EvidenceExtractionValidationError)) throw error;
    const repairPrompt = buildEvidenceExtractionRepairPrompt(prompt, firstExecution.rawText, error.failureReasons);
    const repairedExecution = await extractEvidenceClaimsWithGemini(repairPrompt);
    const repairedArtifact = normalizeEvidenceExtractionArtifact(repairedExecution.parsed, sourcesByUrl);
    try {
      validateEvidenceExtractionArtifact(repairedArtifact, signal, sources);
      return {
        execution: repairedExecution,
        artifact: repairedArtifact,
        validation: {
          repaired: true,
          repairReason: error.failureReasons.join(" | ").slice(0, 500),
          executionOutcome: "validated",
          failureReasons: []
        }
      };
    } catch (repairError: any) {
      if (repairError instanceof EvidenceExtractionValidationError) {
        throw new EvidenceExtractionValidationError("repair_failed", repairError.failureReasons);
      }
      throw repairError;
    }
  }
}

function materializePersistedEvidenceItems(items: JudgedEvidenceItem[]): PersistedEvidenceItem[] {
  return items.map((item) => ({
    evidenceId: randomUUID(),
    ...item
  }));
}

async function persistEvidenceArtifacts(
  signal: ResearchSignalContext,
  claims: PersistedEvidenceItem[],
  context: {
    runId: string;
    version: number;
    isCanonical: boolean;
  }
): Promise<number> {
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

  const engineId = randomUUID();

  let saved = 0;
  for (let index = 0; index < claims.length; index += 1) {
    const claim = claims[index];
    const metadata = {
      signalId: signal.id,
      personaId: signal.personaId,
      rank: index + 1,
      version: context.version,
      confidence: claim.confidence,
      evidenceQuote: claim.evidenceQuote,
      whyItMatters: claim.whyItMatters,
      sourceType: claim.sourceType,
      supportStatus: claim.supportStatus,
      evidenceBundleRunId: context.runId
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
      VALUES (
        ${claim.evidenceId},
        ${context.runId},
        ${engineId},
        ${randomUUID()},
        ${signal.id},
        ${signal.personaId || null},
        'evidence_extraction',
        'evidence_extract',
        ${claim.sourceUrl},
        ${parseSourceDomain(claim.sourceUrl)},
        ${`Evidence claim ${index + 1}`},
        ${claim.publishedAt},
        ${claim.claim},
        ${toSafeJsonObject(metadata)}::jsonb,
        NOW()
      )
      RETURNING id
    `;
    if (rows.length) saved += 1;
  }

  return saved;
}

async function persistEvidenceBundleArtifact(
  signal: ResearchSignalContext,
  options: {
    artifact: EvidenceExtractionArtifact;
    executionOutcome: EvidenceExecutionOutcome;
    failureReasons: string[];
    provider?: string;
    model?: string;
    validation?: EvidenceExtractionValidationResult | null;
    promptHash: string;
    promptSourceVersion: string;
    warnings: string[];
    sourceCount: number;
    persistedEvidenceItems: PersistedEvidenceItem[];
  }
): Promise<EvidenceBundlePersistenceResult & { runId: string; persistedEvidenceItems: PersistedEvidenceItem[] }> {
  if (isTestSignalId(signal.id)) {
    console.log("test-mode persistEvidenceBundleArtifact skip", {
      signalId: signal.id,
      extractionStatus: options.artifact.extractionStatus,
      executionOutcome: options.executionOutcome,
      evidenceCount: options.persistedEvidenceItems.length
    });
    return {
      saved: 1,
      version: 1,
      isCanonical: options.executionOutcome === "validated" && options.artifact.extractionStatus === "READY",
      extractionStatus: options.artifact.extractionStatus,
      executionOutcome: options.executionOutcome,
      runId: "test-evidence-bundle-run",
      persistedEvidenceItems: options.persistedEvidenceItems
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const versionRows = await sql`
    SELECT
      COALESCE(
        MAX(
          CASE
            WHEN COALESCE(metadata->>'version', '') ~ '^[0-9]+$'
              THEN (metadata->>'version')::int
            ELSE 0
          END
        ),
        0
      ) + 1 as version
    FROM research_artifacts
    WHERE signal_id = ${signal.id}
      AND stage = 'evidence_extraction'
      AND artifact_type = 'evidence_bundle'
  `;
  const version = Math.max(1, Number(versionRows?.[0]?.version || 1));
  const isCanonical = options.executionOutcome === "validated" && options.artifact.extractionStatus === "READY";
  const runId = randomUUID();
  const insertedId = randomUUID();
  const sourceUrl = `signal://${signal.id}/evidence-bundle`;
  const metadata = {
    signalId: signal.id,
    personaId: signal.personaId,
    provider: cleanText(options.provider || "", 80) || null,
    model: cleanText(options.model || "", 160) || null,
    version,
    isCanonical,
    extractionStatus: options.artifact.extractionStatus,
    executionOutcome: options.executionOutcome,
    failureReasons: options.failureReasons,
    repaired: options.validation?.repaired === true,
    repairReason: cleanText(options.validation?.repairReason || "", 500) || null,
    promptHash: cleanText(options.promptHash, 120),
    promptSourceVersion: cleanText(options.promptSourceVersion, 120),
    warnings: Array.isArray(options.warnings) ? options.warnings.slice(0, 20) : [],
    sourceCount: options.sourceCount,
    evidenceCount: options.persistedEvidenceItems.length,
    editorialRiskCount: options.artifact.editorialRisks.length,
    missingEvidenceCount: options.artifact.missingEvidence.length,
    evidenceBundle: {
      ...options.artifact,
      evidenceItems: options.persistedEvidenceItems
    }
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
    VALUES (
      ${insertedId},
      ${runId},
      ${randomUUID()},
      ${randomUUID()},
      ${signal.id},
      ${signal.personaId || null},
      'evidence_extraction',
      'evidence_bundle',
      ${sourceUrl},
      ${null},
      ${`Evidence bundle v${version} for signal ${signal.id}`},
      ${null},
      ${options.artifact.decisionRationale || `Evidence extraction ${options.executionOutcome}`},
      ${toSafeJsonObject(metadata)}::jsonb,
      NOW()
    )
    RETURNING id
  `;

  if (rows.length && isCanonical) {
    await sql`
      UPDATE research_artifacts
      SET metadata = jsonb_set(
        CASE
          WHEN jsonb_typeof(metadata) = 'object' THEN metadata
          ELSE '{}'::jsonb
        END,
        '{isCanonical}',
        CASE
          WHEN id = ${insertedId} THEN 'true'::jsonb
          ELSE 'false'::jsonb
        END,
        true
      )
      WHERE signal_id = ${signal.id}
        AND stage = 'evidence_extraction'
        AND artifact_type = 'evidence_bundle'
        AND (
          id = ${insertedId}
          OR COALESCE(metadata->>'isCanonical', 'false') = 'true'
        )
    `;
  }

  return {
    saved: rows.length,
    version,
    isCanonical,
    extractionStatus: options.artifact.extractionStatus,
    executionOutcome: options.executionOutcome,
    runId,
    persistedEvidenceItems: options.persistedEvidenceItems
  };
}

async function runEvidenceExtraction(signalId: number): Promise<{
  sourceCount: number;
  candidateCount: number;
  evidenceCount: number;
  saved: number;
  version?: number;
  isCanonical?: boolean;
  extractionStatus: EvidenceExtractionArtifact["extractionStatus"];
  executionOutcome: EvidenceExecutionOutcome;
  decisionRationale: string;
  editorialRisks: string[];
  missingEvidence: string[];
  followUpQueries: string[];
  provider?: string;
  model?: string;
  repaired?: boolean;
  failureReasons?: string[];
  skipped?: boolean;
}> {
  const signal = await loadResearchSignalContext(signalId);
  const sources = await loadEvidenceSources(signalId);
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  if (!sources.length) {
    const emptyArtifact: EvidenceExtractionArtifact = {
      extractionStatus: "NEEDS_REPORTING",
      decisionRationale: "Phase 3 could not adjudicate evidence because Phase 2 did not provide any approved sources.",
      evidenceItems: [],
      editorialRisks: ["no_approved_sources"],
      missingEvidence: ["Need approved reporting sources before evidence adjudication can establish the story."],
      followUpQueries: ["Find approved reporting sources that directly establish the core event or change."]
    };
    const persisted = await persistEvidenceBundleArtifact(signal, {
      artifact: emptyArtifact,
      executionOutcome: "editorial_insufficiency",
      failureReasons: ["No approved Phase 2 sources were available for evidence adjudication"],
      provider: undefined,
      model: undefined,
      validation: null,
      promptHash: "",
      promptSourceVersion: "",
      warnings: [],
      sourceCount: 0,
      persistedEvidenceItems: []
    });
    return {
      sourceCount: 0,
      candidateCount: 0,
      evidenceCount: 0,
      saved: persisted.saved,
      version: persisted.version,
      isCanonical: persisted.isCanonical,
      extractionStatus: emptyArtifact.extractionStatus,
      executionOutcome: persisted.executionOutcome,
      decisionRationale: emptyArtifact.decisionRationale,
      editorialRisks: emptyArtifact.editorialRisks,
      missingEvidence: emptyArtifact.missingEvidence,
      followUpQueries: emptyArtifact.followUpQueries,
      skipped: true
    };
  }

  const guidanceBundle = await buildStageGuidanceBundle(
    sql,
    "evidence_extraction",
    signal.personaId,
    signal.personaSection
  );
  try {
    const result = await executeEvidenceExtractionWithValidation(signal, sources, guidanceBundle);
    const persistedEvidenceItems = materializePersistedEvidenceItems(result.artifact.evidenceItems);
    let persistedBundle: Awaited<ReturnType<typeof persistEvidenceBundleArtifact>>;
    let savedItems = 0;
    try {
      persistedBundle = await persistEvidenceBundleArtifact(signal, {
        artifact: result.artifact,
        executionOutcome: result.validation.executionOutcome,
        failureReasons: result.validation.failureReasons,
        provider: result.execution.provider,
        model: result.execution.model,
        validation: result.validation,
        promptHash: guidanceBundle.promptHash,
        promptSourceVersion: guidanceBundle.promptSourceVersion,
        warnings: guidanceBundle.warnings,
        sourceCount: sources.length,
        persistedEvidenceItems
      });
      savedItems = await persistEvidenceArtifacts(signal, persistedBundle.persistedEvidenceItems, {
        runId: persistedBundle.runId,
        version: persistedBundle.version,
        isCanonical: persistedBundle.isCanonical
      });
    } catch (persistError: any) {
      return {
        sourceCount: sources.length,
        candidateCount: result.artifact.evidenceItems.length,
        evidenceCount: result.artifact.evidenceItems.length,
        saved: 0,
        extractionStatus: result.artifact.extractionStatus,
        executionOutcome: "persistence_failed",
        decisionRationale: result.artifact.decisionRationale,
        editorialRisks: result.artifact.editorialRisks,
        missingEvidence: result.artifact.missingEvidence,
        followUpQueries: result.artifact.followUpQueries,
        provider: result.execution.provider,
        model: result.execution.model,
        repaired: result.validation.repaired,
        failureReasons: [cleanText(persistError?.message || "evidence_persistence_failed", 500)]
      };
    }
    return {
      sourceCount: sources.length,
      candidateCount: result.artifact.evidenceItems.length,
      evidenceCount: result.artifact.evidenceItems.length,
      saved: persistedBundle.saved + savedItems,
      version: persistedBundle.version,
      isCanonical: persistedBundle.isCanonical,
      extractionStatus: result.artifact.extractionStatus,
      executionOutcome: persistedBundle.executionOutcome,
      decisionRationale: result.artifact.decisionRationale,
      editorialRisks: result.artifact.editorialRisks,
      missingEvidence: result.artifact.missingEvidence,
      followUpQueries: result.artifact.followUpQueries,
      provider: result.execution.provider,
      model: result.execution.model,
      repaired: result.validation.repaired
    };
  } catch (error: any) {
    const executionOutcome: Exclude<EvidenceExecutionOutcome, "validated" | "persistence_failed"> =
      error instanceof EvidenceExtractionValidationError
        ? error.executionOutcome
        : error instanceof EvidenceExtractionProviderError
          ? error.executionOutcome
          : "provider_failure";
    const failureReasons =
      error instanceof EvidenceExtractionValidationError
        ? error.failureReasons
        : [cleanText(error?.message || "evidence_extraction_failed", 500)];
    const failedArtifact: EvidenceExtractionArtifact = {
      extractionStatus: "",
      decisionRationale: "",
      evidenceItems: [],
      editorialRisks: [],
      missingEvidence: [],
      followUpQueries: []
    };
    const persistedFailure = await persistEvidenceBundleArtifact(signal, {
      artifact: failedArtifact,
      executionOutcome,
      failureReasons,
      provider: undefined,
      model: undefined,
      validation: null,
      promptHash: guidanceBundle.promptHash,
      promptSourceVersion: guidanceBundle.promptSourceVersion,
      warnings: guidanceBundle.warnings,
      sourceCount: sources.length,
      persistedEvidenceItems: []
    });
    return {
      sourceCount: sources.length,
      candidateCount: 0,
      evidenceCount: 0,
      saved: persistedFailure.saved,
      version: persistedFailure.version,
      isCanonical: persistedFailure.isCanonical,
      extractionStatus: "",
      executionOutcome: persistedFailure.executionOutcome,
      decisionRationale: "",
      editorialRisks: [],
      missingEvidence: [],
      followUpQueries: [],
      failureReasons
    };
  }
}

function normalizePersistedEvidenceItem(raw: any): StoryPlanningEvidence | null {
  const evidenceId = cleanText(raw?.evidenceId || "", 120);
  const claim = cleanText(raw?.claim || "", 1200);
  const sourceUrl = cleanText(raw?.sourceUrl || "", 2000);
  const evidenceQuote = cleanText(raw?.evidenceQuote || "", 400);
  if (!evidenceId || !claim || !sourceUrl || !evidenceQuote) return null;
  return {
    evidenceId,
    claim,
    sourceUrl,
    sourceType: normalizeStoryPlanningEvidenceSourceType(raw?.sourceType),
    evidenceQuote,
    confidence: clampConfidence(raw?.confidence),
    publishedAt: cleanText(raw?.publishedAt || "", 120) || null,
    whyItMatters: cleanText(raw?.whyItMatters || "", 700)
  };
}

async function loadLatestEvidenceBundleResult(signalId: number): Promise<EvidenceBundleLoadResult | null> {
  if (isTestSignalId(signalId)) {
    const testEvidenceItems = [
      {
        evidenceId: "ev_mock_1",
        claim: "Downtown lane closures begin Saturday morning and continue through Sunday night.",
        sourceUrl: "https://example.com/mock-signal-12345",
        sourceType: "official",
        evidenceQuote: "Dayton public works said lane closures start Saturday morning.",
        confidence: 0.89,
        publishedAt: "2026-03-10T12:00:00Z",
        whyItMatters: "Weekend drivers and businesses downtown will need detour plans.",
        sourceDomain: "example.com",
        supportStatus: "single_source"
      },
      {
        evidenceId: "ev_mock_2",
        claim: "RTA is shifting stops for two routes near utility work zones.",
        sourceUrl: "https://example.com/mock-signal-12345-traffic",
        sourceType: "official",
        evidenceQuote: "RTA said two routes will shift stops near utility work zones.",
        confidence: 0.84,
        publishedAt: "2026-03-10T12:05:00Z",
        whyItMatters: "Transit riders need updated stop locations and timing expectations.",
        sourceDomain: "example.com",
        supportStatus: "single_source"
      }
    ] as any;
    return {
      artifact: {
        extractionStatus: "READY",
        decisionRationale: "Mock evidence bundle is planning-ready.",
        evidenceItems: testEvidenceItems,
        editorialRisks: [],
        missingEvidence: [],
        followUpQueries: []
      },
      version: 1,
      isCanonical: true,
      executionOutcome: "validated",
      failureReasons: []
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const sourceUrl = `signal://${signalId}/evidence-bundle`;
  const rows = await sql`
    SELECT metadata
    FROM research_artifacts
    WHERE signal_id = ${signalId}
      AND stage = 'evidence_extraction'
      AND artifact_type = 'evidence_bundle'
      AND source_url = ${sourceUrl}
    ORDER BY
      CASE
        WHEN COALESCE(metadata->>'version', '') ~ '^[0-9]+$'
          THEN (metadata->>'version')::int
        ELSE 0
      END DESC,
      created_at DESC
    LIMIT 1
  `;

  const metadata = toSafeJsonObject(rows?.[0]?.metadata);
  if (!Object.keys(metadata).length) return null;
  const rawBundle = toSafeJsonObject(metadata.evidenceBundle);
  const rawItems = Array.isArray(rawBundle.evidenceItems) ? rawBundle.evidenceItems : [];
  const evidenceItems = rawItems
    .map((item: any) => normalizePersistedEvidenceItem(item))
    .filter(Boolean) as StoryPlanningEvidence[];
  return {
    artifact: {
      extractionStatus: normalizeEvidenceExtractionStatus(metadata.extractionStatus || rawBundle.extractionStatus),
      decisionRationale: cleanText(rawBundle.decisionRationale || metadata.decisionRationale || "", 800),
      evidenceItems: evidenceItems as any,
      editorialRisks: normalizeStringArray(rawBundle.editorialRisks, 120, 8),
      missingEvidence: normalizeStringArray(rawBundle.missingEvidence, 240, 8),
      followUpQueries: normalizeStringArray(rawBundle.followUpQueries, 240, 8)
    },
    version:
      cleanText(metadata.version, 20) && Number.isFinite(Number(metadata.version))
        ? Number(metadata.version)
        : 0,
    isCanonical: metadata.isCanonical === true,
    executionOutcome: cleanText(metadata.executionOutcome, 40) as EvidenceExecutionOutcome | "",
    failureReasons: Array.isArray(metadata.failureReasons)
      ? metadata.failureReasons.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 12)
      : []
  };
}

async function loadStoryPlanningEvidence(signalId: number): Promise<StoryPlanningEvidence[]> {
  const latestBundle = await loadLatestEvidenceBundleResult(signalId);
  if (latestBundle) {
    if (latestBundle.isCanonical && latestBundle.executionOutcome === "validated" && latestBundle.artifact.extractionStatus === "READY") {
      return latestBundle.artifact.evidenceItems
        .map((item: any) => normalizePersistedEvidenceItem(item))
        .filter(Boolean) as StoryPlanningEvidence[];
    }
    return [];
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const rows = await sql`
    SELECT
      id as "evidenceId",
      content as "claim",
      source_url as "sourceUrl",
      COALESCE(NULLIF(trim(COALESCE(metadata->>'sourceType', '')), ''), '') as "sourceType",
      COALESCE(metadata->>'evidenceQuote', '') as "evidenceQuote",
      COALESCE(metadata->>'whyItMatters', '') as "whyItMatters",
      published_at as "publishedAt",
      CASE
        WHEN COALESCE(metadata->>'confidence', '') ~ '^[0-9]+(\\.[0-9]+)?$'
          THEN (metadata->>'confidence')::numeric
        ELSE 0.5
      END as "confidence"
    FROM research_artifacts
    WHERE signal_id = ${signalId}
      AND stage = 'evidence_extraction'
      AND artifact_type = 'evidence_extract'
      AND source_url IS NOT NULL
    ORDER BY "confidence" DESC, created_at DESC
    LIMIT 8
  `;

  return rows
    .map((row: any) => ({
      evidenceId: cleanText(row.evidenceId || "", 120),
      claim: cleanText(row.claim || "", 1200),
      sourceUrl: cleanText(row.sourceUrl || "", 2000),
      sourceType: normalizeStoryPlanningEvidenceSourceType(row.sourceType),
      evidenceQuote: cleanText(row.evidenceQuote || "", 400),
      confidence: clampConfidence(row.confidence),
      publishedAt: cleanText(row.publishedAt || "", 120) || null,
      whyItMatters: cleanText(row.whyItMatters || "", 700)
    }))
    .filter((item: StoryPlanningEvidence) => item.evidenceId && item.claim && item.sourceUrl && item.evidenceQuote);
}

function normalizeUrlList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of value) {
    const cleaned = cleanText(raw, 2000);
    if (!cleaned) continue;
    const key = cleaned.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(cleaned);
    if (out.length >= 12) break;
  }
  return out;
}

function normalizeStoryPlanningStatus(value: unknown): StoryPlanningStatus | "" {
  const normalized = cleanText(value, 40).toUpperCase();
  if (normalized === "READY" || normalized === "NEEDS_REPORTING" || normalized === "REJECTED") {
    return normalized;
  }
  return "";
}

function normalizeStoryPlanSectionPurpose(value: unknown, index: number): StoryPlanSectionPurpose | "" {
  const normalized = cleanText(value, 40).toLowerCase();
  if (
    normalized === "lead" ||
    normalized === "nut_graph" ||
    normalized === "context" ||
    normalized === "impact" ||
    normalized === "what_next" ||
    normalized === "uncertainty"
  ) {
    return normalized;
  }
  return "" as "";
}

function normalizeStoryPlanAssertiveness(value: unknown): StoryPlanAssertiveness | "" {
  const normalized = cleanText(value, 20).toLowerCase();
  if (normalized === "high" || normalized === "medium" || normalized === "low") return normalized;
  return "";
}

function normalizeStoryPlanningEvidenceSourceType(value: unknown): StoryPlanningEvidence["sourceType"] {
  const normalized = cleanText(value, 40).toLowerCase();
  if (
    normalized === "official" ||
    normalized === "local_news" ||
    normalized === "wire" ||
    normalized === "secondary"
  ) {
    return normalized;
  }
  return "other";
}

function normalizeEvidenceIdList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of value) {
    const cleaned = cleanText(raw, 120);
    if (!cleaned || seen.has(cleaned)) continue;
    seen.add(cleaned);
    out.push(cleaned);
    if (out.length >= 12) break;
  }
  return out;
}

function normalizePersonaFit(value: unknown): StoryPlanArtifact["personaFitAssessment"]["fit"] {
  const normalized = cleanText(value, 20).toLowerCase();
  if (normalized === "strong" || normalized === "medium" || normalized === "weak") return normalized;
  return "";
}

function normalizeStoryPlan(
  raw: any,
  evidence: StoryPlanningEvidence[]
): StoryPlanArtifact {
  const sectionsRaw = Array.isArray(raw?.sections) ? raw.sections : [];
  const sections: StoryPlanSection[] = [];

  for (let index = 0; index < sectionsRaw.length; index += 1) {
    const item = sectionsRaw[index];
    const heading = cleanText(item?.heading || "", 180);
    const summary = cleanText(item?.summary || "", 700);
    const evidenceSourceUrls = normalizeUrlList(item?.evidenceSourceUrls).slice(0, 8);
    const evidenceIds = normalizeEvidenceIdList(item?.evidenceIds).slice(0, 8);
    const priorityRaw = cleanText(item?.priority, 20);
    const parsedPriority = Number.parseInt(priorityRaw, 10);
    sections.push({
      sectionId: cleanText(item?.sectionId || "", 80),
      heading,
      summary,
      purpose: normalizeStoryPlanSectionPurpose(item?.purpose, index),
      evidenceIds,
      evidenceSourceUrls,
      assertiveness: normalizeStoryPlanAssertiveness(item?.assertiveness),
      tensionFlags: Array.isArray(item?.tensionFlags)
        ? item.tensionFlags.map((entry: unknown) => cleanText(entry, 120)).filter(Boolean).slice(0, 8)
        : [],
      qualificationNotes: Array.isArray(item?.qualificationNotes)
        ? item.qualificationNotes.map((entry: unknown) => cleanText(entry, 240)).filter(Boolean).slice(0, 8)
        : [],
      priority: Number.isInteger(parsedPriority) && parsedPriority > 0 ? parsedPriority : null
    });
    if (sections.length >= 8) break;
  }

  const uncertaintyNotes = Array.isArray(raw?.uncertaintyNotes)
    ? raw.uncertaintyNotes.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 8)
    : [];
  const missingInformation = Array.isArray(raw?.missingInformation)
    ? raw.missingInformation.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 8)
    : [];
  const missingInformationQueries = Array.isArray(raw?.missingInformationQueries)
    ? raw.missingInformationQueries.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 8)
    : [];
  const editorialRisks = Array.isArray(raw?.editorialRisks)
    ? raw.editorialRisks.map((item: unknown) => cleanText(item, 120)).filter(Boolean).slice(0, 8)
    : [];

  const normalizedSections = sections;
  const approvedEvidenceIds = normalizeEvidenceIdList(raw?.approvedEvidenceIds).slice(0, 20);
  const approvedSourceUrls = normalizeUrlList(raw?.approvedSourceUrls).slice(0, 20);
  const primaryEvidenceIds = normalizeEvidenceIdList(raw?.primarySourceAssessment?.primaryEvidenceIds).slice(0, 20);

  return {
    planningStatus: normalizeStoryPlanningStatus(raw?.planningStatus),
    decisionRationale: cleanText(raw?.decisionRationale || "", 600),
    angle: cleanText(raw?.angle || "", 280),
    narrativeStrategy: cleanText(raw?.narrativeStrategy || "", 360),
    approvedEvidenceIds,
    approvedSourceUrls,
    sections: normalizedSections,
    uncertaintyNotes,
    missingInformation,
    missingInformationQueries,
    editorialRisks,
    primarySourceAssessment: {
      hasPrimarySource: raw?.primarySourceAssessment?.hasPrimarySource === true,
      primaryEvidenceIds,
      notes: Array.isArray(raw?.primarySourceAssessment?.notes)
        ? raw.primarySourceAssessment.notes.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 8)
        : []
    },
    personaFitAssessment: {
      fit: normalizePersonaFit(raw?.personaFitAssessment?.fit),
      notes: Array.isArray(raw?.personaFitAssessment?.notes)
        ? raw.personaFitAssessment.notes.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 8)
        : []
    }
  };
}

type StoryPlannerExecutionResult = {
  provider: "openai" | "gemini";
  model: string;
  rawText: string;
  parsed: unknown | null;
};

type EvidenceExtractionValidationResult = {
  repaired: boolean;
  repairReason: string | null;
  executionOutcome: "validated";
  failureReasons: string[];
};

type StoryPlanValidationResult = {
  repaired: boolean;
  repairReason: string | null;
  executionOutcome: "validated";
  failureReasons: string[];
};

type StoryPlanPersistenceResult = {
  saved: number;
  version: number;
  isCanonical: boolean;
  planningStatus: StoryPlanningStatus | "";
  executionOutcome: StoryPlanningExecutionOutcome;
};

type StoryPlanLoadResult = {
  plan: StoryPlanArtifact;
  version: number;
  isCanonical: boolean;
  executionOutcome: StoryPlanningExecutionOutcome | "";
  failureReasons: string[];
};

class EvidenceExtractionValidationError extends Error {
  executionOutcome: Exclude<EvidenceExecutionOutcome, "validated" | "provider_failure" | "persistence_failed">;
  failureReasons: string[];

  constructor(
    executionOutcome: Exclude<EvidenceExecutionOutcome, "validated" | "provider_failure" | "persistence_failed">,
    failureReasons: string[]
  ) {
    super(failureReasons[0] || executionOutcome);
    this.name = "EvidenceExtractionValidationError";
    this.executionOutcome = executionOutcome;
    this.failureReasons = failureReasons;
  }
}

class EvidenceExtractionProviderError extends Error {
  executionOutcome: "provider_failure";

  constructor(message: string) {
    super(message);
    this.name = "EvidenceExtractionProviderError";
    this.executionOutcome = "provider_failure";
  }
}

class StoryPlanValidationError extends Error {
  executionOutcome: Exclude<StoryPlanningExecutionOutcome, "validated" | "provider_failure" | "persistence_failed">;
  failureReasons: string[];

  constructor(
    executionOutcome: Exclude<StoryPlanningExecutionOutcome, "validated" | "provider_failure" | "persistence_failed">,
    failureReasons: string[]
  ) {
    super(failureReasons[0] || executionOutcome);
    this.name = "StoryPlanValidationError";
    this.executionOutcome = executionOutcome;
    this.failureReasons = failureReasons;
  }
}

class StoryPlanProviderError extends Error {
  executionOutcome: "provider_failure";

  constructor(message: string) {
    super(message);
    this.name = "StoryPlanProviderError";
    this.executionOutcome = "provider_failure";
  }
}

function buildStoryPlanningPrompt(
  signal: ResearchSignalContext,
  evidence: StoryPlanningEvidence[],
  guidanceBundle: StagePromptBundle
): string {
  const evidenceContext = evidence
    .slice(0, 8)
    .map((item, index) =>
      [
        `Evidence ${index + 1}:`,
        `Evidence ID: ${item.evidenceId}`,
        `Claim: ${item.claim}`,
        `Source URL: ${item.sourceUrl}`,
        `Source Type: ${item.sourceType}`,
        `Published At: ${item.publishedAt || "unknown"}`,
        `Quote: ${item.evidenceQuote}`,
        `Confidence: ${item.confidence}`,
        `Why it matters: ${item.whyItMatters}`
      ].join("\n")
    )
    .join("\n\n");

  return [
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion
      ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}`
      : "",
    "You are the editorial planning decision engine for an autonomous newsroom pipeline.",
    "Use only the provided evidence items.",
    "Do not use outside knowledge.",
    "Do not invent facts, connective narrative, chronology, or corroboration.",
    "You must explicitly decide whether the story is READY, NEEDS_REPORTING, or REJECTED.",
    "Return strict JSON only in this schema:",
    "{\"planningStatus\":\"READY|NEEDS_REPORTING|REJECTED\",\"decisionRationale\":\"...\",\"angle\":\"...\",\"narrativeStrategy\":\"...\",\"approvedEvidenceIds\":[\"ev_123\"],\"approvedSourceUrls\":[\"https://...\"],\"sections\":[{\"sectionId\":\"lead\",\"heading\":\"...\",\"summary\":\"...\",\"purpose\":\"lead|nut_graph|context|impact|what_next|uncertainty\",\"evidenceIds\":[\"ev_123\"],\"evidenceSourceUrls\":[\"https://...\"],\"assertiveness\":\"high|medium|low\",\"tensionFlags\":[\"...\"],\"qualificationNotes\":[\"...\"],\"priority\":1}],\"uncertaintyNotes\":[\"...\"],\"missingInformation\":[\"...\"],\"missingInformationQueries\":[\"...\"],\"editorialRisks\":[\"...\"],\"primarySourceAssessment\":{\"hasPrimarySource\":true,\"primaryEvidenceIds\":[\"ev_123\"],\"notes\":[\"...\"]},\"personaFitAssessment\":{\"fit\":\"strong|medium|weak\",\"notes\":[\"...\"]}}",
    "Rules:",
    "- Cite exact evidenceIds and exact matching source URLs.",
    "- READY requires a valid draft-ready plan grounded only in approved evidence.",
    "- NEEDS_REPORTING means the story may be viable but evidence is not mature enough; include targeted missingInformationQueries.",
    "- REJECTED means the story should not continue autonomously; include decisionRationale and editorialRisks.",
    "- Do not fabricate approvedEvidenceIds or approvedSourceUrls.",
    "- Downgrade assertiveness when tensions exist.",
    "- No markdown. No extra keys.",
    "",
    `Persona ID: ${signal.personaId}`,
    `Section: ${signal.personaSection}`,
    `Beat: ${signal.personaBeat}`,
    `Signal Title: ${signal.title}`,
    `Signal Snippet: ${signal.snippet || ""}`,
    "",
    evidenceContext
  ]
    .filter(Boolean)
    .join("\n");
}

function buildStoryPlanningRepairPrompt(
  originalPrompt: string,
  rawText: string,
  failureReasons: string[]
): string {
  return [
    "Repair the story-planning response so it matches the required JSON contract exactly.",
    "Do not invent facts or evidence references.",
    "If the plan is not supportable as READY, explicitly change planningStatus to NEEDS_REPORTING or REJECTED.",
    "Return strict JSON only.",
    `Validation failures: ${failureReasons.map((reason) => cleanText(reason, 240)).join(" | ")}`,
    "",
    "Original instructions:",
    originalPrompt,
    "",
    "Previous model output to repair:",
    cleanText(rawText, 24000)
  ].join("\n");
}

function shouldFallbackStoryPlanningProvider(error: unknown): boolean {
  return error instanceof StoryPlanProviderError;
}

function parseStoryPlannerResponse(
  rawText: string,
  provider: "openai" | "gemini",
  model: string
): StoryPlannerExecutionResult {
  return {
    provider,
    model,
    rawText,
    parsed: safeJsonParse(rawText)
  };
}

function validateStoryPlanArtifact(plan: StoryPlanArtifact, evidence: StoryPlanningEvidence[]): void {
  const contractFailures: string[] = [];
  const editorialFailures: string[] = [];
  const evidenceById = new Map(evidence.map((item) => [item.evidenceId, item]));
  const allowedUrls = new Set(evidence.map((item) => item.sourceUrl));
  const hasStatus = plan.planningStatus === "READY" || plan.planningStatus === "NEEDS_REPORTING" || plan.planningStatus === "REJECTED";

  if (!hasStatus) contractFailures.push("planningStatus must be READY, NEEDS_REPORTING, or REJECTED");
  if ((plan.planningStatus === "READY" || plan.planningStatus === "NEEDS_REPORTING") && !plan.angle) {
    contractFailures.push("angle is required for READY and NEEDS_REPORTING");
  }
  if ((plan.planningStatus === "NEEDS_REPORTING" || plan.planningStatus === "REJECTED") && !plan.decisionRationale) {
    contractFailures.push("decisionRationale is required for NEEDS_REPORTING and REJECTED");
  }
  if (plan.planningStatus === "REJECTED" && !plan.editorialRisks.length) {
    contractFailures.push("REJECTED must include at least one editorial risk");
  }
  if (!plan.personaFitAssessment.fit) {
    contractFailures.push("personaFitAssessment.fit is required");
  }

  const approvedEvidenceIdSet = new Set(plan.approvedEvidenceIds);
  const approvedSourceUrlSet = new Set(plan.approvedSourceUrls);
  if (plan.approvedEvidenceIds.some((id) => !evidenceById.has(id))) {
    contractFailures.push("approvedEvidenceIds must be a subset of Phase 3 evidence IDs");
  }
  if (plan.approvedSourceUrls.some((url) => !allowedUrls.has(url))) {
    contractFailures.push("approvedSourceUrls must be a subset of Phase 3 evidence source URLs");
  }

  if (plan.planningStatus === "READY") {
    if (!plan.narrativeStrategy) contractFailures.push("READY requires narrativeStrategy");
    if (!plan.sections.length) contractFailures.push("READY requires sections");
    if (!plan.approvedEvidenceIds.length) contractFailures.push("READY requires approvedEvidenceIds");
    if (!plan.approvedSourceUrls.length) contractFailures.push("READY requires approvedSourceUrls");
    if (plan.sections.length < 3 || plan.sections.length > 6) {
      contractFailures.push("READY must include 3 to 6 sections");
    }
    if (plan.approvedEvidenceIds.length < 3) {
      editorialFailures.push("READY requires at least 3 approved evidence IDs");
    }
    if (plan.approvedSourceUrls.length < 2) {
      editorialFailures.push("READY requires at least 2 approved source URLs");
    }
  }

  const priorities = new Set<number>();
  let leadCount = 0;
  let nutGraphCount = 0;
  const sectionEvidenceUniverse = new Set<string>();
  const sectionUrlUniverse = new Set<string>();
  const sectionIds = new Set<string>();

  for (const section of plan.sections) {
    if (!section.sectionId) contractFailures.push("sectionId is required on every section");
    if (section.sectionId && sectionIds.has(section.sectionId)) contractFailures.push("sectionId values must be unique");
    if (section.sectionId) sectionIds.add(section.sectionId);
    if (!section.heading) contractFailures.push("heading is required on every section");
    if (!section.summary) contractFailures.push("summary is required on every section");
    if (!section.purpose) contractFailures.push("purpose is required on every section");
    if (!section.assertiveness) contractFailures.push("assertiveness is required on every section");
    if (!Number.isInteger(section.priority) || Number(section.priority) <= 0) {
      contractFailures.push("priority must be a positive integer on every section");
    } else if (priorities.has(section.priority)) {
      contractFailures.push("section priorities must be unique");
    } else {
      priorities.add(section.priority);
    }

    if (section.purpose === "lead") leadCount += 1;
    if (section.purpose === "nut_graph") nutGraphCount += 1;

    if (section.tensionFlags.length > 0 && !section.qualificationNotes.length) {
      contractFailures.push("sections with tensionFlags must include qualificationNotes");
    }
    if (section.tensionFlags.length > 0 && section.assertiveness !== "low") {
      editorialFailures.push("sections with tensionFlags must use low assertiveness");
    }

    if (section.purpose !== "uncertainty" && section.evidenceIds.length === 0) {
      contractFailures.push(`section ${section.sectionId || section.heading || "unknown"} must include evidenceIds`);
    }
    if (section.purpose !== "uncertainty" && section.evidenceSourceUrls.length === 0) {
      contractFailures.push(`section ${section.sectionId || section.heading || "unknown"} must include evidenceSourceUrls`);
    }
    if (section.purpose === "uncertainty" && section.evidenceIds.length === 0 && section.qualificationNotes.length === 0) {
      contractFailures.push("uncertainty sections without evidenceIds must explain the absence of confirmation");
    }

    const expectedUrls = new Set(
      section.evidenceIds
        .map((evidenceId) => evidenceById.get(evidenceId)?.sourceUrl || "")
        .filter(Boolean)
    );
    for (const evidenceId of section.evidenceIds) {
      if (!evidenceById.has(evidenceId)) {
        contractFailures.push(`section ${section.sectionId || section.heading || "unknown"} references unknown evidenceId ${evidenceId}`);
      } else {
        sectionEvidenceUniverse.add(evidenceId);
      }
    }
    for (const url of section.evidenceSourceUrls) {
      sectionUrlUniverse.add(url);
      if (!allowedUrls.has(url)) {
        contractFailures.push(`section ${section.sectionId || section.heading || "unknown"} references unknown source URL`);
      }
      if (plan.planningStatus === "READY" && !approvedSourceUrlSet.has(url)) {
        contractFailures.push(`section ${section.sectionId || section.heading || "unknown"} uses a source URL outside approvedSourceUrls`);
      }
      if (expectedUrls.size > 0 && !expectedUrls.has(url)) {
        contractFailures.push(`section ${section.sectionId || section.heading || "unknown"} has source URLs that do not match its evidenceIds`);
      }
    }
    for (const evidenceId of section.evidenceIds) {
      if (plan.planningStatus === "READY" && !approvedEvidenceIdSet.has(evidenceId)) {
        contractFailures.push(`section ${section.sectionId || section.heading || "unknown"} uses an evidenceId outside approvedEvidenceIds`);
      }
    }
  }

  if (plan.planningStatus === "READY") {
    if (leadCount < 1) contractFailures.push("READY requires at least one lead section");
    if (nutGraphCount < 1) contractFailures.push("READY requires at least one nut_graph section");
    if (sectionEvidenceUniverse.size <= 1) {
      editorialFailures.push("READY cannot anchor all sections to one evidence ID");
    }
    if (sectionUrlUniverse.size <= 1) {
      editorialFailures.push("READY cannot anchor all sections to one source URL");
    }
  }

  if (plan.planningStatus === "NEEDS_REPORTING") {
    if (!plan.missingInformation.length) {
      contractFailures.push("NEEDS_REPORTING requires missingInformation");
    }
    if (!plan.missingInformationQueries.length) {
      contractFailures.push("NEEDS_REPORTING requires missingInformationQueries");
    }
  }

  if (contractFailures.length) {
    throw new StoryPlanValidationError("contract_invalid", Array.from(new Set(contractFailures)));
  }
  if (editorialFailures.length) {
    throw new StoryPlanValidationError("editorial_insufficiency", Array.from(new Set(editorialFailures)));
  }
}

async function executeStoryPlannerWithValidation(
  prompt: string,
  execute: (promptText: string) => Promise<StoryPlannerExecutionResult>,
  evidence: StoryPlanningEvidence[]
): Promise<{ execution: StoryPlannerExecutionResult; plan: StoryPlanArtifact; validation: StoryPlanValidationResult }> {
  const firstExecution = await execute(prompt);
  const firstPlan = normalizeStoryPlan(firstExecution.parsed, evidence);
  try {
    validateStoryPlanArtifact(firstPlan, evidence);
    return {
      execution: firstExecution,
      plan: firstPlan,
      validation: {
        repaired: false,
        repairReason: null,
        executionOutcome: "validated",
        failureReasons: []
      }
    };
  } catch (error: any) {
    if (!(error instanceof StoryPlanValidationError)) throw error;
    const repairPrompt = buildStoryPlanningRepairPrompt(prompt, firstExecution.rawText, error.failureReasons);
    const repairedExecution = await execute(repairPrompt);
    const repairedPlan = normalizeStoryPlan(repairedExecution.parsed, evidence);
    try {
      validateStoryPlanArtifact(repairedPlan, evidence);
      return {
        execution: repairedExecution,
        plan: repairedPlan,
        validation: {
          repaired: true,
          repairReason: error.failureReasons.join(" | ").slice(0, 500),
          executionOutcome: "validated",
          failureReasons: []
        }
      };
    } catch (repairError: any) {
      if (repairError instanceof StoryPlanValidationError) {
        throw new StoryPlanValidationError("repair_failed", repairError.failureReasons);
      }
      throw repairError;
    }
  }
}

async function buildStoryPlanWithOpenAi(
  signal: ResearchSignalContext,
  evidence: StoryPlanningEvidence[],
  guidanceBundle: StagePromptBundle
): Promise<StoryPlannerExecutionResult> {
  const apiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
  if (!apiKey) throw new StoryPlanProviderError("Missing OPENAI_API_KEY");
  const prompt = buildStoryPlanningPrompt(signal, evidence, guidanceBundle);

  const response = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: HARD_CODED_STORY_PLANNING_OPENAI_MODEL,
      temperature: 0.2,
      response_format: { type: "json_object" },
      messages: [{ role: "user", content: prompt }]
    })
  });

  if (!response.ok) {
    const body = await response.text();
    throw new StoryPlanProviderError(`OpenAI story planning failed ${response.status}: ${body.slice(0, 240)}`);
  }
  const data = await response.json();
  const text = data?.choices?.[0]?.message?.content || "";
  return parseStoryPlannerResponse(text, "openai", HARD_CODED_STORY_PLANNING_OPENAI_MODEL);
}

async function buildStoryPlanWithGemini(
  signal: ResearchSignalContext,
  evidence: StoryPlanningEvidence[],
  guidanceBundle: StagePromptBundle
): Promise<StoryPlannerExecutionResult> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new StoryPlanProviderError("Missing GEMINI_API_KEY");
  const prompt = buildStoryPlanningPrompt(signal, evidence, guidanceBundle);

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    HARD_CODED_STORY_PLANNING_GEMINI_MODEL
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0.2,
        maxOutputTokens: 2200,
        responseMimeType: "application/json"
      }
    })
  });
  if (!response.ok) {
    const body = await response.text();
    throw new StoryPlanProviderError(`Gemini story planning failed ${response.status}: ${body.slice(0, 240)}`);
  }
  const data = await response.json();
  const text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
  return parseStoryPlannerResponse(text, "gemini", HARD_CODED_STORY_PLANNING_GEMINI_MODEL);
}

async function buildStoryPlan(
  signal: ResearchSignalContext,
  evidence: StoryPlanningEvidence[],
  guidanceBundle: StagePromptBundle
): Promise<{
  plan: StoryPlanArtifact;
  provider: "openai" | "gemini";
  model: string;
  validation: StoryPlanValidationResult;
}> {
  const hasOpenAi = Boolean(cleanText(process.env.OPENAI_API_KEY || "", 20));
  const hasGemini = Boolean(cleanText(process.env.GEMINI_API_KEY || "", 20));
  let lastOpenAiError: unknown = null;
  const prompt = buildStoryPlanningPrompt(signal, evidence, guidanceBundle);
  if (hasOpenAi) {
    try {
      const result = await executeStoryPlannerWithValidation(
        prompt,
        async (promptText: string) => {
          const execution = await buildStoryPlanWithOpenAi(signal, evidence, guidanceBundle);
          if (promptText === prompt) return execution;
          const apiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
          const response = await fetch("https://api.openai.com/v1/chat/completions", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
              model: HARD_CODED_STORY_PLANNING_OPENAI_MODEL,
              temperature: 0.2,
              response_format: { type: "json_object" },
              messages: [{ role: "user", content: promptText }]
            })
          });
          if (!response.ok) {
            const body = await response.text();
            throw new StoryPlanProviderError(`OpenAI story planning failed ${response.status}: ${body.slice(0, 240)}`);
          }
          const data = await response.json();
          const text = data?.choices?.[0]?.message?.content || "";
          return parseStoryPlannerResponse(text, "openai", HARD_CODED_STORY_PLANNING_OPENAI_MODEL);
        },
        evidence
      );
      return {
        plan: result.plan,
        provider: "openai",
        model: HARD_CODED_STORY_PLANNING_OPENAI_MODEL,
        validation: result.validation
      };
    } catch (error) {
      lastOpenAiError = error;
      if (!hasGemini || !shouldFallbackStoryPlanningProvider(error)) throw error;
    }
  }
  if (hasGemini) {
    try {
      const result = await executeStoryPlannerWithValidation(
        prompt,
        async (promptText: string) => {
          if (promptText === prompt) {
            return buildStoryPlanWithGemini(signal, evidence, guidanceBundle);
          }
          const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
          const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
            HARD_CODED_STORY_PLANNING_GEMINI_MODEL
          )}:generateContent?key=${encodeURIComponent(apiKey)}`;
          const response = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              contents: [{ parts: [{ text: promptText }] }],
              generationConfig: {
                temperature: 0.2,
                maxOutputTokens: 2200,
                responseMimeType: "application/json"
              }
            })
          });
          if (!response.ok) {
            const body = await response.text();
            throw new StoryPlanProviderError(`Gemini story planning failed ${response.status}: ${body.slice(0, 240)}`);
          }
          const data = await response.json();
          const text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
          return parseStoryPlannerResponse(text, "gemini", HARD_CODED_STORY_PLANNING_GEMINI_MODEL);
        },
        evidence
      );
      return {
        plan: result.plan,
        provider: "gemini",
        model: HARD_CODED_STORY_PLANNING_GEMINI_MODEL,
        validation: result.validation
      };
    } catch (geminiError) {
      // Provider fallback is allowed; deterministic content fallback is not.
      if (hasOpenAi && shouldFallbackStoryPlanningProvider(geminiError)) {
        const result = await executeStoryPlannerWithValidation(
          prompt,
          async (promptText: string) => {
            const apiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
            const response = await fetch("https://api.openai.com/v1/chat/completions", {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${apiKey}`
              },
              body: JSON.stringify({
                model: HARD_CODED_STORY_PLANNING_OPENAI_MODEL,
                temperature: 0.2,
                response_format: { type: "json_object" },
                messages: [{ role: "user", content: promptText }]
              })
            });
            if (!response.ok) {
              const body = await response.text();
              throw new StoryPlanProviderError(`OpenAI story planning failed ${response.status}: ${body.slice(0, 240)}`);
            }
            const data = await response.json();
            const text = data?.choices?.[0]?.message?.content || "";
            return parseStoryPlannerResponse(text, "openai", HARD_CODED_STORY_PLANNING_OPENAI_MODEL);
          },
          evidence
        );
        return {
          plan: result.plan,
          provider: "openai",
          model: HARD_CODED_STORY_PLANNING_OPENAI_MODEL,
          validation: result.validation
        };
      }
      throw geminiError;
    }
  }
  if (lastOpenAiError) throw lastOpenAiError;
  throw new StoryPlanProviderError("No story planning provider available");
}

async function persistStoryPlanArtifact(
  signal: ResearchSignalContext,
  options: {
    plan: StoryPlanArtifact;
    provider: "openai" | "gemini";
    model: string;
    evidenceCount: number;
    validation: StoryPlanValidationResult;
    promptHash: string;
    promptSourceVersion: string;
    warnings: string[];
  }
): Promise<StoryPlanPersistenceResult> {
  if (isTestSignalId(signal.id)) {
    console.log("test-mode persistStoryPlanArtifact skip", {
      signalId: signal.id,
      provider: options.provider,
      model: options.model,
      evidenceCount: options.evidenceCount,
      plan: options.plan
    });
    return {
      saved: 1,
      version: 1,
      isCanonical: options.plan.planningStatus === "READY",
      planningStatus: options.plan.planningStatus,
      executionOutcome: options.validation.executionOutcome
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const runId = randomUUID();
  const engineId = randomUUID();
  const candidateId = randomUUID();
  const sourceUrl = `signal://${signal.id}/story-plan`;
  const versionRows = await sql`
    SELECT
      COALESCE(
        MAX(
          CASE
            WHEN COALESCE(metadata->>'version', '') ~ '^[0-9]+$'
              THEN (metadata->>'version')::int
            ELSE 0
          END
        ),
        0
      ) + 1 as version
    FROM research_artifacts
    WHERE signal_id = ${signal.id}
      AND stage = 'story_planning'
      AND artifact_type = 'story_plan'
  `;
  const version = Math.max(1, Number(versionRows?.[0]?.version || 1));
  const isCanonical =
    options.validation.executionOutcome === "validated" && options.plan.planningStatus === "READY";
  const metadata = {
    signalId: signal.id,
    personaId: signal.personaId,
    provider: options.provider,
    model: options.model,
    version,
    isCanonical,
    planningStatus: options.plan.planningStatus,
    executionOutcome: options.validation.executionOutcome,
    failureReasons: options.validation.failureReasons,
    repaired: options.validation.repaired,
    repairReason: cleanText(options.validation.repairReason || "", 500) || null,
    promptHash: cleanText(options.promptHash, 120),
    promptSourceVersion: cleanText(options.promptSourceVersion, 120),
    warnings: Array.isArray(options.warnings) ? options.warnings.slice(0, 20) : [],
    evidenceCount: options.evidenceCount,
    sectionCount: options.plan.sections.length,
    uncertaintyCount: options.plan.uncertaintyNotes.length,
    missingInformationCount: options.plan.missingInformation.length,
    plan: options.plan
  };
  const insertedId = randomUUID();

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
      ${insertedId},
      ${runId},
      ${engineId},
      ${candidateId},
      ${signal.id},
      ${signal.personaId || null},
      'story_planning',
      'story_plan',
      ${sourceUrl},
      ${null},
      ${`Story plan v${version} for signal ${signal.id}`},
      ${null},
      ${options.plan.angle || options.plan.decisionRationale || `Story planning result for signal ${signal.id}`},
      ${toSafeJsonObject(metadata)}::jsonb,
      NOW()
    RETURNING id
  `;

  if (rows.length && isCanonical) {
    await sql`
      UPDATE research_artifacts
      SET metadata = jsonb_set(
        CASE
          WHEN jsonb_typeof(metadata) = 'object' THEN metadata
          ELSE '{}'::jsonb
        END,
        '{isCanonical}',
        CASE
          WHEN id = ${insertedId} THEN 'true'::jsonb
          ELSE 'false'::jsonb
        END,
        true
      )
      WHERE signal_id = ${signal.id}
        AND stage = 'story_planning'
        AND artifact_type = 'story_plan'
        AND (
          id = ${insertedId}
          OR COALESCE(metadata->>'isCanonical', 'false') = 'true'
        )
    `;
  }

  return {
    saved: rows.length,
    version,
    isCanonical,
    planningStatus: options.plan.planningStatus,
    executionOutcome: options.validation.executionOutcome
  };
}

async function persistStoryPlanningFailureArtifact(
  signal: ResearchSignalContext,
  options: {
    executionOutcome: Exclude<StoryPlanningExecutionOutcome, "validated" | "persistence_failed">;
    failureReasons: string[];
    evidenceCount: number;
    promptHash: string;
    promptSourceVersion: string;
    warnings: string[];
  }
): Promise<StoryPlanPersistenceResult> {
  if (isTestSignalId(signal.id)) {
    console.log("test-mode persistStoryPlanningFailureArtifact skip", {
      signalId: signal.id,
      executionOutcome: options.executionOutcome,
      failureReasons: options.failureReasons
    });
    return {
      saved: 1,
      version: 1,
      isCanonical: false,
      planningStatus: "",
      executionOutcome: options.executionOutcome
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const versionRows = await sql`
    SELECT
      COALESCE(
        MAX(
          CASE
            WHEN COALESCE(metadata->>'version', '') ~ '^[0-9]+$'
              THEN (metadata->>'version')::int
            ELSE 0
          END
        ),
        0
      ) + 1 as version
    FROM research_artifacts
    WHERE signal_id = ${signal.id}
      AND stage = 'story_planning'
      AND artifact_type = 'story_plan'
  `;
  const version = Math.max(1, Number(versionRows?.[0]?.version || 1));

  const metadata = {
    signalId: signal.id,
    personaId: signal.personaId,
    version,
    isCanonical: false,
    planningStatus: "",
    executionOutcome: options.executionOutcome,
    failureReasons: options.failureReasons,
    promptHash: cleanText(options.promptHash, 120),
    promptSourceVersion: cleanText(options.promptSourceVersion, 120),
    warnings: Array.isArray(options.warnings) ? options.warnings.slice(0, 20) : [],
    evidenceCount: options.evidenceCount,
    plan: {}
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
    VALUES (
      ${randomUUID()},
      ${randomUUID()},
      ${randomUUID()},
      ${randomUUID()},
      ${signal.id},
      ${signal.personaId || null},
      'story_planning',
      'story_plan',
      ${`signal://${signal.id}/story-plan`},
      ${null},
      ${`Story plan v${version} failed for signal ${signal.id}`},
      ${null},
      ${cleanText(options.failureReasons.join(" | "), 2000) || `Story planning ${options.executionOutcome}`},
      ${toSafeJsonObject(metadata)}::jsonb,
      NOW()
    )
    RETURNING id
  `;

  return {
    saved: rows.length,
    version,
    isCanonical: false,
    planningStatus: "",
    executionOutcome: options.executionOutcome
  };
}

async function runStoryPlanning(signalId: number): Promise<{
  evidenceCount: number;
  sectionCount: number;
  saved: number;
  version?: number;
  isCanonical?: boolean;
  planningStatus?: StoryPlanningStatus | "";
  executionOutcome?: StoryPlanningExecutionOutcome;
  missingInformation?: string[];
  missingInformationQueries?: string[];
  editorialRisks?: string[];
  provider?: "openai" | "gemini";
  model?: string;
  skipped?: boolean;
}> {
  const signal = await loadResearchSignalContext(signalId);
  const evidence = await loadStoryPlanningEvidence(signalId);
  if (!evidence.length) {
    return { evidenceCount: 0, sectionCount: 0, saved: 0, skipped: true };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const guidanceBundle = await buildStageGuidanceBundle(
    sql,
    "story_planning",
    signal.personaId,
    signal.personaSection
  );
  try {
    const storyPlanResult = await buildStoryPlan(signal, evidence, guidanceBundle);
    let persisted: StoryPlanPersistenceResult;
    try {
      persisted = await persistStoryPlanArtifact(signal, {
        plan: storyPlanResult.plan,
        provider: storyPlanResult.provider,
        model: storyPlanResult.model,
        evidenceCount: evidence.length,
        validation: storyPlanResult.validation,
        promptHash: guidanceBundle.promptHash,
        promptSourceVersion: guidanceBundle.promptSourceVersion,
        warnings: guidanceBundle.warnings
      });
    } catch (persistError: any) {
      return {
        evidenceCount: evidence.length,
        sectionCount: storyPlanResult.plan.sections.length,
        saved: 0,
        version: 0,
        isCanonical: false,
        planningStatus: storyPlanResult.plan.planningStatus,
        executionOutcome: "persistence_failed",
        provider: storyPlanResult.provider,
        model: storyPlanResult.model,
        skipped: true
      };
    }

    return {
      evidenceCount: evidence.length,
      sectionCount: storyPlanResult.plan.sections.length,
      saved: persisted.saved,
      version: persisted.version,
      isCanonical: persisted.isCanonical,
      planningStatus: persisted.planningStatus,
      executionOutcome: persisted.executionOutcome,
      missingInformation: storyPlanResult.plan.missingInformation,
      missingInformationQueries: storyPlanResult.plan.missingInformationQueries,
      editorialRisks: storyPlanResult.plan.editorialRisks,
      provider: storyPlanResult.provider,
      model: storyPlanResult.model
    };
  } catch (error: any) {
    const executionOutcome: Exclude<StoryPlanningExecutionOutcome, "validated" | "persistence_failed"> =
      error instanceof StoryPlanValidationError
        ? error.executionOutcome
        : error instanceof StoryPlanProviderError
          ? error.executionOutcome
          : "provider_failure";
    const failureReasons =
      error instanceof StoryPlanValidationError
        ? error.failureReasons
        : [cleanText(error?.message || "story_planning_failed", 500)];
    const persistedFailure = await persistStoryPlanningFailureArtifact(signal, {
      executionOutcome,
      failureReasons,
      evidenceCount: evidence.length,
      promptHash: guidanceBundle.promptHash,
      promptSourceVersion: guidanceBundle.promptSourceVersion,
      warnings: guidanceBundle.warnings
    });
    return {
      evidenceCount: evidence.length,
      sectionCount: 0,
      saved: persistedFailure.saved,
      version: persistedFailure.version,
      isCanonical: false,
      planningStatus: persistedFailure.planningStatus,
      executionOutcome: persistedFailure.executionOutcome,
      skipped: true
    };
  }
}

async function loadLatestStoryPlanResult(
  signalId: number,
  evidence: StoryPlanningEvidence[]
): Promise<StoryPlanLoadResult | null> {
  if (isTestSignalId(signalId)) {
    const testPlan = normalizeStoryPlan(
      {
        planningStatus: "READY",
        decisionRationale: "Core event, impact, and practical next-step information are sufficiently supported.",
        angle: "Downtown Dayton weekend closures will disrupt drivers and transit riders through Sunday night.",
        narrativeStrategy:
          "Lead with the closure timeline and immediate disruption, then explain transit changes, public impact, and remaining uncertainty.",
        approvedEvidenceIds: ["ev_mock_1", "ev_mock_2"],
        approvedSourceUrls: [
          "https://example.com/mock-signal-12345",
          "https://example.com/mock-signal-12345-traffic"
        ],
        sections: [
          {
            sectionId: "lead",
            heading: "What is changing this weekend",
            summary: "Explain when the downtown lane closures begin, how long they are expected to last, and who is affected first.",
            purpose: "lead",
            evidenceIds: ["ev_mock_1"],
            evidenceSourceUrls: ["https://example.com/mock-signal-12345"],
            assertiveness: "high",
            tensionFlags: [],
            qualificationNotes: [],
            priority: 1
          },
          {
            sectionId: "nut_graph",
            heading: "Why the closures matter beyond traffic",
            summary: "Show why the closures matter for downtown access, weekend movement, and public planning.",
            purpose: "nut_graph",
            evidenceIds: ["ev_mock_1", "ev_mock_2"],
            evidenceSourceUrls: [
              "https://example.com/mock-signal-12345",
              "https://example.com/mock-signal-12345-traffic"
            ],
            assertiveness: "medium",
            tensionFlags: [],
            qualificationNotes: [],
            priority: 2
          },
          {
            sectionId: "impact",
            heading: "Transit riders should expect route adjustments",
            summary: "Detail the temporary stop changes and what riders need to check before traveling downtown.",
            purpose: "impact",
            evidenceIds: ["ev_mock_2"],
            evidenceSourceUrls: ["https://example.com/mock-signal-12345-traffic"],
            assertiveness: "medium",
            tensionFlags: [],
            qualificationNotes: [],
            priority: 3
          }
        ],
        uncertaintyNotes: ["Exact reopening timing may shift based on field progress."],
        missingInformation: ["Intersection-level closure windows were not fully published."],
        missingInformationQueries: ["Find any city-posted intersection-level closure windows for the weekend work."],
        editorialRisks: [],
        primarySourceAssessment: {
          hasPrimarySource: true,
          primaryEvidenceIds: ["ev_mock_1", "ev_mock_2"],
          notes: ["Both core impact points are supported by official agency statements."]
        },
        personaFitAssessment: {
          fit: "strong",
          notes: ["This matches a local public-works and transit accountability brief."]
        }
      },
      evidence
    );
    return {
      plan: testPlan,
      version: 1,
      isCanonical: true,
      executionOutcome: "validated",
      failureReasons: []
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const sourceUrl = `signal://${signalId}/story-plan`;
  const rows = await sql`
    SELECT metadata
    FROM research_artifacts
    WHERE signal_id = ${signalId}
      AND stage = 'story_planning'
      AND artifact_type = 'story_plan'
      AND source_url = ${sourceUrl}
    ORDER BY
      CASE
        WHEN COALESCE(metadata->>'version', '') ~ '^[0-9]+$'
          THEN (metadata->>'version')::int
        ELSE 0
      END DESC,
      created_at DESC
    LIMIT 1
  `;

  const metadata = toSafeJsonObject(rows?.[0]?.metadata);
  const rawPlan = toSafeJsonObject(metadata.plan);
  if (!Object.keys(metadata).length) return null;
  return {
    plan: normalizeStoryPlan(rawPlan, evidence),
    version:
      cleanText(metadata.version, 20) && Number.isFinite(Number(metadata.version))
        ? Number(metadata.version)
        : 0,
    isCanonical: metadata.isCanonical === true,
    executionOutcome: cleanText(metadata.executionOutcome, 40) as StoryPlanningExecutionOutcome | "",
    failureReasons: Array.isArray(metadata.failureReasons)
      ? metadata.failureReasons.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 12)
      : []
  };
}

async function loadLatestStoryPlanArtifact(
  signalId: number,
  evidence: StoryPlanningEvidence[]
): Promise<StoryPlanArtifact | null> {
  const latest = await loadLatestStoryPlanResult(signalId, evidence);
  if (!latest) return null;
  if (!latest.isCanonical) return null;
  if (latest.executionOutcome !== "validated") return null;
  if (latest.plan.planningStatus !== "READY") return null;
  return latest.plan;
}

function dedupeUrls(urls: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of urls) {
    const url = cleanText(raw, 2000);
    const key = url.toLowerCase();
    if (!url || seen.has(key)) continue;
    seen.add(key);
    out.push(url);
    if (out.length >= 8) break;
  }
  return out;
}

function normalizeDraftWriterProvider(value: unknown): DraftWriterProvider | null {
  const provider = cleanText(value, 40).toLowerCase();
  return SUPPORTED_DRAFT_WRITER_PROVIDERS.includes(provider as DraftWriterProvider)
    ? (provider as DraftWriterProvider)
    : null;
}

function normalizeDraftWriterModel(provider: DraftWriterProvider, value: unknown): string {
  const model = cleanText(value, 160);
  if (!model) return "";
  const curated = getCuratedDraftWriterModels();
  return model === curated[provider] ? model : "";
}

function getHardcodedDraftWriterConfig(): DraftWriterConfig {
  const curated = getCuratedDraftWriterModels();
  return {
    provider: "openai",
    model: curated.openai,
    source: "hardcoded_fallback"
  };
}

async function loadDraftWritingStageConfig(
  sql: any,
  personaId: string
): Promise<{ provider: string; modelOrEndpoint: string } | null> {
  if (!personaId) return null;
  const rows = await sql`
    SELECT
      provider,
      model_or_endpoint as "modelOrEndpoint"
    FROM topic_engine_stage_configs
    WHERE persona_id = ${personaId}
      AND stage_name = 'draft_writing'
    LIMIT 1
  `;
  if (!rows?.[0]) return null;
  return {
    provider: cleanText(rows[0].provider, 120),
    modelOrEndpoint: cleanText(rows[0].modelOrEndpoint, 160)
  };
}

async function resolveDraftWriterConfig(sql: any, personaId: string): Promise<DraftWriterConfig> {
  const stageConfig = await loadDraftWritingStageConfig(sql, personaId);
  const provider = normalizeDraftWriterProvider(stageConfig?.provider);
  if (!provider) return getHardcodedDraftWriterConfig();
  const model = normalizeDraftWriterModel(provider, stageConfig?.modelOrEndpoint);
  if (!model) return getHardcodedDraftWriterConfig();
  return {
    provider,
    model,
    source: "persona_config"
  };
}

function buildDraftWritingPrompt(
  signal: ResearchSignalContext,
  plan: StoryPlanArtifact,
  evidence: StoryPlanningEvidence[],
  guidanceBundle: StagePromptBundle
): string {
  const approvedEvidenceIds = new Set(plan.approvedEvidenceIds);
  const approvedEvidence = evidence.filter((item) => approvedEvidenceIds.has(item.evidenceId));
  const evidenceContext = approvedEvidence
    .slice(0, 10)
    .map((item, index) =>
      [
        `Evidence ${index + 1}:`,
        `Evidence ID: ${cleanText(item.evidenceId, 120)}`,
        `Claim: ${cleanText(item.claim, 600)}`,
        `Source URL: ${cleanText(item.sourceUrl, 2000)}`,
        `Quote: ${cleanText(item.evidenceQuote, 1200)}`,
        `Confidence: ${item.confidence}`,
        `Why it matters: ${cleanText(item.whyItMatters, 700)}`
      ].join("\n")
    )
    .join("\n\n");
  const planContext = plan.sections
    .slice(0, 8)
    .map((section, index) =>
      [
        `Section ${index + 1}: ${cleanText(section.heading, 220)}`,
        `Summary: ${cleanText(section.summary, 1400)}`,
        `Purpose: ${cleanText(section.purpose, 80)}`,
        `Evidence IDs: ${(section.evidenceIds || []).map((id) => cleanText(id, 120)).filter(Boolean).join(", ")}`,
        `Evidence Source URLs: ${(section.evidenceSourceUrls || []).map((url) => cleanText(url, 500)).filter(Boolean).join(", ")}`,
        `Assertiveness: ${cleanText(section.assertiveness, 40)}`,
        `Tension Flags: ${(section.tensionFlags || []).map((item) => cleanText(item, 120)).filter(Boolean).join(", ") || "none"}`,
        `Qualification Notes: ${(section.qualificationNotes || []).map((item) => cleanText(item, 240)).filter(Boolean).join(" | ") || "none"}`
      ].join("\n")
    )
    .join("\n\n");
  const allowedSourceUrls = dedupeUrls(plan.approvedSourceUrls || []).join("\n");

  return [
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion
      ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}`
      : "",
    "You are writing a publication-ready newsroom draft for a local autonomous newsroom pipeline.",
    "Use only the verified story plan and evidence below.",
    "Do not invent facts, quotes, attributions, figures, chronology, or background.",
    "Do not use outside knowledge.",
    "Keep the writing concrete, news-style, and publication ready.",
    "Return strict JSON only in this schema:",
    "{\"headline\":\"...\",\"dek\":\"...\",\"body\":\"...\",\"sourceUrls\":[\"...\"],\"uncertaintyNotes\":[\"...\"],\"coverageGaps\":[\"...\"]}",
    "Rules:",
    "- body must be a fully written article, not an outline.",
    "- Write in clean paragraphs with no markdown.",
    "- headline and dek must be specific and publication-ready.",
    "- sourceUrls must only contain URLs from the approved list below.",
    "- uncertaintyNotes and coverageGaps may be empty arrays.",
    "- No extra keys. No commentary outside JSON.",
    "",
    `Persona ID: ${signal.personaId}`,
    `Section: ${signal.personaSection}`,
    `Beat: ${signal.personaBeat}`,
    `Signal Title: ${signal.title}`,
    `Signal Snippet: ${signal.snippet || ""}`,
    "",
    `Planned Angle: ${cleanText(plan.angle, 500)}`,
    `Narrative Strategy: ${cleanText(plan.narrativeStrategy, 1200)}`,
    `Plan Uncertainty Notes: ${plan.uncertaintyNotes.map((item) => cleanText(item, 200)).filter(Boolean).join(" | ") || "none"}`,
    `Plan Missing Information: ${plan.missingInformation.map((item) => cleanText(item, 200)).filter(Boolean).join(" | ") || "none"}`,
    `Plan Editorial Risks: ${plan.editorialRisks.map((item) => cleanText(item, 120)).filter(Boolean).join(" | ") || "none"}`,
    "",
    "Story Plan:",
    planContext,
    "",
    "Verified Evidence:",
    evidenceContext,
    "",
    "Approved Source URLs:",
    allowedSourceUrls
  ]
    .filter(Boolean)
    .join("\n");
}

function buildDraftWritingRepairPrompt(
  originalPrompt: string,
  rawText: string,
  failureReason: string
): string {
  return [
    "Repair the draft-writing response so it matches the required JSON contract exactly.",
    "Return strict JSON only.",
    `Repair reason: ${cleanText(failureReason, 500)}`,
    "",
    "Original instructions:",
    originalPrompt,
    "",
    "Previous model output to repair:",
    cleanText(rawText, 24000)
  ].join("\n");
}

function collectAllowedDraftSourceUrls(
  plan: StoryPlanArtifact,
  evidence: StoryPlanningEvidence[]
): string[] {
  return dedupeUrls(plan.approvedSourceUrls || []);
}

function normalizeDraftWritingArtifact(parsed: any): DraftWritingArtifact {
  const raw = parsed && typeof parsed === "object" ? parsed : {};
  return {
    headline: cleanText(raw.headline || raw.title || "", 220),
    dek: cleanText(raw.dek || raw.description || "", 320),
    body: cleanText(raw.body || raw.content || "", 22000),
    sourceUrls: dedupeUrls(Array.isArray(raw.sourceUrls) ? raw.sourceUrls : []),
    uncertaintyNotes: Array.isArray(raw.uncertaintyNotes)
      ? raw.uncertaintyNotes.map((item: unknown) => cleanText(item, 300)).filter(Boolean).slice(0, 8)
      : [],
    coverageGaps: Array.isArray(raw.coverageGaps)
      ? raw.coverageGaps.map((item: unknown) => cleanText(item, 300)).filter(Boolean).slice(0, 8)
      : []
  };
}

function sanitizeDraftWritingArtifact(draft: DraftWritingArtifact, allowedSourceUrls: string[]): DraftWritingArtifact {
  const allowed = new Set(allowedSourceUrls.map((url) => cleanText(url, 2000).toLowerCase()).filter(Boolean));
  const filteredUrls = dedupeUrls(
    draft.sourceUrls.filter((url) => allowed.has(cleanText(url, 2000).toLowerCase()))
  );
  return {
    headline: cleanText(draft.headline, 220),
    dek: cleanText(draft.dek, 320),
    body: cleanText(draft.body, 22000),
    sourceUrls: filteredUrls,
    uncertaintyNotes: Array.isArray(draft.uncertaintyNotes)
      ? draft.uncertaintyNotes.map((item) => cleanText(item, 300)).filter(Boolean).slice(0, 8)
      : [],
    coverageGaps: Array.isArray(draft.coverageGaps)
      ? draft.coverageGaps.map((item) => cleanText(item, 300)).filter(Boolean).slice(0, 8)
      : []
  };
}

function validateDraftWritingArtifact(draft: DraftWritingArtifact, allowedSourceUrls: string[]): void {
  if (!cleanText(draft.headline, 220)) {
    throw new Error("Draft writing returned empty headline");
  }
  if (!cleanText(draft.body, 22000)) {
    throw new Error("Draft writing returned empty body");
  }
  if (cleanText(draft.body, 22000).length < 400) {
    throw new Error("Draft writing returned body below minimum length");
  }
  if (!draft.sourceUrls.length) {
    throw new Error("Draft writing returned no approved source URLs");
  }
  const allowed = new Set(allowedSourceUrls.map((url) => cleanText(url, 2000).toLowerCase()).filter(Boolean));
  if (draft.sourceUrls.some((url) => !allowed.has(cleanText(url, 2000).toLowerCase()))) {
    throw new Error("Draft writing returned unapproved source URLs");
  }
}

function parseDraftWriterResponse(
  rawText: string,
  config: DraftWriterConfig
): DraftWriterExecutionResult {
  return {
    provider: config.provider,
    model: config.model,
    source: config.source,
    rawText,
    parsed: safeJsonParse(rawText)
  };
}

async function executeDraftWriterWithValidation(
  prompt: string,
  config: DraftWriterConfig,
  allowedSourceUrls: string[]
): Promise<{ execution: DraftWriterExecutionResult; draft: DraftWritingArtifact; validation: DraftWritingValidationResult }> {
  const firstExecution = await executeDraftWriter(prompt, config);
  try {
    const firstDraft = sanitizeDraftWritingArtifact(normalizeDraftWritingArtifact(firstExecution.parsed), allowedSourceUrls);
    validateDraftWritingArtifact(firstDraft, allowedSourceUrls);
    return {
      execution: firstExecution,
      draft: firstDraft,
      validation: {
        repaired: false,
        repairReason: null,
        sourceUrlCountAccepted: firstDraft.sourceUrls.length
      }
    };
  } catch (error: any) {
    const repairReason = cleanText(error?.message || "draft_validation_failed", 500);
    const repairPrompt = buildDraftWritingRepairPrompt(prompt, firstExecution.rawText, repairReason);
    const repairedExecution = await executeDraftWriter(repairPrompt, config);
    const repairedDraft = sanitizeDraftWritingArtifact(
      normalizeDraftWritingArtifact(repairedExecution.parsed),
      allowedSourceUrls
    );
    validateDraftWritingArtifact(repairedDraft, allowedSourceUrls);
    return {
      execution: repairedExecution,
      draft: repairedDraft,
      validation: {
        repaired: true,
        repairReason,
        sourceUrlCountAccepted: repairedDraft.sourceUrls.length
      }
    };
  }
}

async function writeDraftWithAnthropic(prompt: string, config: DraftWriterConfig): Promise<DraftWriterExecutionResult> {
  const apiKey = cleanText(process.env.ANTHROPIC_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

  const response = await fetchWithTimeout(
    "https://api.anthropic.com/v1/messages",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01"
      },
      body: JSON.stringify({
        model: config.model,
        max_tokens: 3200,
        temperature: 0.35,
        messages: [{ role: "user", content: prompt }]
      })
    },
    DRAFT_WRITING_TIMEOUT_MS
  );

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Anthropic draft writing failed ${response.status}: ${body.slice(0, 240)}`);
  }

  const data = await response.json();
  const rawText = data?.content?.map((item: any) => (item?.type === "text" ? item.text || "" : "")).join("") || "";
  return parseDraftWriterResponse(rawText, config);
}

async function writeDraftWithOpenAI(prompt: string, config: DraftWriterConfig): Promise<DraftWriterExecutionResult> {
  const apiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing OPENAI_API_KEY");

  const response = await fetchWithTimeout(
    "https://api.openai.com/v1/chat/completions",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: config.model,
        temperature: 0.35,
        max_completion_tokens: 3200,
        response_format: { type: "json_object" },
        messages: [{ role: "user", content: prompt }]
      })
    },
    DRAFT_WRITING_TIMEOUT_MS
  );

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`OpenAI draft writing failed ${response.status}: ${body.slice(0, 240)}`);
  }

  const data = await response.json();
  const rawText = data?.choices?.[0]?.message?.content || "";
  return parseDraftWriterResponse(rawText, config);
}

async function writeDraftWithGemini(prompt: string, config: DraftWriterConfig): Promise<DraftWriterExecutionResult> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    config.model
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const response = await fetchWithTimeout(
    endpoint,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.35,
          maxOutputTokens: 3200,
          responseMimeType: "application/json"
        }
      })
    },
    DRAFT_WRITING_TIMEOUT_MS
  );

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Gemini draft writing failed ${response.status}: ${body.slice(0, 240)}`);
  }

  const data = await response.json();
  const rawText = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
  return parseDraftWriterResponse(rawText, config);
}

async function writeDraftWithGrok(prompt: string, config: DraftWriterConfig): Promise<DraftWriterExecutionResult> {
  const apiKey = cleanText(process.env.GROK_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GROK_API_KEY");

  const response = await fetchWithTimeout(
    "https://api.x.ai/v1/chat/completions",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: config.model,
        temperature: 0.35,
        max_tokens: 3200,
        messages: [{ role: "user", content: prompt }]
      })
    },
    DRAFT_WRITING_TIMEOUT_MS
  );

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Grok draft writing failed ${response.status}: ${body.slice(0, 240)}`);
  }

  const data = await response.json();
  const rawText = data?.choices?.[0]?.message?.content || "";
  return parseDraftWriterResponse(rawText, config);
}

async function executeDraftWriter(prompt: string, config: DraftWriterConfig): Promise<DraftWriterExecutionResult> {
  if (config.provider === "openai") return writeDraftWithOpenAI(prompt, config);
  if (config.provider === "gemini") return writeDraftWithGemini(prompt, config);
  if (config.provider === "grok") return writeDraftWithGrok(prompt, config);
  return writeDraftWithAnthropic(prompt, config);
}

function buildDeterministicDraftFromPlan(
  signal: ResearchSignalContext,
  plan: StoryPlanArtifact,
  evidence: StoryPlanningEvidence[]
): DraftWritingArtifact {
  const sectionParagraphs = plan.sections
    .slice(0, 8)
    .map((section) => `${section.heading}: ${cleanText(section.summary, 1200)}`)
    .filter(Boolean);

  const body = sectionParagraphs.join("\n\n");
  const urlsFromPlan = dedupeUrls(plan.sections.flatMap((section) => section.evidenceSourceUrls || []));
  const fallbackUrls = dedupeUrls(evidence.map((item) => item.sourceUrl));

  return {
    headline: cleanText(plan.angle || signal.title, 220) || "Local update",
    dek:
      cleanText(plan.narrativeStrategy, 320) ||
      "Verified local developments and what they mean for Dayton-area readers.",
    body: cleanText(body, 22000),
    sourceUrls: urlsFromPlan.length ? urlsFromPlan : fallbackUrls,
    uncertaintyNotes: plan.uncertaintyNotes.map((note) => cleanText(note, 300)).filter(Boolean).slice(0, 8),
    coverageGaps: plan.missingInformation.map((item) => cleanText(item, 300)).filter(Boolean).slice(0, 8)
  };
}

async function persistDraftWritingArtifact(
  signal: ResearchSignalContext,
  draft: DraftWritingArtifact,
  context: {
    provider: string;
    model: string;
    configSource?: "persona_config" | "hardcoded_fallback";
    fallbackFailure?: DraftWriterFailureInfo | null;
    validation: DraftWritingValidationResult;
    evidenceCount: number;
    sectionCount: number;
    promptHash: string;
    promptSourceVersion: string;
    warnings: string[];
  }
): Promise<number> {
  if (isTestSignalId(signal.id)) {
    console.log("test-mode persistDraftWritingArtifact skip", {
      signalId: signal.id,
      provider: context.provider,
      model: context.model,
      evidenceCount: context.evidenceCount,
      sectionCount: context.sectionCount,
      draft
    });
    return 1;
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const runId = randomUUID();
  const engineId = randomUUID();
  const candidateId = randomUUID();
  const sourceUrl = `signal://${signal.id}/draft`;

  const metadata = {
    signalId: signal.id,
    personaId: signal.personaId,
    provider: cleanText(context.provider, 80),
    model: cleanText(context.model, 160),
    configSource: cleanText(context.configSource || "", 40) || null,
    fallbackFailure: context.fallbackFailure
      ? {
          provider: cleanText(context.fallbackFailure.provider, 80),
          model: cleanText(context.fallbackFailure.model, 160),
          source: cleanText(context.fallbackFailure.source, 40),
          message: cleanText(context.fallbackFailure.message, 1000)
        }
      : null,
    validation: {
      repaired: context.validation?.repaired === true,
      repairReason: cleanText(context.validation?.repairReason || "", 500) || null,
      sourceUrlCountAccepted: Math.max(0, Number(context.validation?.sourceUrlCountAccepted || 0))
    },
    evidenceCount: context.evidenceCount,
    sectionCount: context.sectionCount,
    sourceCount: draft.sourceUrls.length,
    promptHash: cleanText(context.promptHash, 120),
    promptSourceVersion: cleanText(context.promptSourceVersion, 120),
    warnings: Array.isArray(context.warnings) ? context.warnings.slice(0, 20) : [],
    draft
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
      'draft_writing',
      'draft_package',
      ${sourceUrl},
      ${null},
      ${draft.headline || `Draft package for signal ${signal.id}`},
      ${null},
      ${draft.body},
      ${toSafeJsonObject(metadata)}::jsonb,
      NOW()
    WHERE NOT EXISTS (
      SELECT 1
      FROM research_artifacts ra
      WHERE ra.signal_id = ${signal.id}
        AND ra.stage = 'draft_writing'
        AND ra.artifact_type = 'draft_package'
        AND ra.source_url = ${sourceUrl}
    )
    RETURNING id
  `;

  return rows.length;
}

async function runDraftWriting(signalId: number): Promise<{
  evidenceCount: number;
  sectionCount: number;
  sourceCount: number;
  bodyChars: number;
  saved: number;
  provider: string;
  model: string;
  skipped?: boolean;
}> {
  const signal = await loadResearchSignalContext(signalId);
  const evidence = await loadStoryPlanningEvidence(signalId);
  if (!evidence.length) {
    return {
      evidenceCount: 0,
      sectionCount: 0,
      sourceCount: 0,
      bodyChars: 0,
      saved: 0,
      provider: getHardcodedDraftWriterConfig().provider,
      model: getHardcodedDraftWriterConfig().model,
      skipped: true
    };
  }

  const latestPlanResult = await loadLatestStoryPlanResult(signalId, evidence);
  const plan = latestPlanResult?.plan || null;
  if (
    !latestPlanResult ||
    latestPlanResult.executionOutcome !== "validated" ||
    latestPlanResult.plan.planningStatus !== "READY" ||
    !latestPlanResult.isCanonical ||
    !plan ||
    !plan.sections.length
  ) {
    return {
      evidenceCount: evidence.length,
      sectionCount: 0,
      sourceCount: 0,
      bodyChars: 0,
      saved: 0,
      provider: getHardcodedDraftWriterConfig().provider,
      model: getHardcodedDraftWriterConfig().model,
      skipped: true
    };
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const guidanceBundle = await buildStageGuidanceBundle(
    sql,
    "draft_writing",
    signal.personaId,
    signal.personaSection
  );
  let execution: DraftWriterExecutionResult;
  let draft: DraftWritingArtifact;
  let validation: DraftWritingValidationResult;
  let fallbackFailure: DraftWriterFailureInfo | null = null;
  const allowedSourceUrls = collectAllowedDraftSourceUrls(plan, evidence);
  if (!allowedSourceUrls.length) {
    return {
      evidenceCount: evidence.length,
      sectionCount: plan.sections.length,
      sourceCount: 0,
      bodyChars: 0,
      saved: 0,
      provider: getHardcodedDraftWriterConfig().provider,
      model: getHardcodedDraftWriterConfig().model,
      skipped: true
    };
  }
  if (isTestSignalId(signalId)) {
    execution = {
      ...getHardcodedDraftWriterConfig(),
      rawText: "",
      parsed: buildDeterministicDraftFromPlan(signal, plan, evidence)
    };
    draft = sanitizeDraftWritingArtifact(normalizeDraftWritingArtifact(execution.parsed), allowedSourceUrls);
    validateDraftWritingArtifact(draft, allowedSourceUrls);
    validation = {
      repaired: false,
      repairReason: null,
      sourceUrlCountAccepted: draft.sourceUrls.length
    };
  } else {
    const resolvedWriter = await resolveDraftWriterConfig(sql, signal.personaId);
    const prompt = buildDraftWritingPrompt(signal, plan, evidence, guidanceBundle);
    const fallbackWriter = getHardcodedDraftWriterConfig();
    try {
      const result = await executeDraftWriterWithValidation(prompt, resolvedWriter, allowedSourceUrls);
      execution = result.execution;
      draft = result.draft;
      validation = result.validation;
    } catch (error: any) {
      const shouldTryFallback =
        resolvedWriter.source === "persona_config" &&
        (resolvedWriter.provider !== fallbackWriter.provider || resolvedWriter.model !== fallbackWriter.model);
      if (!shouldTryFallback) throw error;
      fallbackFailure = {
        provider: resolvedWriter.provider,
        model: resolvedWriter.model,
        source: resolvedWriter.source,
        message: cleanText(error?.message || "draft_writer_failed", 1000)
      };
      const fallbackResult = await executeDraftWriterWithValidation(prompt, fallbackWriter, allowedSourceUrls);
      execution = fallbackResult.execution;
      draft = fallbackResult.draft;
      validation = fallbackResult.validation;
    }
  }
  const saved = await persistDraftWritingArtifact(signal, draft, {
    provider: execution.provider,
    model: execution.model,
    configSource: execution.source,
    fallbackFailure,
    validation,
    evidenceCount: evidence.length,
    sectionCount: plan.sections.length,
    promptHash: guidanceBundle.promptHash,
    promptSourceVersion: guidanceBundle.promptSourceVersion,
    warnings: guidanceBundle.warnings
  });

  return {
    evidenceCount: evidence.length,
    sectionCount: plan.sections.length,
    sourceCount: draft.sourceUrls.length,
    bodyChars: draft.body.length,
    saved,
    provider: execution.provider,
    model: execution.model
  };
}

type Layer6PersonaConfig = {
  imageDbEnabled: boolean;
  imageSourcingEnabled: boolean;
  imageGenerationEnabled: boolean;
  imageMode: "manual" | "auto";
  imageProfile: "professional" | "creative" | "cheap";
  imageFallbackAssetUrl: string | null;
  imageFallbackCloudinaryPublicId: string | null;
  quotaPostgresImageDaily: number;
  quotaSourcedImageDaily: number;
  quotaGeneratedImageDaily: number;
  quotaTextOnlyDaily: number;
  layer6TimeoutSeconds: number;
  layer6BudgetUsd: number;
  exaMaxAttempts: number;
  generationMaxAttempts: number;
};

type Layer6RuntimePolicy = {
  allowPostgres: boolean;
  allowExa: boolean;
  allowGenerated: boolean;
  exaAttempts: number;
  generationAttempts: number;
  budgetUsd: number;
};

type Layer6Candidate = {
  id: string;
  tier: "postgres_pass1" | "postgres_pass2" | "exa" | "generated" | "persona_fallback";
  source: string;
  sourceUrl: string;
  imageUrl: string;
  imageTitle: string;
  imageCredit: string;
  attemptNumber: number;
  latencyMs: number;
  costUsd: number;
  contextScore: number | null;
  qualityScore: number | null;
  trustScore: number;
  weightedScore: number | null;
  isRejected: boolean;
  rejectionReason: string | null;
  cloudinary: { publicId: string | null; secureUrl: string | null; metadata: Record<string, unknown> };
  metadata: Record<string, unknown>;
};

const LAYER6_TIER_ORDER: Array<Layer6Candidate["tier"]> = [
  "postgres_pass1",
  "postgres_pass2",
  "exa",
  "generated",
  "persona_fallback"
];

const LAYER6_TRUST_BY_TIER: Record<Layer6Candidate["tier"], number> = {
  postgres_pass1: 0.92,
  postgres_pass2: 0.85,
  exa: 0.74,
  generated: 0.58,
  persona_fallback: 0.8
};

function layer6RemainingTimeoutMs(deadlineMs: number, defaultMs = 8000): number {
  const remaining = Math.max(0, Number(deadlineMs || 0) - Date.now() - 200);
  if (remaining <= 0) return 200;
  return Math.max(200, Math.min(defaultMs, remaining));
}

async function fetchWithTimeout(url: string, init: RequestInit, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(200, Number(timeoutMs || 0)));
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function buildLayer6RuntimePolicy(config: Layer6PersonaConfig): Layer6RuntimePolicy {
  const modeAllowsExternal = config.imageMode === "auto";
  let exaAttempts = config.exaMaxAttempts;
  let generationAttempts = config.generationMaxAttempts;
  let budgetUsd = config.layer6BudgetUsd;

  if (config.imageProfile === "cheap") {
    exaAttempts = Math.min(exaAttempts, 1);
    generationAttempts = Math.min(generationAttempts, 1);
    budgetUsd = Math.min(budgetUsd, 0.1);
  } else if (config.imageProfile === "creative") {
    generationAttempts = Math.min(20, generationAttempts + 1);
    budgetUsd = Math.min(50, Math.max(budgetUsd, 0.35));
  }

  const allowExa = modeAllowsExternal && config.imageSourcingEnabled;
  const allowGenerated = modeAllowsExternal && config.imageGenerationEnabled;
  return {
    allowPostgres: config.imageDbEnabled,
    allowExa,
    allowGenerated,
    exaAttempts: allowExa ? Math.max(0, exaAttempts) : 0,
    generationAttempts: allowGenerated ? Math.max(0, generationAttempts) : 0,
    budgetUsd: Math.max(0, budgetUsd)
  };
}

function normalizeLayer6Outcome(value: unknown): "with_image" | "text_only" {
  const v = cleanText(value, 40).toLowerCase();
  return v === "with_image" ? "with_image" : "text_only";
}

function scoreWeightedImageCandidate(candidate: Layer6Candidate): number {
  const context = Number.isFinite(Number(candidate.contextScore)) ? Number(candidate.contextScore) : 0;
  const quality = Number.isFinite(Number(candidate.qualityScore)) ? Number(candidate.qualityScore) : 0;
  const trust = Number.isFinite(Number(candidate.trustScore)) ? Number(candidate.trustScore) : 0;
  return Number((context * 0.55 + quality * 0.25 + trust * 10 * 0.2).toFixed(3));
}

function chooseLayer6Winner(candidates: Layer6Candidate[]): Layer6Candidate | null {
  const finalists = candidates.filter((c) => !c.isRejected && Number.isFinite(Number(c.weightedScore)));
  if (!finalists.length) return null;
  finalists.sort((a, b) => {
    const scoreDelta = Number(b.weightedScore || 0) - Number(a.weightedScore || 0);
    if (Math.abs(scoreDelta) <= 0.05) {
      return LAYER6_TIER_ORDER.indexOf(a.tier) - LAYER6_TIER_ORDER.indexOf(b.tier);
    }
    return scoreDelta;
  });
  return finalists[0] || null;
}

async function loadLayer6PersonaConfig(sql: any, personaId: string): Promise<Layer6PersonaConfig> {
  const rows = await sql`
    SELECT
      COALESCE(image_db_enabled, TRUE) as "imageDbEnabled",
      COALESCE(image_sourcing_enabled, TRUE) as "imageSourcingEnabled",
      COALESCE(image_generation_enabled, FALSE) as "imageGenerationEnabled",
      COALESCE(NULLIF(trim(image_mode), ''), 'manual') as "imageMode",
      COALESCE(NULLIF(trim(image_profile), ''), 'professional') as "imageProfile",
      image_fallback_asset_url as "imageFallbackAssetUrl",
      image_fallback_cloudinary_public_id as "imageFallbackCloudinaryPublicId",
      COALESCE(quota_postgres_image_daily, 2) as "quotaPostgresImageDaily",
      COALESCE(quota_sourced_image_daily, 2) as "quotaSourcedImageDaily",
      COALESCE(quota_generated_image_daily, 2) as "quotaGeneratedImageDaily",
      COALESCE(quota_text_only_daily, 3) as "quotaTextOnlyDaily",
      COALESCE(layer6_timeout_seconds, 90) as "layer6TimeoutSeconds",
      COALESCE(layer6_budget_usd, 0.20) as "layer6BudgetUsd",
      COALESCE(exa_max_attempts, 3) as "exaMaxAttempts",
      COALESCE(generation_max_attempts, 2) as "generationMaxAttempts"
    FROM personas
    WHERE id = ${personaId}
    LIMIT 1
  `;
  const row = rows?.[0] || {};
  const modeRaw = cleanText(row.imageMode || "manual", 20).toLowerCase();
  const profileRaw = cleanText(row.imageProfile || "professional", 30).toLowerCase();
  return {
    imageDbEnabled: row.imageDbEnabled !== false,
    imageSourcingEnabled: row.imageSourcingEnabled !== false,
    imageGenerationEnabled: row.imageGenerationEnabled === true,
    imageMode: modeRaw === "auto" ? "auto" : "manual",
    imageProfile:
      profileRaw === "creative" || profileRaw === "cheap" ? (profileRaw as "creative" | "cheap") : "professional",
    imageFallbackAssetUrl: cleanText(row.imageFallbackAssetUrl || "", 5000) || null,
    imageFallbackCloudinaryPublicId: cleanText(row.imageFallbackCloudinaryPublicId || "", 500) || null,
    quotaPostgresImageDaily: Math.min(Math.max(Number(row.quotaPostgresImageDaily || 2), 0), 5000),
    quotaSourcedImageDaily: Math.min(Math.max(Number(row.quotaSourcedImageDaily || 2), 0), 5000),
    quotaGeneratedImageDaily: Math.min(Math.max(Number(row.quotaGeneratedImageDaily || 2), 0), 5000),
    quotaTextOnlyDaily: Math.min(Math.max(Number(row.quotaTextOnlyDaily || 3), 0), 5000),
    layer6TimeoutSeconds: Math.min(Math.max(Number(row.layer6TimeoutSeconds || 90), 15), 600),
    layer6BudgetUsd: Math.min(Math.max(Number(row.layer6BudgetUsd || 0.2), 0), 50),
    exaMaxAttempts: Math.min(Math.max(Number(row.exaMaxAttempts || 3), 1), 20),
    generationMaxAttempts: Math.min(Math.max(Number(row.generationMaxAttempts || 2), 1), 20)
  };
}

async function loadLayer6DailyUsage(
  sql: any,
  personaId: string
): Promise<{ postgres: number; exa: number; generated: number; textOnly: number }> {
  try {
    const rows = await sql`
      WITH ranked_runs AS (
        SELECT
          final_outcome,
          ROW_NUMBER() OVER (
            PARTITION BY COALESCE(signal_id::text, id::text)
            ORDER BY
              CASE WHEN COALESCE(diagnostics->>'idempotencyCanonical', 'false') = 'true' THEN 0 ELSE 1 END,
              COALESCE(updated_at, created_at, started_at) DESC,
              id DESC
          ) AS rn
        FROM image_pipeline_runs
        WHERE persona_id = ${personaId}
          AND status IN ('completed', 'timed_out')
          AND (started_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date
      )
      SELECT
        COALESCE(SUM(CASE WHEN final_outcome = 'postgres_selected' THEN 1 ELSE 0 END), 0)::int as "postgresCount",
        COALESCE(SUM(CASE WHEN final_outcome = 'exa_selected' THEN 1 ELSE 0 END), 0)::int as "exaCount",
        COALESCE(SUM(CASE WHEN final_outcome = 'generated_selected' THEN 1 ELSE 0 END), 0)::int as "generatedCount",
        COALESCE(SUM(CASE WHEN final_outcome = 'text_only' THEN 1 ELSE 0 END), 0)::int as "textOnlyCount"
      FROM ranked_runs
      WHERE rn = 1
    `;
    const row = rows?.[0] || {};
    return {
      postgres: Number(row.postgresCount || 0),
      exa: Number(row.exaCount || 0),
      generated: Number(row.generatedCount || 0),
      textOnly: Number(row.textOnlyCount || 0)
    };
  } catch (_) {
    return { postgres: 0, exa: 0, generated: 0, textOnly: 0 };
  }
}

function fallbackContextScoreForTier(tier: Layer6Candidate["tier"]): number {
  if (tier === "postgres_pass1") return 7.2;
  if (tier === "postgres_pass2") return 6.6;
  if (tier === "exa") return 6.1;
  if (tier === "generated") return 5.4;
  return 6.4;
}

function fallbackQualityScoreForTier(tier: Layer6Candidate["tier"]): number {
  if (tier === "generated") return 5.2;
  if (tier === "exa") return 6.0;
  if (tier === "persona_fallback") return 6.4;
  return 6.8;
}

function isMissingRelationError(error: any, relationName: string): boolean {
  const code = cleanText(error?.code || "", 40);
  const message = cleanText(error?.message || "", 500).toLowerCase();
  if (code === "42P01") return true;
  return message.includes(`relation "${relationName.toLowerCase()}" does not exist`);
}

async function runLayer6PostgresSearch(
  sql: any,
  signal: ResearchSignalContext,
  tier: "postgres_pass1" | "postgres_pass2"
): Promise<Layer6Candidate[]> {
  const strict = tier === "postgres_pass1";
  const startedAt = Date.now();
  let rows: any[] = [];
  try {
    rows = await sql`
      SELECT
        image_url as "imageUrl",
        COALESCE(title, description, '') as "imageTitle",
        COALESCE(credit, '') as "imageCredit",
        COALESCE(license_source_url, '') as "sourceUrl",
        COALESCE(persona, '') as "persona",
        COALESCE(beat, '') as "beat",
        COALESCE(section, '') as "section",
        created_at as "createdAt"
      FROM media_library
      WHERE image_url IS NOT NULL
        AND trim(image_url) <> ''
        AND (${strict} = FALSE OR persona = ${signal.personaId})
        AND (${strict} = FALSE OR section = ${signal.personaSection})
        AND (${strict} = FALSE OR approved = TRUE)
        AND (${strict} = TRUE OR section = ${signal.personaSection} OR section IS NULL OR trim(section) = '')
      ORDER BY approved DESC, created_at DESC
      LIMIT ${strict ? 8 : 12}
    `;
  } catch (error: any) {
    if (isMissingRelationError(error, "media_library")) {
      return [];
    }
    throw error;
  }
  const queryLatencyMs = Math.max(0, Date.now() - startedAt);

  return (Array.isArray(rows) ? rows : [])
    .map((row: any, idx: number) => {
      const imageUrl = cleanText(row.imageUrl || "", 5000);
      if (!imageUrl) return null;
      return {
        id: randomUUID(),
        tier,
        source: "postgres_media_library",
        sourceUrl: cleanText(row.sourceUrl || "", 2000),
        imageUrl,
        imageTitle: cleanText(row.imageTitle || "", 600),
        imageCredit: cleanText(row.imageCredit || "", 300),
        attemptNumber: idx + 1,
        latencyMs: queryLatencyMs,
        costUsd: 0,
        contextScore: null,
        qualityScore: null,
        trustScore: LAYER6_TRUST_BY_TIER[tier],
        weightedScore: null,
        isRejected: false,
        rejectionReason: null,
        cloudinary: { publicId: null, secureUrl: null, metadata: {} },
        metadata: {
          persona: cleanText(row.persona || "", 255),
          beat: cleanText(row.beat || "", 120),
          section: cleanText(row.section || "", 120),
          createdAt: row.createdAt ? new Date(row.createdAt).toISOString() : null
        }
      } as Layer6Candidate;
    })
    .filter(Boolean) as Layer6Candidate[];
}

async function scoreContextWithGeminiForImage(
  signal: ResearchSignalContext,
  candidate: Layer6Candidate,
  timeoutMs: number
): Promise<number | null> {
  const geminiApiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  const openAiApiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
  const prompt = [
    "Rate image relevance for this local news draft context from 0-10.",
    "Return strict JSON only: {\"context_score\": number}",
    `Signal title: ${signal.title}`,
    `Signal snippet: ${signal.snippet || ""}`,
    `Persona section: ${signal.personaSection}`,
    `Image URL: ${candidate.imageUrl}`,
    `Image title: ${candidate.imageTitle}`,
    `Image credit: ${candidate.imageCredit}`
  ].join("\n");

  const parseContextScore = (text: string): number | null => {
    const parsed = safeJsonParse(text);
    const value = Number(parsed?.context_score);
    if (!Number.isFinite(value)) return null;
    return Math.min(Math.max(value, 0), 10);
  };

  if (geminiApiKey) {
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${encodeURIComponent(
      geminiApiKey
    )}`;
    const response = await fetchWithTimeout(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.1,
          maxOutputTokens: 120,
          responseMimeType: "application/json"
        }
      })
    }, timeoutMs);
    if (response.ok) {
      const data = await response.json();
      const text = data?.candidates?.[0]?.content?.parts?.map((p: any) => p?.text || "").join("") || "";
      const score = parseContextScore(text);
      if (score !== null) return score;
    }
  }

  if (!openAiApiKey) return null;
  try {
    const response = await fetchWithTimeout("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${openAiApiKey}`
      },
      body: JSON.stringify({
        model: HARD_CODED_GATEKEEPER_OPENAI_MODEL,
        temperature: 0.1,
        response_format: { type: "json_object" },
        messages: [{ role: "user", content: prompt }]
      })
    }, timeoutMs);
    if (!response.ok) return null;
    const data = await response.json();
    const text = data?.choices?.[0]?.message?.content || "";
    return parseContextScore(text);
  } catch (_) {
    return null;
  }
}

async function scoreEverypixelQuality(
  imageUrl: string,
  model: "stock" | "ugc",
  timeoutMs: number
): Promise<number | null> {
  const apiKey = cleanText(process.env.EVERYPIXEL_API_KEY || "", 200);
  const apiSecret = cleanText(process.env.EVERYPIXEL_API_SECRET || "", 200);
  if (!apiKey || !apiSecret || !imageUrl) return null;
  const auth = Buffer.from(`${apiKey}:${apiSecret}`).toString("base64");
  try {
    const response = await fetchWithTimeout("https://api.everypixel.com/v1/quality", {
      method: "POST",
      headers: {
        Authorization: `Basic ${auth}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ data: { url: imageUrl }, type: model })
    }, timeoutMs);
    if (!response.ok) return null;
    const data = await response.json();
    const score = Number(data?.quality?.score);
    if (!Number.isFinite(score)) return null;
    return Math.min(Math.max(score * 10, 0), 10);
  } catch (_) {
    return null;
  }
}

async function uploadImageUrlToCloudinary(imageUrl: string, timeoutMs: number): Promise<{
  publicId: string | null;
  secureUrl: string | null;
  metadata: Record<string, unknown>;
}> {
  const cloudName = cleanText(process.env.CLOUDINARY_CLOUD_NAME || "", 120) || cleanText(process.env.NEXT_PUBLIC_CLOUDINARY_CLOUD_NAME || "", 120);
  const uploadPreset = cleanText(process.env.CLOUDINARY_UPLOAD_PRESET || "", 200) || "dayton-enquirer";
  if (!cloudName || !imageUrl) return { publicId: null, secureUrl: null, metadata: {} };
  try {
    const form = new URLSearchParams();
    form.set("file", imageUrl);
    form.set("upload_preset", uploadPreset);
    const response = await fetchWithTimeout(`https://api.cloudinary.com/v1_1/${encodeURIComponent(cloudName)}/image/upload`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: form.toString()
    }, timeoutMs);
    if (!response.ok) return { publicId: null, secureUrl: null, metadata: {} };
    const data = await response.json();
    return {
      publicId: cleanText(data?.public_id || "", 500) || null,
      secureUrl: cleanText(data?.secure_url || "", 5000) || null,
      metadata: {
        width: Number(data?.width || 0) || null,
        height: Number(data?.height || 0) || null,
        format: cleanText(data?.format || "", 40) || null,
        bytes: Number(data?.bytes || 0) || null
      }
    };
  } catch (_) {
    return { publicId: null, secureUrl: null, metadata: {} };
  }
}

type Layer6MediaLibraryMetadata = {
  title: string;
  description: string;
  tags: string[];
  entities: string[];
  tone: string;
  credit: string;
  licenseType: string;
  licenseSourceUrl: string | null;
};

function normalizeMetadataList(value: unknown, maxItems = 20, maxLen = 80): string[] {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of value) {
    const item = cleanText(raw, maxLen);
    if (!item) continue;
    const key = item.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
    if (out.length >= maxItems) break;
  }
  return out;
}

function buildFallbackMediaLibraryMetadata(
  signal: ResearchSignalContext,
  candidate: Layer6Candidate
): Layer6MediaLibraryMetadata {
  const isGenerated = candidate.tier === "generated";
  return {
    title: cleanText(candidate.imageTitle || signal.title || "News image", 240),
    description: cleanText(
      candidate.imageTitle
        || signal.snippet
        || `Image selected for ${signal.personaSection} coverage.`,
      2000
    ),
    tags: normalizeMetadataList(
      [
        signal.personaSection,
        signal.personaBeat,
        isGenerated ? "ai-generated" : "sourced",
        "newsroom"
      ],
      12,
      60
    ),
    entities: normalizeMetadataList([signal.personaId, signal.personaSection, signal.personaBeat], 10, 80),
    tone: "neutral",
    credit: cleanText(
      candidate.imageCredit || (isGenerated ? "AI generated (Flux)" : "Sourced image"),
      240
    ),
    licenseType: isGenerated ? "ai_generated" : "editorial_sourced_web",
    licenseSourceUrl: cleanText(candidate.sourceUrl || "", 2000) || null
  };
}

function shouldGenerateMediaLibraryMetadata(candidate: Layer6Candidate): boolean {
  if (candidate.tier === "generated") return true;
  if (candidate.tier === "exa") {
    const hasTitle = Boolean(cleanText(candidate.imageTitle || "", 60));
    const hasCredit = Boolean(cleanText(candidate.imageCredit || "", 60));
    return !(hasTitle && hasCredit);
  }
  return false;
}

async function generateMediaLibraryMetadataWithGemini(
  signal: ResearchSignalContext,
  candidate: Layer6Candidate,
  timeoutMs: number
): Promise<Layer6MediaLibraryMetadata | null> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) return null;
  const model =
    cleanText(process.env.TOPIC_ENGINE_IMAGE_METADATA_GEMINI_MODEL || "", 120)
    || "gemini-1.5-flash";
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(
    apiKey
  )}`;
  const prompt = [
    "Generate newsroom image metadata as strict JSON only.",
    "Schema: {\"title\":\"...\",\"description\":\"...\",\"tags\":[\"...\"],\"entities\":[\"...\"],\"tone\":\"neutral|urgent|informative|analytical\",\"credit\":\"...\",\"licenseType\":\"...\",\"licenseSourceUrl\":\"...|null\"}",
    `Signal title: ${signal.title}`,
    `Signal snippet: ${signal.snippet || ""}`,
    `Section: ${signal.personaSection}`,
    `Beat: ${signal.personaBeat}`,
    `Candidate tier: ${candidate.tier}`,
    `Image URL: ${candidate.imageUrl}`,
    `Candidate title: ${candidate.imageTitle || ""}`,
    `Candidate credit: ${candidate.imageCredit || ""}`,
    `Candidate source URL: ${candidate.sourceUrl || ""}`
  ].join("\n");

  try {
    const response = await fetchWithTimeout(
      endpoint,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: 0.2,
            maxOutputTokens: 300,
            responseMimeType: "application/json"
          }
        })
      },
      timeoutMs
    );
    if (!response.ok) return null;
    const data = await response.json();
    const text = data?.candidates?.[0]?.content?.parts?.map((p: any) => p?.text || "").join("") || "";
    const parsed = safeJsonParse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    return {
      title: cleanText((parsed as any).title, 240),
      description: cleanText((parsed as any).description, 2000),
      tags: normalizeMetadataList((parsed as any).tags, 20, 80),
      entities: normalizeMetadataList((parsed as any).entities, 20, 80),
      tone: cleanText((parsed as any).tone, 80),
      credit: cleanText((parsed as any).credit, 240),
      licenseType: cleanText((parsed as any).licenseType, 120),
      licenseSourceUrl: cleanText((parsed as any).licenseSourceUrl, 2000) || null
    };
  } catch (_) {
    return null;
  }
}

async function generateMediaLibraryMetadataWithOpenAI(
  signal: ResearchSignalContext,
  candidate: Layer6Candidate,
  timeoutMs: number
): Promise<Layer6MediaLibraryMetadata | null> {
  const apiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
  if (!apiKey) return null;
  const endpoint = "https://api.openai.com/v1/chat/completions";
  const prompt = [
    "Return strict JSON only for newsroom image metadata.",
    "Schema: {\"title\":\"...\",\"description\":\"...\",\"tags\":[\"...\"],\"entities\":[\"...\"],\"tone\":\"neutral|urgent|informative|analytical\",\"credit\":\"...\",\"licenseType\":\"...\",\"licenseSourceUrl\":\"...|null\"}",
    `Signal title: ${signal.title}`,
    `Signal snippet: ${signal.snippet || ""}`,
    `Section: ${signal.personaSection}`,
    `Beat: ${signal.personaBeat}`,
    `Candidate tier: ${candidate.tier}`,
    `Image URL: ${candidate.imageUrl}`,
    `Candidate title: ${candidate.imageTitle || ""}`,
    `Candidate credit: ${candidate.imageCredit || ""}`,
    `Candidate source URL: ${candidate.sourceUrl || ""}`
  ].join("\n");

  try {
    const response = await fetchWithTimeout(
      endpoint,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`
        },
        body: JSON.stringify({
          model: "gpt-4o-mini",
          temperature: 0.2,
          messages: [{ role: "user", content: prompt }]
        })
      },
      timeoutMs
    );
    if (!response.ok) return null;
    const data = await response.json();
    const text = data?.choices?.[0]?.message?.content || "";
    const parsed = safeJsonParse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    return {
      title: cleanText((parsed as any).title, 240),
      description: cleanText((parsed as any).description, 2000),
      tags: normalizeMetadataList((parsed as any).tags, 20, 80),
      entities: normalizeMetadataList((parsed as any).entities, 20, 80),
      tone: cleanText((parsed as any).tone, 80),
      credit: cleanText((parsed as any).credit, 240),
      licenseType: cleanText((parsed as any).licenseType, 120),
      licenseSourceUrl: cleanText((parsed as any).licenseSourceUrl, 2000) || null
    };
  } catch (_) {
    return null;
  }
}

async function generateMediaLibraryMetadata(
  signal: ResearchSignalContext,
  candidate: Layer6Candidate,
  timeoutMs: number
): Promise<Layer6MediaLibraryMetadata> {
  const fallback = buildFallbackMediaLibraryMetadata(signal, candidate);
  const gemini = await generateMediaLibraryMetadataWithGemini(signal, candidate, timeoutMs);
  if (gemini) {
    return {
      ...fallback,
      ...gemini,
      tags: gemini.tags?.length ? gemini.tags : fallback.tags,
      entities: gemini.entities?.length ? gemini.entities : fallback.entities
    };
  }
  const openai = await generateMediaLibraryMetadataWithOpenAI(signal, candidate, timeoutMs);
  if (openai) {
    return {
      ...fallback,
      ...openai,
      tags: openai.tags?.length ? openai.tags : fallback.tags,
      entities: openai.entities?.length ? openai.entities : fallback.entities
    };
  }
  return fallback;
}

async function upsertSelectedLayer6ImageToMediaLibrary(
  sql: any,
  signal: ResearchSignalContext,
  selected: Layer6Candidate,
  timeoutMs: number
): Promise<void> {
  if (!(selected.tier === "exa" || selected.tier === "generated")) return;
  const imageUrl = cleanText(selected.cloudinary?.secureUrl || selected.imageUrl || "", 5000);
  if (!imageUrl) return;
  const metadata = shouldGenerateMediaLibraryMetadata(selected)
    ? await generateMediaLibraryMetadata(signal, selected, timeoutMs)
    : buildFallbackMediaLibraryMetadata(signal, selected);

  try {
    const existing = await sql`
      SELECT id
      FROM media_library
      WHERE image_url = ${imageUrl}
      ORDER BY id DESC
      LIMIT 1
    `;
    if (existing[0]?.id) {
      await sql`
        UPDATE media_library
        SET
          section = COALESCE(${signal.personaSection}, section),
          beat = COALESCE(${signal.personaBeat}, beat),
          persona = COALESCE(${signal.personaId}, persona),
          title = COALESCE(${metadata.title || null}, title),
          description = COALESCE(${metadata.description || null}, description),
          tags = CASE
            WHEN jsonb_array_length(${JSON.stringify(metadata.tags)}::jsonb) > 0 THEN ${JSON.stringify(metadata.tags)}::jsonb
            ELSE tags
          END,
          entities = CASE
            WHEN jsonb_array_length(${JSON.stringify(metadata.entities)}::jsonb) > 0 THEN ${JSON.stringify(metadata.entities)}::jsonb
            ELSE entities
          END,
          tone = COALESCE(${metadata.tone || null}, tone),
          image_public_id = COALESCE(${selected.cloudinary?.publicId || null}, image_public_id),
          credit = COALESCE(${metadata.credit || null}, credit),
          license_type = COALESCE(${metadata.licenseType || null}, license_type),
          license_source_url = COALESCE(${metadata.licenseSourceUrl || null}, license_source_url),
          updated_at = NOW()
        WHERE id = ${existing[0].id}
      `;
      return;
    }

    await sql`
      INSERT INTO media_library (
        section,
        beat,
        persona,
        title,
        description,
        tags,
        entities,
        tone,
        image_url,
        image_public_id,
        credit,
        license_type,
        license_source_url,
        approved,
        created_at,
        updated_at
      )
      VALUES (
        ${signal.personaSection || "local"},
        ${signal.personaBeat || null},
        ${signal.personaId || null},
        ${metadata.title || null},
        ${metadata.description || null},
        ${JSON.stringify(metadata.tags)}::jsonb,
        ${JSON.stringify(metadata.entities)}::jsonb,
        ${metadata.tone || null},
        ${imageUrl},
        ${selected.cloudinary?.publicId || null},
        ${metadata.credit || null},
        ${metadata.licenseType || null},
        ${metadata.licenseSourceUrl || null},
        ${false},
        NOW(),
        NOW()
      )
    `;
  } catch (error) {
    if (isMissingRelationError(error, "media_library")) return;
    throw error;
  }
}

function buildCloudinaryDeliveryUrl(publicId: string): string | null {
  const cloudName =
    cleanText(process.env.CLOUDINARY_CLOUD_NAME || "", 120)
    || cleanText(process.env.NEXT_PUBLIC_CLOUDINARY_CLOUD_NAME || "", 120);
  const rawPublicId = cleanText(publicId || "", 500);
  if (!cloudName || !rawPublicId) return null;
  const encodedPublicId = rawPublicId
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");
  return `https://res.cloudinary.com/${encodeURIComponent(cloudName)}/image/upload/f_auto,q_auto/${encodedPublicId}`;
}

async function runLayer6ExaSearch(
  signal: ResearchSignalContext,
  attempts: number,
  deadlineMs: number
): Promise<{ candidates: Layer6Candidate[]; attemptsMade: number }> {
  const exaApiKey = cleanText(process.env.EXA_API_KEY || "", 500);
  if (!exaApiKey || attempts <= 0) return { candidates: [], attemptsMade: 0 };
  const candidates: Layer6Candidate[] = [];
  let attemptsMade = 0;
  for (let i = 1; i <= attempts; i += 1) {
    if (Date.now() >= deadlineMs) break;
    attemptsMade += 1;
    const attemptStartedAt = Date.now();
    try {
      const response = await fetchWithTimeout("https://api.exa.ai/search", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${exaApiKey}`
        },
        body: JSON.stringify({
          query: `${signal.title} ${signal.personaSection} news image`,
          numResults: 6,
          type: "auto",
          contents: { text: { maxCharacters: 1200 } }
        })
      }, layer6RemainingTimeoutMs(deadlineMs, 7000));
      if (!response.ok) continue;
      const data = await response.json();
      const attemptLatencyMs = Math.max(0, Date.now() - attemptStartedAt);
      const results = Array.isArray(data?.results) ? data.results : [];
      for (const row of results) {
        const imageUrl = cleanText(row?.image || row?.imageUrl || "", 5000);
        if (!imageUrl) continue;
        candidates.push({
          id: randomUUID(),
          tier: "exa",
          source: "exa_search",
          sourceUrl: cleanText(row?.url || "", 2000),
          imageUrl,
          imageTitle: cleanText(row?.title || "", 600),
          imageCredit: cleanText(row?.author || "", 300),
          attemptNumber: i,
          latencyMs: attemptLatencyMs,
          costUsd: 0,
          contextScore: null,
          qualityScore: null,
          trustScore: LAYER6_TRUST_BY_TIER.exa,
          weightedScore: null,
          isRejected: false,
          rejectionReason: null,
          cloudinary: { publicId: null, secureUrl: null, metadata: {} },
          metadata: {
            exaId: cleanText(row?.id || "", 120),
            publishedDate: cleanText(row?.publishedDate || "", 120) || null
          }
        });
      }
      if (candidates.length) break;
    } catch (_) {
      // ignore and continue attempts
    }
  }
  return { candidates: candidates.slice(0, 12), attemptsMade };
}

function extractFluxImageUrl(payload: any): string {
  const direct = cleanText(
    payload?.imageUrl ||
      payload?.image_url ||
      payload?.url ||
      payload?.result?.imageUrl ||
      payload?.result?.image_url ||
      payload?.result?.sample ||
      payload?.data?.imageUrl ||
      payload?.data?.image_url ||
      payload?.data?.url ||
      "",
    5000
  );
  if (direct) return direct;

  const firstArrayHit = [
    ...(Array.isArray(payload?.images) ? payload.images : []),
    ...(Array.isArray(payload?.result?.images) ? payload.result.images : []),
    ...(Array.isArray(payload?.data?.images) ? payload.data.images : [])
  ]
    .map((row: any) => cleanText(row?.url || row?.imageUrl || row?.image_url || "", 5000))
    .find(Boolean);
  return firstArrayHit || "";
}

function extractFluxRequestId(payload: any): string {
  return cleanText(
    payload?.id ||
      payload?.request_id ||
      payload?.requestId ||
      payload?.job_id ||
      payload?.jobId ||
      payload?.task_id ||
      payload?.taskId ||
      payload?.result?.id ||
      "",
    300
  );
}

function extractFluxPollingUrl(baseUrl: string, payload: any, requestId: string): string {
  const explicit = cleanText(
    payload?.polling_url || payload?.pollUrl || payload?.status_url || payload?.result_url || payload?.urls?.result || "",
    2000
  );
  if (explicit) return explicit;
  if (!requestId) return "";
  try {
    const origin = new URL(baseUrl).origin;
    return `${origin}/v1/get_result?id=${encodeURIComponent(requestId)}`;
  } catch (_) {
    return "";
  }
}

function sleepMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function resolveFluxImageUrlViaPolling(payload: {
  baseUrl: string;
  initialResponse: any;
  fluxApiKey: string;
  deadlineMs: number;
}): Promise<string> {
  const requestId = extractFluxRequestId(payload.initialResponse);
  const pollingUrl = extractFluxPollingUrl(payload.baseUrl, payload.initialResponse, requestId);
  if (!pollingUrl) return "";

  const maxPolls = 12;
  for (let poll = 1; poll <= maxPolls; poll += 1) {
    if (Date.now() >= payload.deadlineMs) break;
    if (poll > 1) {
      await sleepMs(1200);
      if (Date.now() >= payload.deadlineMs) break;
    }

    try {
      const response = await fetchWithTimeout(
        pollingUrl,
        {
          method: "GET",
          headers: {
            ...(payload.fluxApiKey
              ? {
                  Authorization: `Bearer ${payload.fluxApiKey}`,
                  "x-api-key": payload.fluxApiKey,
                  "x-key": payload.fluxApiKey
                }
              : {})
          }
        },
        layer6RemainingTimeoutMs(payload.deadlineMs, 6000)
      );
      if (!response.ok) continue;
      const pollData = await response.json();
      const imageUrl = extractFluxImageUrl(pollData);
      if (imageUrl) return imageUrl;

      const status = cleanText(
        pollData?.status || pollData?.state || pollData?.result?.status || pollData?.task_status || "",
        80
      ).toLowerCase();
      if (status === "failed" || status === "error" || status === "cancelled" || status === "canceled") return "";
    } catch (_) {
      // keep polling until deadline/max polls
    }
  }
  return "";
}

async function runLayer6GeneratedFallback(
  signal: ResearchSignalContext,
  attempts: number,
  deadlineMs: number
): Promise<{ candidates: Layer6Candidate[]; attemptsMade: number }> {
  const baseUrl = cleanText(process.env.FLUX_GENERATE_ENDPOINT || "", 2000);
  const fluxApiKey = cleanText(process.env.FLUX_API_KEY || "", 500);
  if (!baseUrl || attempts <= 0) return { candidates: [], attemptsMade: 0 };
  const out: Layer6Candidate[] = [];
  let attemptsMade = 0;
  for (let i = 1; i <= attempts; i += 1) {
    if (Date.now() >= deadlineMs) break;
    attemptsMade += 1;
    const attemptStartedAt = Date.now();
    try {
      const response = await fetchWithTimeout(baseUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(fluxApiKey
            ? { Authorization: `Bearer ${fluxApiKey}`, "x-api-key": fluxApiKey, "x-key": fluxApiKey }
            : {})
        },
        body: JSON.stringify({
          prompt: `${signal.title}. Editorial, documentary photo style, local newsroom realism.`,
          signalId: signal.id,
          personaId: signal.personaId
        })
      }, layer6RemainingTimeoutMs(deadlineMs, 8000));
      if (!response.ok) continue;
      const data = await response.json();
      const attemptLatencyMs = Math.max(0, Date.now() - attemptStartedAt);
      let imageUrl = extractFluxImageUrl(data);
      if (!imageUrl) {
        imageUrl = await resolveFluxImageUrlViaPolling({
          baseUrl,
          initialResponse: data,
          fluxApiKey,
          deadlineMs
        });
      }
      if (!imageUrl) continue;
      out.push({
        id: randomUUID(),
        tier: "generated",
        source: "flux_generation",
        sourceUrl: "",
        imageUrl,
        imageTitle: cleanText(signal.title, 600),
        imageCredit: "Generated",
        attemptNumber: i,
        latencyMs: attemptLatencyMs,
        costUsd: Number(data?.costUsd || 0) || 0,
        contextScore: null,
        qualityScore: null,
        trustScore: LAYER6_TRUST_BY_TIER.generated,
        weightedScore: null,
        isRejected: false,
        rejectionReason: null,
        cloudinary: { publicId: null, secureUrl: null, metadata: {} },
        metadata: { provider: cleanText(data?.provider || "flux", 80), model: cleanText(data?.model || "", 120) }
      });
    } catch (_) {
      // ignore generation attempt failure
    }
  }
  return { candidates: out, attemptsMade };
}

function buildLayer6DraftWriteQueries(
  sql: any,
  payload: {
    signal: ResearchSignalContext;
    outcome: "postgres_selected" | "exa_selected" | "generated_selected" | "persona_fallback" | "text_only";
    selected: Layer6Candidate | null;
    imageStatus: "with_image" | "text_only";
  }
): any[] {
  const queries: any[] = [];
  const sourceUrl = `signal://${payload.signal.id}/draft`;
  const selectedImageUrl = cleanText(
    payload.selected?.cloudinary.secureUrl || payload.selected?.imageUrl || "",
    5000
  );
  const selectedImageTitle = cleanText(payload.selected?.imageTitle || "", 800);
  const selectedImageCredit = cleanText(payload.selected?.imageCredit || "", 300);

  if (selectedImageUrl) {
    queries.push(sql`
      WITH latest_draft AS (
        SELECT id
        FROM research_artifacts
        WHERE signal_id = ${payload.signal.id}
          AND stage = 'draft_writing'
          AND artifact_type = 'draft_package'
          AND source_url = ${sourceUrl}
        ORDER BY created_at DESC
        LIMIT 1
      )
      UPDATE research_artifacts ra
      SET metadata = jsonb_set(
        jsonb_set(
          COALESCE(ra.metadata, '{}'::jsonb),
          '{draft}',
          COALESCE(ra.metadata->'draft', '{}'::jsonb) || jsonb_build_object(
            'image', ${selectedImageUrl},
            'imageCaption', ${selectedImageTitle || ""},
            'imageCredit', ${selectedImageCredit || ""}
          ),
          true
        ),
        '{layer6_image}',
        ${toSafeJsonObject({
          status: payload.imageStatus,
          outcome: payload.outcome,
          selectedTier: payload.selected?.tier || null,
          selectedImageUrl: selectedImageUrl || null,
          selectedCloudinaryPublicId: payload.selected?.cloudinary.publicId || null,
          selectedSourceUrl: payload.selected?.sourceUrl || null,
          updatedAt: new Date().toISOString()
        })}::jsonb,
        true
      )
      FROM latest_draft
      WHERE ra.id = latest_draft.id
    `);

    queries.push(sql`
      WITH latest_draft_row AS (
        SELECT id
        FROM article_drafts
        WHERE source_url = ${sourceUrl}
          AND COALESCE(status, 'pending_review') <> 'published'
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
        LIMIT 1
      )
      UPDATE article_drafts ad
      SET
        image = ${selectedImageUrl},
        image_caption = ${selectedImageTitle || ""},
        image_credit = ${selectedImageCredit || ""},
        updated_at = NOW()
      FROM latest_draft_row ldr
      WHERE ad.id = ldr.id
    `);
    return queries;
  }

  queries.push(sql`
    WITH latest_draft AS (
      SELECT id
      FROM research_artifacts
      WHERE signal_id = ${payload.signal.id}
        AND stage = 'draft_writing'
        AND artifact_type = 'draft_package'
        AND source_url = ${sourceUrl}
      ORDER BY created_at DESC
      LIMIT 1
    )
    UPDATE research_artifacts ra
    SET metadata = jsonb_set(
      COALESCE(ra.metadata, '{}'::jsonb),
      '{layer6_image}',
      ${toSafeJsonObject({
        status: payload.imageStatus,
        outcome: payload.outcome,
        selectedTier: payload.selected?.tier || null,
        selectedImageUrl: null,
        selectedCloudinaryPublicId: payload.selected?.cloudinary.publicId || null,
        selectedSourceUrl: payload.selected?.sourceUrl || null,
        updatedAt: new Date().toISOString()
      })}::jsonb,
      true
    )
    FROM latest_draft
    WHERE ra.id = latest_draft.id
  `);

  queries.push(sql`
    WITH latest_draft_row AS (
      SELECT id
      FROM article_drafts
      WHERE source_url = ${sourceUrl}
        AND COALESCE(status, 'pending_review') <> 'published'
      ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
      LIMIT 1
    )
    UPDATE article_drafts ad
    SET
      image = '',
      image_caption = '',
      image_credit = '',
      updated_at = NOW()
    FROM latest_draft_row ldr
    WHERE ad.id = ldr.id
  `);
  return queries;
}

async function persistLayer6DraftAndTelemetryAtomic(payload: {
  runId: string;
  signal: ResearchSignalContext;
  status: "completed" | "failed" | "timed_out";
  outcome: "postgres_selected" | "exa_selected" | "generated_selected" | "persona_fallback" | "text_only";
  selectedCandidateId: string | null;
  selected: Layer6Candidate | null;
  candidates: Layer6Candidate[];
  attemptsPostgres: number;
  attemptsExa: number;
  attemptsGeneration: number;
  totalCostUsd: number;
  latencyMsTotal: number;
  budgetLimit: number;
  timeoutSeconds: number;
  rejectionReasons: string[];
  imageStatus: "with_image" | "text_only";
  sourceEventId?: string | null;
  trigger?: string | null;
}): Promise<void> {
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const selectedCanonicalImageUrl = payload.selected?.cloudinary.secureUrl || payload.selected?.imageUrl || null;
  const queries: any[] = [];
  queries.push(...buildLayer6DraftWriteQueries(sql, {
    signal: payload.signal,
    outcome: payload.outcome,
    selected: payload.selected,
    imageStatus: payload.imageStatus
  }));

  queries.push(sql`
      INSERT INTO image_pipeline_runs (
        id,
        signal_id,
        persona_id,
        started_at,
        completed_at,
        status,
        final_outcome,
        selected_candidate_id,
        selected_tier,
        selected_image_url,
        selected_image_credit,
        selected_source_url,
        selected_cloudinary_public_id,
        selected_cloudinary_secure_url,
        selected_cloudinary_asset_metadata,
        attempts_postgres,
        attempts_exa,
        attempts_generation,
        latency_ms_total,
        cost_usd_estimated,
        budget_usd_limit,
        timeout_seconds_limit,
        rejection_reasons,
        diagnostics,
        created_at,
        updated_at
      )
      VALUES (
        ${payload.runId},
        ${payload.signal.id},
        ${payload.signal.personaId},
        NOW(),
        NOW(),
        ${payload.status},
        ${payload.outcome},
        ${null},
        ${payload.selected?.tier || null},
        ${selectedCanonicalImageUrl},
        ${payload.selected?.imageCredit || null},
        ${payload.selected?.sourceUrl || null},
        ${payload.selected?.cloudinary.publicId || null},
        ${payload.selected?.cloudinary.secureUrl || null},
        ${toSafeJsonObject(payload.selected?.cloudinary.metadata || {})}::jsonb,
        ${payload.attemptsPostgres},
        ${payload.attemptsExa},
        ${payload.attemptsGeneration},
        ${payload.latencyMsTotal},
        ${payload.totalCostUsd},
        ${payload.budgetLimit},
        ${payload.timeoutSeconds},
        ${payload.rejectionReasons}::jsonb,
        ${toSafeJsonObject({
          stage: "layer6_image_sourcing",
          sourceEventId: payload.sourceEventId || null,
          trigger: payload.trigger || null,
          idempotencyCanonical: true
        })}::jsonb,
        NOW(),
        NOW()
      )
      ON CONFLICT (id) DO UPDATE
      SET
        completed_at = EXCLUDED.completed_at,
        status = EXCLUDED.status,
        final_outcome = EXCLUDED.final_outcome,
        selected_candidate_id = EXCLUDED.selected_candidate_id,
        selected_tier = EXCLUDED.selected_tier,
        selected_image_url = EXCLUDED.selected_image_url,
        selected_image_credit = EXCLUDED.selected_image_credit,
        selected_source_url = EXCLUDED.selected_source_url,
        selected_cloudinary_public_id = EXCLUDED.selected_cloudinary_public_id,
        selected_cloudinary_secure_url = EXCLUDED.selected_cloudinary_secure_url,
        selected_cloudinary_asset_metadata = EXCLUDED.selected_cloudinary_asset_metadata,
        attempts_postgres = EXCLUDED.attempts_postgres,
        attempts_exa = EXCLUDED.attempts_exa,
        attempts_generation = EXCLUDED.attempts_generation,
        latency_ms_total = EXCLUDED.latency_ms_total,
        cost_usd_estimated = EXCLUDED.cost_usd_estimated,
        rejection_reasons = EXCLUDED.rejection_reasons,
        diagnostics = EXCLUDED.diagnostics,
        updated_at = NOW()
    `);

  for (const candidate of payload.candidates) {
    queries.push(sql`
        INSERT INTO image_candidates (
          id,
          run_id,
          signal_id,
          persona_id,
          candidate_tier,
          candidate_source,
          source_url,
          image_url,
          image_title,
          image_credit,
          cloudinary_public_id,
          cloudinary_secure_url,
          cloudinary_asset_metadata,
          context_score,
          quality_score,
          trust_score,
          weighted_score,
          score_components,
          confidence,
          is_selected,
          selected_rank,
          rejected,
          rejection_reason,
          rejection_details,
          attempt_number,
          latency_ms,
          cost_usd_estimated,
          metadata,
          created_at,
          updated_at
        )
        VALUES (
          ${candidate.id},
          ${payload.runId},
          ${payload.signal.id},
          ${payload.signal.personaId},
          ${candidate.tier},
          ${candidate.source || null},
          ${candidate.sourceUrl || null},
          ${candidate.imageUrl || null},
          ${candidate.imageTitle || null},
          ${candidate.imageCredit || null},
          ${candidate.cloudinary.publicId || null},
          ${candidate.cloudinary.secureUrl || null},
          ${toSafeJsonObject(candidate.cloudinary.metadata)}::jsonb,
          ${candidate.contextScore},
          ${candidate.qualityScore},
          ${candidate.trustScore},
          ${candidate.weightedScore},
          ${toSafeJsonObject({
            context: candidate.contextScore,
            quality: candidate.qualityScore,
            trust: candidate.trustScore
          })}::jsonb,
          ${candidate.weightedScore},
          ${candidate.id === payload.selectedCandidateId},
          ${candidate.id === payload.selectedCandidateId ? 1 : null},
          ${candidate.isRejected},
          ${candidate.rejectionReason || null},
          ${toSafeJsonObject({ reason: candidate.rejectionReason || null })}::jsonb,
          ${candidate.attemptNumber},
          ${candidate.latencyMs},
          ${candidate.costUsd},
          ${toSafeJsonObject(candidate.metadata)}::jsonb,
          NOW(),
          NOW()
        )
        ON CONFLICT (id) DO UPDATE
        SET
          context_score = EXCLUDED.context_score,
          quality_score = EXCLUDED.quality_score,
          trust_score = EXCLUDED.trust_score,
          weighted_score = EXCLUDED.weighted_score,
          score_components = EXCLUDED.score_components,
          confidence = EXCLUDED.confidence,
          is_selected = EXCLUDED.is_selected,
          selected_rank = EXCLUDED.selected_rank,
          rejected = EXCLUDED.rejected,
          rejection_reason = EXCLUDED.rejection_reason,
          rejection_details = EXCLUDED.rejection_details,
          latency_ms = EXCLUDED.latency_ms,
          cost_usd_estimated = EXCLUDED.cost_usd_estimated,
          cloudinary_public_id = EXCLUDED.cloudinary_public_id,
          cloudinary_secure_url = EXCLUDED.cloudinary_secure_url,
          cloudinary_asset_metadata = EXCLUDED.cloudinary_asset_metadata,
          metadata = EXCLUDED.metadata,
          updated_at = NOW()
      `);
  }

  if (payload.selectedCandidateId) {
    queries.push(sql`
      UPDATE image_pipeline_runs
      SET
        selected_candidate_id = ${payload.selectedCandidateId},
        selected_tier = ${payload.selected?.tier || null},
        selected_image_url = ${selectedCanonicalImageUrl},
        selected_image_credit = ${payload.selected?.imageCredit || null},
        selected_source_url = ${payload.selected?.sourceUrl || null},
        selected_cloudinary_public_id = ${payload.selected?.cloudinary.publicId || null},
        selected_cloudinary_secure_url = ${payload.selected?.cloudinary.secureUrl || null},
        selected_cloudinary_asset_metadata = ${toSafeJsonObject(payload.selected?.cloudinary.metadata || {})}::jsonb,
        updated_at = NOW()
      WHERE id = ${payload.runId}
    `);
  }

  await sql.transaction(queries);
}

async function runLayer6ImageSourcing(
  signalId: number,
  options?: { sourceEventId?: string | null; trigger?: string | null }
): Promise<{
  outcome: "postgres_selected" | "exa_selected" | "generated_selected" | "persona_fallback" | "text_only";
  imageStatus: "with_image" | "text_only";
  selectedImageUrl: string | null;
  selectedTier: string | null;
  selectedCloudinaryPublicId: string | null;
  candidateCount: number;
}> {
  const started = Date.now();
  const stageStartedAt = Date.now();
  const signal = await loadResearchSignalContext(signalId);
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const personaConfig = await loadLayer6PersonaConfig(sql, signal.personaId);
  const runtimePolicy = buildLayer6RuntimePolicy(personaConfig);
  const sourceEventId = cleanText(options?.sourceEventId || "", 200) || null;
  const trigger = cleanText(options?.trigger || "", 80) || "image_sourcing_event";
  const toReplayOutcome = (
    value: string
  ): "postgres_selected" | "exa_selected" | "generated_selected" | "persona_fallback" | "text_only" | null => {
    const normalized = cleanText(value || "", 80).toLowerCase();
    if (
      normalized === "postgres_selected"
      || normalized === "exa_selected"
      || normalized === "generated_selected"
      || normalized === "persona_fallback"
      || normalized === "text_only"
    ) {
      return normalized as "postgres_selected" | "exa_selected" | "generated_selected" | "persona_fallback" | "text_only";
    }
    return null;
  };
  const toReplayResult = (priorRun: any): {
    outcome: "postgres_selected" | "exa_selected" | "generated_selected" | "persona_fallback" | "text_only";
    imageStatus: "with_image" | "text_only";
    selectedImageUrl: string | null;
    selectedTier: string | null;
    selectedCloudinaryPublicId: string | null;
    candidateCount: number;
  } | null => {
    if (!priorRun) return null;
    const priorStatus = cleanText(priorRun.status || "", 40).toLowerCase();
    if (priorStatus !== "completed" && priorStatus !== "timed_out") return null;
    const replayOutcome = toReplayOutcome(priorRun.finalOutcome || "");
    if (!replayOutcome) return null;
    return {
      outcome: replayOutcome,
      imageStatus: replayOutcome === "text_only" ? "text_only" : "with_image",
      selectedImageUrl: cleanText(priorRun.selectedImageUrl || "", 5000) || null,
      selectedTier: cleanText(priorRun.selectedTier || "", 80) || null,
      selectedCloudinaryPublicId: cleanText(priorRun.selectedCloudinaryPublicId || "", 500) || null,
      candidateCount: Number(priorRun.candidateCount || 0)
    };
  };
  const loadReplayRun = async () => {
    if (!sourceEventId) return null;
    const priorRunRows = await sql`
      SELECT
        status,
        final_outcome as "finalOutcome",
        selected_tier as "selectedTier",
        selected_image_url as "selectedImageUrl",
        selected_cloudinary_public_id as "selectedCloudinaryPublicId",
        (
          SELECT COUNT(*)::int
          FROM image_candidates c
          WHERE c.run_id = image_pipeline_runs.id
        ) as "candidateCount"
      FROM image_pipeline_runs
      WHERE signal_id = ${signalId}
        AND diagnostics->>'sourceEventId' = ${sourceEventId}
        AND diagnostics ? 'sourceEventId'
        AND jsonb_typeof(COALESCE(diagnostics, '{}'::jsonb)) = 'object'
        AND trim(COALESCE(diagnostics->>'sourceEventId', '')) <> ''
        AND COALESCE(diagnostics->>'idempotencyCanonical', 'false') = 'true'
      ORDER BY COALESCE(updated_at, created_at, started_at) DESC, id DESC
      LIMIT 1
    `;
    return priorRunRows?.[0] || null;
  };
  if (sourceEventId) {
    const priorRun = await loadReplayRun();
    const replayResult = toReplayResult(priorRun);
    if (replayResult) return replayResult;
  }
  const dailyUsage = await loadLayer6DailyUsage(sql, signal.personaId);
  const runId = randomUUID();
  const deadlineMs = stageStartedAt + personaConfig.layer6TimeoutSeconds * 1000;
  const isTimedOut = () => Date.now() > deadlineMs;
  const allowPostgres = runtimePolicy.allowPostgres && dailyUsage.postgres < personaConfig.quotaPostgresImageDaily;
  const allowExa = runtimePolicy.allowExa && dailyUsage.exa < personaConfig.quotaSourcedImageDaily;
  const allowGenerated = runtimePolicy.allowGenerated && dailyUsage.generated < personaConfig.quotaGeneratedImageDaily;
  const allowTextOnly = dailyUsage.textOnly < personaConfig.quotaTextOnlyDaily;

  const candidates: Layer6Candidate[] = [];
  let attemptsPostgres = 0;
  let attemptsExa = 0;
  let attemptsGeneration = 0;
  let forcedTimeout = false;
  let forcedBudgetStop = false;

  if (personaConfig.imageDbEnabled && allowPostgres && !isTimedOut()) {
    const pass1 = await runLayer6PostgresSearch(sql, signal, "postgres_pass1");
    attemptsPostgres += 1;
    candidates.push(...pass1);

    if (!pass1.length && !isTimedOut()) {
      const pass2 = await runLayer6PostgresSearch(sql, signal, "postgres_pass2");
      attemptsPostgres += 1;
      candidates.push(...pass2);
    }
  }

  if (isTimedOut()) forcedTimeout = true;

  if (!candidates.length && allowExa && !isTimedOut()) {
    const exaResult = await runLayer6ExaSearch(signal, runtimePolicy.exaAttempts, deadlineMs);
    attemptsExa = exaResult.attemptsMade;
    candidates.push(...exaResult.candidates);
  }

  if (isTimedOut()) forcedTimeout = true;

  if (!candidates.length && allowGenerated && !isTimedOut()) {
    const generatedResult = await runLayer6GeneratedFallback(signal, runtimePolicy.generationAttempts, deadlineMs);
    attemptsGeneration = generatedResult.attemptsMade;
    candidates.push(...generatedResult.candidates);
  }

  const personaFallbackImageUrl =
    cleanText(personaConfig.imageFallbackAssetUrl || "", 5000)
    || buildCloudinaryDeliveryUrl(cleanText(personaConfig.imageFallbackCloudinaryPublicId || "", 500))
    || "";
  const hasPersonaFallback = Boolean(personaFallbackImageUrl);
  if (!candidates.length && hasPersonaFallback) {
    candidates.push({
      id: randomUUID(),
      tier: "persona_fallback",
      source: "persona_fallback",
      sourceUrl: "",
      imageUrl: personaFallbackImageUrl,
      imageTitle: signal.title,
      imageCredit: "",
      attemptNumber: 1,
      latencyMs: 0,
      costUsd: 0,
      contextScore: null,
      qualityScore: null,
      trustScore: LAYER6_TRUST_BY_TIER.persona_fallback,
      weightedScore: null,
      isRejected: false,
      rejectionReason: null,
      cloudinary: {
        publicId: cleanText(personaConfig.imageFallbackCloudinaryPublicId || "", 500) || null,
        secureUrl: null,
        metadata: {}
      },
      metadata: { personaFallback: true }
    });
  }

  const rejectionReasons: string[] = [];
  let runningCostUsd = 0;
  for (const candidate of candidates) {
    if (isTimedOut()) {
      forcedTimeout = true;
      candidate.isRejected = true;
      candidate.rejectionReason = "layer6_timeout";
      rejectionReasons.push("layer6_timeout");
      continue;
    }
    if (runningCostUsd + Number(candidate.costUsd || 0) > runtimePolicy.budgetUsd) {
      forcedBudgetStop = true;
      candidate.isRejected = true;
      candidate.rejectionReason = "layer6_budget_exceeded";
      rejectionReasons.push("layer6_budget_exceeded");
      continue;
    }
    runningCostUsd += Number(candidate.costUsd || 0);

    const contextStartedAt = Date.now();
    candidate.contextScore = await scoreContextWithGeminiForImage(
      signal,
      candidate,
      layer6RemainingTimeoutMs(deadlineMs, 6000)
    );
    candidate.latencyMs += Math.max(0, Date.now() - contextStartedAt);
    if (candidate.tier === "exa" || candidate.tier === "generated") {
      const qualityStartedAt = Date.now();
      candidate.qualityScore = await scoreEverypixelQuality(
        candidate.imageUrl,
        candidate.tier === "generated" ? "ugc" : "stock",
        layer6RemainingTimeoutMs(deadlineMs, 5000)
      );
      candidate.latencyMs += Math.max(0, Date.now() - qualityStartedAt);
    } else if (!Number.isFinite(Number(candidate.qualityScore))) {
      candidate.qualityScore = fallbackQualityScoreForTier(candidate.tier);
    }
    if (!Number.isFinite(Number(candidate.contextScore))) {
      candidate.contextScore = fallbackContextScoreForTier(candidate.tier);
      rejectionReasons.push("context_scoring_unavailable_used_fallback");
    }
    if (!Number.isFinite(Number(candidate.qualityScore))) {
      candidate.qualityScore = fallbackQualityScoreForTier(candidate.tier);
    }
    if (Number(candidate.contextScore) < 5) {
      candidate.isRejected = true;
      candidate.rejectionReason = "context_below_threshold";
      rejectionReasons.push("context_below_threshold");
      continue;
    }
    candidate.weightedScore = scoreWeightedImageCandidate(candidate);
  }

  let selected = chooseLayer6Winner(candidates);
  if (!selected && !allowTextOnly && hasPersonaFallback) {
    const forcedFallback = candidates.find((candidate) => candidate.tier === "persona_fallback");
    if (forcedFallback) {
      if (!Number.isFinite(Number(forcedFallback.contextScore))) {
        forcedFallback.contextScore = Math.max(5, fallbackContextScoreForTier("persona_fallback"));
      }
      if (!Number.isFinite(Number(forcedFallback.qualityScore))) {
        forcedFallback.qualityScore = fallbackQualityScoreForTier("persona_fallback");
      }
      if (Number(forcedFallback.contextScore) >= 5) {
        forcedFallback.isRejected = false;
        forcedFallback.rejectionReason = null;
        forcedFallback.weightedScore = scoreWeightedImageCandidate(forcedFallback);
        selected = forcedFallback;
        rejectionReasons.push("text_only_quota_forced_persona_fallback");
      } else {
        forcedFallback.isRejected = true;
        forcedFallback.rejectionReason = "context_below_threshold";
        rejectionReasons.push("forced_persona_fallback_blocked_context_threshold");
      }
    }
  }
  let outcome: "postgres_selected" | "exa_selected" | "generated_selected" | "persona_fallback" | "text_only" =
    "text_only";
  if (selected) {
    if (selected.tier === "postgres_pass1" || selected.tier === "postgres_pass2") outcome = "postgres_selected";
    else if (selected.tier === "exa") outcome = "exa_selected";
    else if (selected.tier === "generated") outcome = "generated_selected";
    else outcome = "persona_fallback";
  }

  if (!selected && !allowTextOnly && !hasPersonaFallback) {
    rejectionReasons.push("text_only_quota_reached_no_fallback_available");
    rejectionReasons.push("text_only_quota_exceeded_publish_override");
  }
  if (!selected && !allowTextOnly && hasPersonaFallback) {
    rejectionReasons.push("text_only_quota_reached_fallback_not_selected");
    rejectionReasons.push("text_only_quota_exceeded_publish_override");
  }

  const hasReusablePersonaFallbackCloudinary =
    selected?.tier === "persona_fallback"
    && Boolean(cleanText(selected?.cloudinary?.publicId || "", 500));
  if (
    selected?.imageUrl
    && !isTimedOut()
    && !hasReusablePersonaFallbackCloudinary
  ) {
    const uploadStartedAt = Date.now();
    const uploaded = await uploadImageUrlToCloudinary(
      selected.imageUrl,
      layer6RemainingTimeoutMs(deadlineMs, 8000)
    );
    selected.cloudinary = {
      publicId: uploaded.publicId || selected.cloudinary.publicId || null,
      secureUrl: uploaded.secureUrl || selected.cloudinary.secureUrl || null,
      metadata: Object.keys(uploaded.metadata || {}).length
        ? uploaded.metadata
        : (selected.cloudinary.metadata || {})
    };
    selected.latencyMs += Math.max(0, Date.now() - uploadStartedAt);
  } else if (hasReusablePersonaFallbackCloudinary && selected) {
    // Persona fallback can already reference a stable Cloudinary asset; avoid duplicate uploads.
    selected.cloudinary = {
      publicId: cleanText(selected.cloudinary?.publicId || "", 500) || null,
      secureUrl:
        selected.cloudinary?.secureUrl
        || buildCloudinaryDeliveryUrl(cleanText(selected.cloudinary?.publicId || "", 500))
        || null,
      metadata: selected.cloudinary?.metadata || {}
    };
  } else if (isTimedOut()) {
    forcedTimeout = true;
    rejectionReasons.push("layer6_timeout_before_cloudinary");
  }

  if (selected?.imageUrl && !isTimedOut()) {
    try {
      await upsertSelectedLayer6ImageToMediaLibrary(
        sql,
        signal,
        selected,
        layer6RemainingTimeoutMs(deadlineMs, 6500)
      );
    } catch (error) {
      rejectionReasons.push("media_library_upsert_failed");
    }
  }

  const imageStatus = normalizeLayer6Outcome(selected?.imageUrl ? "with_image" : "text_only");
  const totalCostUsd = Number(
    candidates.reduce((sum, c) => sum + Number(c.costUsd || 0), 0).toFixed(4)
  );
  try {
    await persistLayer6DraftAndTelemetryAtomic({
      runId,
      signal,
      status: forcedTimeout ? "timed_out" : "completed",
      outcome,
      selectedCandidateId: selected?.id || null,
      selected: selected || null,
      candidates,
      attemptsPostgres,
      attemptsExa,
      attemptsGeneration,
      totalCostUsd,
      latencyMsTotal: Math.max(0, Date.now() - started),
      budgetLimit: runtimePolicy.budgetUsd,
      timeoutSeconds: personaConfig.layer6TimeoutSeconds,
      imageStatus,
      sourceEventId,
      trigger,
      rejectionReasons: Array.from(
        new Set([
          ...rejectionReasons,
          forcedBudgetStop ? "layer6_budget_stop" : "",
          forcedTimeout ? "layer6_timeout" : ""
        ].filter(Boolean))
      )
    });
  } catch (error: any) {
    const message = cleanText(error?.message || "", 1000).toLowerCase();
    const constraintName = cleanText(error?.constraint || "", 200);
    const isIdempotencyConflict =
      sourceEventId
      && (
        constraintName === "uq_image_pipeline_runs_signal_source_event"
        || message.includes("uq_image_pipeline_runs_signal_source_event")
      );
    if (!isIdempotencyConflict) throw error;
    const priorRun = await loadReplayRun();
    const replayResult = toReplayResult(priorRun);
    if (!replayResult) throw error;
    return replayResult;
  }

  return {
    outcome,
    imageStatus,
    selectedImageUrl: selected?.cloudinary.secureUrl || selected?.imageUrl || null,
    selectedTier: selected?.tier || null,
    selectedCloudinaryPublicId: selected?.cloudinary.publicId || null,
    candidateCount: candidates.length
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
      personaSection: "local",
      personaBeat: "general-local",
      beatPolicy: {
        includeKeywords: [],
        excludeKeywords: [],
        requiredLocalTerms: []
      },
      metadata: {
        testMode: true,
        signalId: TEST_SIGNAL_ID
      },
      createdAt: new Date().toISOString(),
      isAutoPromoteEnabled: true,
      dictionaryContext: {
        snapshot: null,
        entityMatches: [],
        jurisdictions: [],
        eligibleAssertions: []
      },
      normalizationContext: {
        snapshotId: null,
        snapshotVersion: null,
        normalizedEntities: [],
        normalizedEntityIds: [],
        canonicalNames: [],
        canonicalSlugs: [],
        aliasTexts: [],
        jurisdictionIds: [],
        jurisdictionNames: [],
        assertionSummaries: []
      },
      localityContext: {
        isLocalByDictionary: false,
        canBypassLocalTermGate: false,
        confidence: "none",
        evidenceTypes: [],
        matchedJurisdictionNames: [],
        matchedEntityIds: []
      }
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
      COALESCE(NULLIF(trim(to_jsonb(p)->>'section'), ''), COALESCE(s.section_hint, ''), 'local') as "personaSection",
      COALESCE(NULLIF(trim(to_jsonb(p)->>'beat'), ''), '') as "personaBeat",
      COALESCE(to_jsonb(p)->'beat_policy', '{}'::jsonb) as "beatPolicy",
      COALESCE(s.metadata, '{}'::jsonb) as "metadata",
      s.created_at as "createdAt",
      COALESCE(te.is_auto_promote_enabled, false) as "isAutoPromoteEnabled"
    FROM topic_signals s
    LEFT JOIN topic_engines te
      ON te.persona_id = s.persona_id
    LEFT JOIN personas p
      ON p.id = s.persona_id
    WHERE s.id = ${signalId}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row) {
    throw new Error(`Signal ${signalId} not found`);
  }

  const dictionaryContext = await loadLayer1DictionaryContext(sql, {
    title: cleanText(row.title, 500),
    snippet: cleanText(row.snippet, 8000),
    sectionHint: cleanText(row.sectionHint, 255)
  });
  const normalizationContext = buildLayer1NormalizationContext(dictionaryContext);
  const localityContext = buildLayer1LocalityContext(normalizationContext);

  return {
    id: Number(row.id),
    personaId: cleanText(row.personaId, 255),
    sourceType: cleanText(row.sourceType, 30) as SourceType,
    title: cleanText(row.title, 500),
    snippet: cleanText(row.snippet, 8000),
    sectionHint: cleanText(row.sectionHint, 255),
    personaSection: cleanText(row.personaSection, 80).toLowerCase() || "local",
    personaBeat: cleanText(row.personaBeat, 120).toLowerCase(),
    beatPolicy: parseBeatPolicy(row.beatPolicy),
    metadata: toSafeJsonObject(row.metadata),
    createdAt: new Date(row.createdAt).toISOString(),
    isAutoPromoteEnabled: Boolean(row.isAutoPromoteEnabled),
    dictionaryContext,
    normalizationContext,
    localityContext
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
  const groupingProbes = buildLayer1GroupingProbes(signal.normalizationContext);

  const baseArticleRows = await sql`
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

  const baseCandidateRows = await sql`
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

  const [anchorArticleRows, anchorCandidateRows] = groupingProbes.length
    ? await Promise.all([
        sql`
          SELECT
            'article'::text as "sourceType",
            a.id::text as "sourceId",
            a.slug::text as "sourceSlug",
            COALESCE(a.title, '')::text as "title",
            COALESCE(a.description, '')::text as "snippet",
            COALESCE(a.section, '')::text as "section",
            COALESCE(a.pub_date, a.created_at, NOW()) as "occurredAt"
          FROM articles a
          WHERE COALESCE(a.pub_date, a.created_at, NOW()) >= NOW() - interval '120 days'
          ORDER BY COALESCE(a.pub_date, a.created_at) DESC
          LIMIT 150
        `,
        sql`
          SELECT
            'candidate'::text as "sourceType",
            c.id::text as "sourceId",
            NULL::text as "sourceSlug",
            COALESCE(c.title, '')::text as "title",
            COALESCE(c.snippet, '')::text as "snippet",
            NULL::text as "section",
            COALESCE(c.published_at, c.created_at, NOW()) as "occurredAt"
          FROM topic_engine_candidates c
          WHERE c.persona_id = ${signal.personaId}
            AND COALESCE(c.published_at, c.created_at, NOW()) >= NOW() - interval '30 days'
          ORDER BY COALESCE(c.published_at, c.created_at) DESC
          LIMIT 150
        `
      ])
    : [[], []];

  return [...baseArticleRows, ...baseCandidateRows, ...anchorArticleRows, ...anchorCandidateRows]
    .map((row: any): PriorArtMatch | null => {
      const normalizedTitle = cleanText(row.title || "", 600);
      const normalizedSnippet = cleanText(row.snippet || "", 1200);
      const score = scorePriorArtMatch(
        normalizedTitle,
        normalizedSnippet,
        title,
        snippetProbe,
        groupingProbes
      );
      const matchedByExistingHeuristics =
        containsWholePhrase(normalizedTitle, title) || containsWholePhrase(normalizedSnippet, snippetProbe);
      const matchedByGroupingProbes =
        textMatchesGroupingProbe(normalizedTitle, groupingProbes)
        || textMatchesGroupingProbe(normalizedSnippet, groupingProbes);
      if (!matchedByExistingHeuristics && !matchedByGroupingProbes) return null;

      return {
      sourceType: (row.sourceType === "candidate" ? "candidate" : "article") as "article" | "candidate",
      sourceId: cleanText(row.sourceId, 80),
      sourceSlug: cleanText(row.sourceSlug || "", 255) || null,
      title: normalizedTitle,
      snippet: normalizedSnippet,
      section: cleanText(row.section || "", 120) || null,
      occurredAt: new Date(row.occurredAt || Date.now()).toISOString(),
      score
      };
    })
    .filter((row: PriorArtMatch | null): row is PriorArtMatch => Boolean(row))
    .sort((a, b) => b.score - a.score || Date.parse(b.occurredAt) - Date.parse(a.occurredAt))
    .filter(
      (row, index, collection) =>
        collection.findIndex((candidate) => candidate.sourceType === row.sourceType && candidate.sourceId === row.sourceId)
        === index
    )
    .slice(0, 3);
}

// STEP 3: check_corroboration_pre_ai
async function checkCorroborationPreAI(signal: SignalRecord): Promise<CorroborationSummary> {
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const titleProbe = cleanText(signal.title, 220);
  const snippetProbe = cleanText(signal.snippet.split(/\s+/).slice(0, 12).join(" "), 220);
  const groupingProbes = buildLayer1GroupingProbes(signal.normalizationContext);

  const corroborationRows = await sql`
    SELECT
      s.id,
      s.source_type as "sourceType",
      s.session_hash as "sessionHash",
      COALESCE(s.title, '') as "title",
      COALESCE(s.snippet, '') as "snippet"
    FROM topic_signals s
    WHERE s.persona_id = ${signal.personaId}
      AND s.id <> ${signal.id}
      AND s.created_at >= NOW() - interval '24 hours'
    ORDER BY s.created_at DESC
    LIMIT 250
  `;

  const matchedRows = corroborationRows.filter((row: any) => {
    const rowTitle = cleanText(row.title || "", 600);
    const rowSnippet = cleanText(row.snippet || "", 1200);
    const matchedByExistingHeuristics =
      containsWholePhrase(rowTitle, titleProbe) || containsWholePhrase(rowSnippet, snippetProbe);
    const matchedByGroupingProbes =
      textMatchesGroupingProbe(rowTitle, groupingProbes) || textMatchesGroupingProbe(rowSnippet, groupingProbes);
    return matchedByExistingHeuristics || matchedByGroupingProbes;
  });

  return {
    similarSignals24h: matchedRows.length,
    distinctSourceTypes24h: Array.from(
      new Set(matchedRows.map((row: any) => cleanText(row.sourceType, 30)).filter(Boolean))
    ),
    distinctChatSessions24h: new Set(
      matchedRows
        .filter((row: any) => row.sourceType === "chat_yes" || row.sourceType === "chat_specify")
        .map((row: any) => cleanText(row.sessionHash || "", 128))
        .filter(Boolean)
    ).size
  };
}

// STEP 4: gatekeeper_classify (Gemini 1.5 Flash)
async function classifyWithGatekeeper(
  signal: SignalRecord,
  priorArt: PriorArtMatch[],
  corroboration: CorroborationSummary
): Promise<GatekeeperOutput> {
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const guidanceBundle = await buildStageGuidanceBundle(
    sql,
    "topic_qualification",
    signal.personaId,
    signal.personaSection
  );

  const geminiApiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  const openAiApiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
  const promptSafeSignal = buildPromptSafeSignalPayload(signal);
  const dictionaryPromptContext = buildGatekeeperDictionaryPromptContext(signal);
  const prompt = [
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion
      ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}`
      : "",
    "You are a local newsroom gatekeeper classifier.",
    "Return strict JSON only.",
    "Schema:",
    "{\"is_newsworthy\":0-1,\"is_local\":true|false,\"confidence\":0-1,\"category\":\"...\",\"relation_to_archive\":\"none|duplicate|update|follow_up\",\"event_key\":\"...\",\"action\":\"reject|watch|promote\",\"next_step\":\"none|research_discovery|cluster_update|story_planning\",\"policy_flags\":[\"...\"],\"reasoning\":\"...\"}",
    "Rules:",
    "- Prefer watch over promote when evidence is thin.",
    "- Keep next_step consistent with action.",
    "- event_key should be a stable short key for same event family.",
    "- Use dictionary context as read-only published-snapshot grounding; do not assume anything beyond it.",
    "",
    `Signal: ${JSON.stringify(promptSafeSignal)}`,
    `Dictionary context: ${JSON.stringify(dictionaryPromptContext)}`,
    `Prior art: ${JSON.stringify(priorArt)}`,
    `Corroboration: ${JSON.stringify(corroboration)}`
  ].join("\n");

  let text = "";
  const gatekeeperErrors: string[] = [];

  if (geminiApiKey) {
    const model =
      cleanText(process.env.TOPIC_ENGINE_GATEKEEPER_GEMINI_MODEL || "", 120) ||
      HARD_CODED_GATEKEEPER_MODEL;
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
      model
    )}:generateContent?key=${encodeURIComponent(geminiApiKey)}`;
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
      gatekeeperErrors.push(`gemini:${model}:${response.status}:${body.slice(0, 220)}`);
    } else {
      const data = await response.json();
      text =
        data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
      if (!cleanText(text, 20).length) {
        gatekeeperErrors.push(`gemini:${model}:empty_response`);
      }
    }
  }

  if (!cleanText(text, 20).length && openAiApiKey) {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${openAiApiKey}`
      },
      body: JSON.stringify({
        model: HARD_CODED_GATEKEEPER_OPENAI_MODEL,
        temperature: 0.1,
        response_format: { type: "json_object" },
        messages: [{ role: "user", content: prompt }]
      })
    });
    if (!response.ok) {
      const body = await response.text();
      gatekeeperErrors.push(`openai:${HARD_CODED_GATEKEEPER_OPENAI_MODEL}:${response.status}:${body.slice(0, 220)}`);
    } else {
      const data = await response.json();
      text = data?.choices?.[0]?.message?.content || "";
    }
  }

  if (!cleanText(text, 20).length) {
    throw new Error(
      `Gatekeeper classify failed: ${gatekeeperErrors.length ? gatekeeperErrors.join(" | ") : "no_provider_available"}`
    );
  }

  const parsed = safeJsonParse(text) || {};

  const relation = ["none", "duplicate", "update", "follow_up"].includes(String(parsed.relation_to_archive || ""))
    ? (parsed.relation_to_archive as RelationToArchive)
    : "none";
  const action = ["reject", "watch", "promote"].includes(String(parsed.action || ""))
    ? (parsed.action as Action)
    : "watch";
  const nextStep = ["none", "research_discovery", "cluster_update", "story_planning"].includes(
    String(parsed.next_step || "")
  )
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
async function persistDecision(signal: SignalRecord, decision: GatekeeperOutput): Promise<PersistedDecision> {
  const signalId = signal.id;
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
  const observabilityMetadata = buildDecisionObservabilityMetadata(signal);

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
      metadata = COALESCE(topic_signals.metadata, '{}'::jsonb) || ${toSafeJsonObject(observabilityMetadata)}::jsonb,
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
  if (decision.next_step === "story_planning") {
    await step.sendEvent("emit-evidence-extraction-start-direct", {
      name: "evidence.extraction.start",
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
export function createQuotaPacingIntakeFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "quota-pacing-intake" },
    { event: "signal.received" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const signal = await step.run("load-signal-for-pacing", async () => loadSignalById(signalId));
      const queueDecision = await step.run("apply-quota-pacing-gate", async () =>
        applyQuotaPacingGate(signalId, signal.personaId)
      );

      if (queueDecision.decision === "released" || queueDecision.decision === "pass_through") {
        await step.sendEvent("emit-gatekeeper-start", {
          name: "signal.gatekeeper.start",
          data: {
            signalId,
            personaId: signal.personaId,
            trigger:
              queueDecision.decision === "pass_through"
                ? "signal_received_pass_through"
                : "signal_received_released"
          }
        });
      }

      return {
        ok: true,
        signalId,
        personaId: signal.personaId,
        decision: queueDecision.decision,
        reasonCode: queueDecision.reasonCode,
        scheduledForUtc: queueDecision.scheduledForUtc
      };
    }
  );
}

export function createQuotaPacingReleaseSchedulerFunction(inngest: Inngest) {
  return inngest.createFunction(
    {
      id: "quota-pacing-release-scheduler",
      concurrency: { limit: 1 }
    },
    { cron: "*/10 * * * *" },
    async ({ step }: any) => {
      const due = await step.run("release-due-queued-signals", async () => releaseDueQueuedSignals(50));
      for (const item of due) {
        await step.sendEvent(`emit-gatekeeper-start-${item.signalId}`, {
          name: "signal.gatekeeper.start",
          data: {
            signalId: item.signalId,
            personaId: item.personaId,
            trigger: "quota_pacing_scheduler"
          }
        });
      }
      return {
        ok: true,
        releasedCount: due.length
      };
    }
  );
}

export function createGatekeeperPipeline(inngest: Inngest) {
  return inngest.createFunction(
    { id: "gatekeeper-pipeline" },
    { event: "signal.gatekeeper.start" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const signal = await step.run("1-load_signal", async () => loadSignalById(signalId));
      const policyShortCircuit = await step.run("1b-apply_beat_policy_prefilter", async () =>
        applyBeatPolicyPreFilter(signal)
      );
      if (policyShortCircuit) {
        const persisted = await step.run("1c-persist_policy_prefilter", async () =>
          persistDecision(signal, policyShortCircuit)
        );
        return {
          ok: true,
          signalId,
          action: persisted.action,
          nextStep: persisted.next_step,
          policyShortCircuit: true
        };
      }
      const priorArt = await step.run("2-lookup_prior_art", async () => lookupPriorArt(signal));
      const corroboration = await step.run("3-check_corroboration_pre_ai", async () => checkCorroborationPreAI(signal));
      const modelOut = await step.run("4-gatekeeper_classify", async () =>
        classifyWithGatekeeper(signal, priorArt, corroboration)
      );
      const guarded = await step.run("5-apply_guardrails", async () =>
        applyGatekeeperGuardrails(signal, modelOut, corroboration)
      );
      const persisted = await step.run("6-persist_decision", async () => persistDecision(signal, guarded));
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
      if (result.status === "failed") {
        return {
          ok: false,
          signalId,
          ...result
        };
      }
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

export function createResearchDiscoveryRetryFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "research-discovery-retry" },
    { event: "research.discovery.retry" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("research-discovery-retry", async () => runResearchDiscovery(signalId));
      if (result.status === "failed") {
        return {
          ok: false,
          signalId,
          ...result,
          reason: cleanText(event?.data?.reason || "needs_reporting", 80),
          missingInformation: Array.isArray(event?.data?.missingInformation) ? event.data.missingInformation : [],
          queries: Array.isArray(event?.data?.queries) ? event.data.queries : []
        };
      }
      await step.sendEvent("emit-evidence-extraction-start-from-retry", {
        name: "evidence.extraction.start",
        data: {
          signalId,
          trigger: "research_retry"
        }
      });
      return {
        ok: true,
        signalId,
        ...result,
        reason: cleanText(event?.data?.reason || "needs_reporting", 80),
        missingInformation: Array.isArray(event?.data?.missingInformation) ? event.data.missingInformation : [],
        queries: Array.isArray(event?.data?.queries) ? event.data.queries : []
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
      if (result.executionOutcome === "validated" && result.extractionStatus === "READY" && result.isCanonical) {
        await step.sendEvent("emit-story-planning-start", {
          name: "story.planning.start",
          data: {
            signalId,
            trigger: cleanText(event?.data?.trigger || "evidence_ready", 80)
          }
        });
      } else if (result.extractionStatus === "NEEDS_REPORTING") {
        await step.sendEvent("emit-research-discovery-retry-from-evidence", {
          name: "research.discovery.retry",
          data: {
            signalId,
            personaId: cleanText(event?.data?.personaId || "", 255) || null,
            reason: "phase_3_needs_reporting",
            missingInformation: Array.isArray(result?.missingEvidence) ? result.missingEvidence : [],
            queries: Array.isArray(result?.followUpQueries) ? result.followUpQueries : [],
            editorialRisks: Array.isArray(result?.editorialRisks) ? result.editorialRisks : []
          }
        });
      }
      return {
        ok: true,
        signalId,
        ...result
      };
    }
  );
}

export function createStoryPlanningStartFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "story-planning-start" },
    { event: "story.planning.start" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("story-planning", async () => runStoryPlanning(signalId));
      if (result.executionOutcome === "validated" && result.planningStatus === "READY" && result.isCanonical) {
        await step.sendEvent("emit-draft-writing-start", {
          name: "draft.writing.start",
          data: {
            signalId
          }
        });
      } else if (result.executionOutcome === "validated" && result.planningStatus === "NEEDS_REPORTING") {
        await step.sendEvent("emit-research-discovery-retry", {
          name: "research.discovery.retry",
          data: {
            signalId,
            personaId: cleanText(event?.data?.personaId || "", 255) || null,
            reason: "needs_reporting",
            missingInformation: Array.isArray((result as any)?.missingInformation) ? (result as any).missingInformation : [],
            queries: Array.isArray((result as any)?.missingInformationQueries) ? (result as any).missingInformationQueries : [],
            editorialRisks: Array.isArray((result as any)?.editorialRisks) ? (result as any).editorialRisks : []
          }
        });
      }
      return {
        ok: true,
        signalId,
        ...result
      };
    }
  );
}

export function createDraftWritingStartFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "draft-writing-start" },
    { event: "draft.writing.start" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("draft-writing", async () => runDraftWriting(signalId));
      await step.sendEvent("emit-image-sourcing-start", {
        name: "image.sourcing.start",
        data: {
          signalId,
          trigger: "draft_writing"
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

export function createImageSourcingStartFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "image-sourcing-start" },
    { event: "image.sourcing.start" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("layer6-image-sourcing", async () =>
        runLayer6ImageSourcing(signalId, {
          sourceEventId: cleanText(event?.id || "", 200) || null,
          trigger: cleanText(event?.data?.trigger || "image_sourcing_event", 80) || "image_sourcing_event"
        })
      );
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
      const targetStep =
        nextStep === "cluster_update"
          ? "cluster_update"
          : nextStep === "story_planning"
            ? "story_planning"
          : nextStep === "research_discovery"
            ? "research_discovery"
            : action === "promote"
              ? "research_discovery"
              : "none";

      if (targetStep === "none") {
        return {
          ok: true,
          signalId,
          routed: false,
          reason: "next_step_not_routable"
        };
      }

      if (targetStep === "cluster_update") {
        await step.sendEvent("emit-cluster-update-from-manual", {
          name: "cluster.update.start",
          data: {
            signalId,
            trigger: "admin_manual"
          }
        });
        return {
          ok: true,
          signalId,
          routed: true,
          targetEvent: "cluster.update.start"
        };
      }

      if (targetStep === "story_planning") {
        await step.sendEvent("emit-evidence-extraction-from-manual", {
          name: "evidence.extraction.start",
          data: {
            signalId,
            trigger: "admin_manual"
          }
        });
        return {
          ok: true,
          signalId,
          routed: true,
          targetEvent: "evidence.extraction.start"
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
