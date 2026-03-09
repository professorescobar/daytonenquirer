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

type StoryPlanningEvidence = {
  claim: string;
  sourceUrl: string;
  evidenceQuote: string;
  confidence: number;
  whyItMatters: string;
};

type StoryPlanSection = {
  heading: string;
  summary: string;
  evidenceSourceUrls: string[];
};

type StoryPlanArtifact = {
  angle: string;
  narrativeStrategy: string;
  sections: StoryPlanSection[];
  uncertaintyNotes: string[];
  missingInformation: string[];
};

type DraftWritingArtifact = {
  headline: string;
  dek: string;
  body: string;
  sourceUrls: string[];
  uncertaintyNotes: string[];
  coverageGaps: string[];
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

function isTestSignalId(signalId: number): boolean {
  return TEST_MODE_ENABLED && signalId === TEST_SIGNAL_ID;
}

function cleanText(value: unknown, max = 8000): string {
  return String(value || "").trim().slice(0, max);
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
      personaSection: "local",
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
    title: cleanText(row.title, 500),
    snippet: cleanText(row.snippet || "", 4000) || null,
    sourceName: cleanText(row.sourceName || "", 500) || null,
    sourceUrl: cleanText(row.sourceUrl || "", 2000) || null,
    eventKey: cleanText(row.eventKey || "", 500) || null,
    dedupeKey: cleanText(row.dedupeKey || "", 500) || null
  };
}

async function generateResearchQueries(
  signal: ResearchSignalContext,
  guidanceBundle: StagePromptBundle
): Promise<string[]> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

  const model = HARD_CODED_RESEARCH_QUERY_MODEL;
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    model
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const prompt = [
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion
      ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}`
      : "",
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
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const guidanceBundle = await buildStageGuidanceBundle(
    sql,
    "research_discovery",
    signal.personaId,
    signal.personaSection
  );
  const queries = await generateResearchQueries(signal, guidanceBundle);
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
  sources: EvidenceSource[],
  guidanceBundle: StagePromptBundle
): Promise<EvidenceClaim[]> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

  const candidateModels = [...HARD_CODED_EVIDENCE_MODEL_CANDIDATES];
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
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion
      ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}`
      : "",
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

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const guidanceBundle = await buildStageGuidanceBundle(
    sql,
    "evidence_extraction",
    signal.personaId,
    signal.personaSection
  );
  const rawClaims = await extractEvidenceClaimsWithGemini(signal, sources, guidanceBundle);
  const allowedUrls = new Set(sources.map((source) => source.sourceUrl.toLowerCase()));
  const claims = normalizeEvidenceClaims(rawClaims, allowedUrls);
  const saved = await persistEvidenceArtifacts(signal, claims);

  return {
    sourceCount: sources.length,
    claimCount: claims.length,
    saved
  };
}

async function loadStoryPlanningEvidence(signalId: number): Promise<StoryPlanningEvidence[]> {
  if (isTestSignalId(signalId)) {
    return [
      {
        claim: "Downtown lane closures begin Saturday morning and continue through Sunday night.",
        sourceUrl: "https://example.com/mock-signal-12345",
        evidenceQuote: "Dayton public works said lane closures start Saturday morning.",
        confidence: 0.89,
        whyItMatters: "Weekend drivers and businesses downtown will need detour plans."
      },
      {
        claim: "RTA is shifting stops for two routes near utility work zones.",
        sourceUrl: "https://example.com/mock-signal-12345-traffic",
        evidenceQuote: "RTA said two routes will shift stops near utility work zones.",
        confidence: 0.84,
        whyItMatters: "Transit riders need updated stop locations and timing expectations."
      }
    ];
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);
  const rows = await sql`
    SELECT
      content as "claim",
      source_url as "sourceUrl",
      COALESCE(metadata->>'evidenceQuote', '') as "evidenceQuote",
      COALESCE(metadata->>'whyItMatters', '') as "whyItMatters",
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
      claim: cleanText(row.claim || "", 1200),
      sourceUrl: cleanText(row.sourceUrl || "", 2000),
      evidenceQuote: cleanText(row.evidenceQuote || "", 400),
      confidence: clampConfidence(row.confidence),
      whyItMatters: cleanText(row.whyItMatters || "", 700)
    }))
    .filter((item: StoryPlanningEvidence) => item.claim && item.sourceUrl && item.evidenceQuote);
}

function normalizeUrlList(value: unknown, allowedUrls: Set<string>): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of value) {
    const cleaned = cleanText(raw, 2000);
    if (!cleaned) continue;
    const key = cleaned.toLowerCase();
    if (!allowedUrls.has(key) || seen.has(key)) continue;
    seen.add(key);
    out.push(cleaned);
    if (out.length >= 4) break;
  }
  return out;
}

function normalizeStoryPlan(
  raw: any,
  evidence: StoryPlanningEvidence[]
): StoryPlanArtifact {
  const allowedUrls = new Set(evidence.map((item) => item.sourceUrl.toLowerCase()));
  const sectionsRaw = Array.isArray(raw?.sections) ? raw.sections : [];
  const sections: StoryPlanSection[] = [];

  for (const item of sectionsRaw) {
    const heading = cleanText(item?.heading || "", 180);
    const summary = cleanText(item?.summary || "", 700);
    if (!heading || !summary) continue;
    const evidenceSourceUrls = normalizeUrlList(item?.evidenceSourceUrls, allowedUrls);
    sections.push({ heading, summary, evidenceSourceUrls });
    if (sections.length >= 8) break;
  }

  const uncertaintyNotes = Array.isArray(raw?.uncertaintyNotes)
    ? raw.uncertaintyNotes.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 8)
    : [];
  const missingInformation = Array.isArray(raw?.missingInformation)
    ? raw.missingInformation.map((item: unknown) => cleanText(item, 240)).filter(Boolean).slice(0, 8)
    : [];

  const fallbackAngle = cleanText(raw?.angle || "", 280) || "What changed, who is affected, and why it matters now.";
  const fallbackNarrativeStrategy =
    cleanText(raw?.narrativeStrategy || "", 360) ||
    "Lead with the most immediate verified impact, then provide context, timeline, and practical implications.";

  const normalizedSections = sections.length
    ? sections
    : evidence.slice(0, 3).map((item, index) => ({
        heading: `Section ${index + 1}`,
        summary: cleanText(item.claim, 700),
        evidenceSourceUrls: [item.sourceUrl]
      }));

  return {
    angle: fallbackAngle,
    narrativeStrategy: fallbackNarrativeStrategy,
    sections: normalizedSections,
    uncertaintyNotes,
    missingInformation
  };
}

async function buildStoryPlanWithOpenAi(
  signal: ResearchSignalContext,
  evidence: StoryPlanningEvidence[],
  guidanceBundle: StagePromptBundle
): Promise<StoryPlanArtifact> {
  const apiKey = cleanText(process.env.OPENAI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing OPENAI_API_KEY");

  const evidenceContext = evidence
    .slice(0, 8)
    .map((item, index) =>
      [
        `Evidence ${index + 1}:`,
        `Claim: ${item.claim}`,
        `Source URL: ${item.sourceUrl}`,
        `Quote: ${item.evidenceQuote}`,
        `Confidence: ${item.confidence}`,
        `Why it matters: ${item.whyItMatters}`
      ].join("\n")
    )
    .join("\n\n");
  const prompt = [
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion
      ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}`
      : "",
    "You are creating a structured newsroom story plan from verified evidence only.",
    "Return strict JSON only in this schema:",
    "{\"angle\":\"...\",\"narrativeStrategy\":\"...\",\"sections\":[{\"heading\":\"...\",\"summary\":\"...\",\"evidenceSourceUrls\":[\"...\"]}],\"uncertaintyNotes\":[\"...\"],\"missingInformation\":[\"...\"]}",
    "Rules:",
    "- Use only evidence provided below.",
    "- Include 3 to 6 sections.",
    "- Keep each section summary concrete and publication-oriented.",
    "- Every section must include at least one evidenceSourceUrls item that exactly matches provided source URLs.",
    "- uncertaintyNotes and missingInformation may be empty arrays.",
    "- No markdown. No additional keys.",
    "",
    `Signal Title: ${signal.title}`,
    `Signal Snippet: ${signal.snippet || ""}`,
    "",
    evidenceContext
  ].join("\n");

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
    throw new Error(`OpenAI story planning failed ${response.status}: ${body.slice(0, 240)}`);
  }
  const data = await response.json();
  const text = data?.choices?.[0]?.message?.content || "";
  const parsed = safeJsonParse(text) || {};
  return normalizeStoryPlan(parsed, evidence);
}

async function buildStoryPlanWithGemini(
  signal: ResearchSignalContext,
  evidence: StoryPlanningEvidence[],
  guidanceBundle: StagePromptBundle
): Promise<StoryPlanArtifact> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

  const evidenceContext = evidence
    .slice(0, 8)
    .map((item, index) =>
      [
        `Evidence ${index + 1}:`,
        `Claim: ${item.claim}`,
        `Source URL: ${item.sourceUrl}`,
        `Quote: ${item.evidenceQuote}`,
        `Confidence: ${item.confidence}`,
        `Why it matters: ${item.whyItMatters}`
      ].join("\n")
    )
    .join("\n\n");
  const prompt = [
    guidanceBundle.compiledPrompt ? `Stage Guidance:\n${guidanceBundle.compiledPrompt}` : "",
    guidanceBundle.promptSourceVersion
      ? `Guidance Source Version: ${guidanceBundle.promptSourceVersion}`
      : "",
    "You are creating a structured newsroom story plan from verified evidence only.",
    "Return strict JSON only in this schema:",
    "{\"angle\":\"...\",\"narrativeStrategy\":\"...\",\"sections\":[{\"heading\":\"...\",\"summary\":\"...\",\"evidenceSourceUrls\":[\"...\"]}],\"uncertaintyNotes\":[\"...\"],\"missingInformation\":[\"...\"]}",
    "Rules:",
    "- Use only evidence provided below.",
    "- Include 3 to 6 sections.",
    "- Keep each section summary concrete and publication-oriented.",
    "- Every section must include at least one evidenceSourceUrls item that exactly matches provided source URLs.",
    "- uncertaintyNotes and missingInformation may be empty arrays.",
    "- No markdown. No additional keys.",
    "",
    `Signal Title: ${signal.title}`,
    `Signal Snippet: ${signal.snippet || ""}`,
    "",
    evidenceContext
  ].join("\n");

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
    throw new Error(`Gemini story planning failed ${response.status}: ${body.slice(0, 240)}`);
  }
  const data = await response.json();
  const text = data?.candidates?.[0]?.content?.parts?.map((part: any) => part?.text || "").join("") || "";
  const parsed = safeJsonParse(text) || {};
  return normalizeStoryPlan(parsed, evidence);
}

async function buildStoryPlan(
  signal: ResearchSignalContext,
  evidence: StoryPlanningEvidence[],
  guidanceBundle: StagePromptBundle
): Promise<{ plan: StoryPlanArtifact; provider: "openai" | "gemini"; model: string }> {
  const hasOpenAi = Boolean(cleanText(process.env.OPENAI_API_KEY || "", 20));
  const hasGemini = Boolean(cleanText(process.env.GEMINI_API_KEY || "", 20));
  if (hasOpenAi) {
    try {
      const plan = await buildStoryPlanWithOpenAi(signal, evidence, guidanceBundle);
      return {
        plan,
        provider: "openai",
        model: HARD_CODED_STORY_PLANNING_OPENAI_MODEL
      };
    } catch (error) {
      if (!hasGemini) throw error;
    }
  }
  const plan = await buildStoryPlanWithGemini(signal, evidence, guidanceBundle);
  return {
    plan,
    provider: "gemini",
    model: HARD_CODED_STORY_PLANNING_GEMINI_MODEL
  };
}

async function persistStoryPlanArtifact(
  signal: ResearchSignalContext,
  plan: StoryPlanArtifact,
  provider: "openai" | "gemini",
  model: string,
  evidenceCount: number
): Promise<number> {
  if (isTestSignalId(signal.id)) {
    console.log("test-mode persistStoryPlanArtifact skip", {
      signalId: signal.id,
      provider,
      model,
      evidenceCount,
      plan
    });
    return 1;
  }

  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 2000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  const sql = neon(databaseUrl);

  const runId = randomUUID();
  const engineId = randomUUID();
  const candidateId = randomUUID();
  const sourceUrl = `signal://${signal.id}/story-plan`;
  const metadata = {
    signalId: signal.id,
    personaId: signal.personaId,
    provider,
    model,
    evidenceCount,
    sectionCount: plan.sections.length,
    uncertaintyCount: plan.uncertaintyNotes.length,
    missingInformationCount: plan.missingInformation.length,
    plan
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
      'story_planning',
      'story_plan',
      ${sourceUrl},
      ${null},
      ${`Story plan for signal ${signal.id}`},
      ${null},
      ${plan.angle},
      ${toSafeJsonObject(metadata)}::jsonb,
      NOW()
    WHERE NOT EXISTS (
      SELECT 1
      FROM research_artifacts ra
      WHERE ra.signal_id = ${signal.id}
        AND ra.stage = 'story_planning'
        AND ra.artifact_type = 'story_plan'
        AND ra.source_url = ${sourceUrl}
    )
    RETURNING id
  `;

  return rows.length;
}

async function runStoryPlanning(signalId: number): Promise<{
  evidenceCount: number;
  sectionCount: number;
  saved: number;
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
  const storyPlanResult = await buildStoryPlan(signal, evidence, guidanceBundle);
  const saved = await persistStoryPlanArtifact(
    signal,
    storyPlanResult.plan,
    storyPlanResult.provider,
    storyPlanResult.model,
    evidence.length
  );

  return {
    evidenceCount: evidence.length,
    sectionCount: storyPlanResult.plan.sections.length,
    saved,
    provider: storyPlanResult.provider,
    model: storyPlanResult.model
  };
}

async function loadLatestStoryPlanArtifact(
  signalId: number,
  evidence: StoryPlanningEvidence[]
): Promise<StoryPlanArtifact | null> {
  if (isTestSignalId(signalId)) {
    return normalizeStoryPlan(
      {
        angle: "Downtown Dayton weekend closures impact drivers and transit riders",
        narrativeStrategy:
          "Lead with closure timing and direct impacts, then transit adjustments and remaining uncertainty.",
        sections: [
          {
            heading: "What is changing this weekend",
            summary: "City crews will close select downtown lanes starting Saturday morning through Sunday night.",
            evidenceSourceUrls: ["https://example.com/mock-signal-12345"]
          },
          {
            heading: "Transit and commute impact",
            summary: "RTA says two routes will temporarily shift stops near utility work zones.",
            evidenceSourceUrls: ["https://example.com/mock-signal-12345-traffic"]
          },
          {
            heading: "What residents should do now",
            summary: "Drivers and riders should check detours and service notices before heading downtown.",
            evidenceSourceUrls: [
              "https://example.com/mock-signal-12345",
              "https://example.com/mock-signal-12345-traffic"
            ]
          }
        ],
        uncertaintyNotes: ["Exact reopening timing may shift based on field progress."],
        missingInformation: ["Intersection-level closure windows were not fully published."]
      },
      evidence
    );
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
    ORDER BY created_at DESC
    LIMIT 1
  `;

  const metadata = toSafeJsonObject(rows?.[0]?.metadata);
  const rawPlan = toSafeJsonObject(metadata.plan);
  if (!Object.keys(rawPlan).length) return null;
  return normalizeStoryPlan(rawPlan, evidence);
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
    provider: "deterministic",
    model: "draft-writing-deterministic-v1",
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
      provider: "deterministic",
      model: "draft-writing-deterministic-v1",
      skipped: true
    };
  }

  const plan = await loadLatestStoryPlanArtifact(signalId, evidence);
  if (!plan || !plan.sections.length) {
    return {
      evidenceCount: evidence.length,
      sectionCount: 0,
      sourceCount: 0,
      bodyChars: 0,
      saved: 0,
      provider: "deterministic",
      model: "draft-writing-deterministic-v1",
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

  const draft = buildDeterministicDraftFromPlan(signal, plan, evidence);
  const saved = await persistDraftWritingArtifact(signal, draft, {
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
    provider: "deterministic",
    model: "draft-writing-deterministic-v1"
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
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) return null;
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${encodeURIComponent(
    apiKey
  )}`;
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
  try {
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
    if (!response.ok) return null;
    const data = await response.json();
    const text = data?.candidates?.[0]?.content?.parts?.map((p: any) => p?.text || "").join("") || "";
    const parsed = safeJsonParse(text);
    const value = Number(parsed?.context_score);
    if (!Number.isFinite(value)) return null;
    return Math.min(Math.max(value, 0), 10);
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
    .map((row: any): PriorArtMatch => ({
      sourceType: (row.sourceType === "candidate" ? "candidate" : "article") as "article" | "candidate",
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
      (
        COUNT(DISTINCT s.session_hash) FILTER (
          WHERE s.source_type IN ('chat_yes', 'chat_specify')
            AND s.session_hash IS NOT NULL
            AND length(trim(s.session_hash)) > 0
        )
      )::int as "distinctChatSessions24h"
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
    "",
    `Signal: ${JSON.stringify(signal)}`,
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
  if (decision.next_step === "story_planning") {
    await step.sendEvent("emit-story-planning-start-direct", {
      name: "story.planning.start",
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
          persistDecision(signalId, policyShortCircuit)
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
      await step.sendEvent("emit-story-planning-start", {
        name: "story.planning.start",
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

export function createStoryPlanningStartFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "story-planning-start" },
    { event: "story.planning.start" },
    async ({ event, step }: any) => {
      const signalId = Number(event?.data?.signalId || 0);
      if (!signalId) throw new Error("Missing signalId");

      const result = await step.run("story-planning", async () => runStoryPlanning(signalId));
      await step.sendEvent("emit-draft-writing-start", {
        name: "draft.writing.start",
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
        await step.sendEvent("emit-story-planning-from-manual", {
          name: "story.planning.start",
          data: {
            signalId,
            trigger: "admin_manual"
          }
        });
        return {
          ok: true,
          signalId,
          routed: true,
          targetEvent: "story.planning.start"
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
