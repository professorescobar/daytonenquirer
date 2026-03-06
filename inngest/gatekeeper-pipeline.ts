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

const TEST_SIGNAL_ID = 12345;
const TEST_MODE_ENABLED =
  String(process.env.TOPIC_ENGINE_TEST_MODE || "").trim().toLowerCase() === "true" ||
  String(process.env.VERCEL_ENV || "").trim().toLowerCase() !== "production";
const HARD_CODED_GATEKEEPER_MODEL = "gemini-1.5-flash";
const HARD_CODED_RESEARCH_QUERY_MODEL = "gemini-1.5-flash";
const HARD_CODED_EVIDENCE_MODEL_CANDIDATES = ["gemini-1.5-pro", "gemini-1.5-pro-002"];

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
  timezone: string
): Promise<number[]> {
  const rows = await sql`
    SELECT
      to_char((q.scheduled_for_utc AT TIME ZONE ${timezone})::time, 'HH24:MI:SS') as "scheduledTime"
    FROM topic_engine_release_queue q
    WHERE q.persona_id = ${personaId}
      AND q.status IN ('queued', 'released')
      AND q.scheduled_day_local = ${dayLocal}::date
      AND q.scheduled_for_utc IS NOT NULL
  `;
  return rows
    .map((row: any) => parseTimeToMinutes(cleanText(row.scheduledTime, 20), -1))
    .filter((value: number) => value >= 0);
}

function pickScheduledMinute(
  config: PacingConfig,
  nowLocalTime: string,
  isToday: boolean,
  existingSlots: number[]
): number {
  const nowMinutes = parseTimeToMinutes(nowLocalTime, 0);
  const startMinutes = parseTimeToMinutes(config.windowStartLocal, 6 * 60);
  const endMinutes = parseTimeToMinutes(config.windowEndLocal, 22 * 60);
  const window = normalizeWindow(startMinutes, endMinutes);
  const posts = Math.max(0, config.postsPerActiveDay);

  let candidateSlots: number[] = [];
  if (posts <= 0) {
    candidateSlots = [];
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

  const released: Array<{ signalId: number; personaId: string }> = [];
  for (const row of dueRows) {
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

  const model = HARD_CODED_RESEARCH_QUERY_MODEL;
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

// STEP 4: gatekeeper_classify (Gemini 1.5 Flash)
async function classifyWithGatekeeper(
  signal: SignalRecord,
  priorArt: PriorArtMatch[],
  corroboration: CorroborationSummary
): Promise<GatekeeperOutput> {
  const apiKey = cleanText(process.env.GEMINI_API_KEY || "", 500);
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

  const model = HARD_CODED_GATEKEEPER_MODEL;
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
