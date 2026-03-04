// Skeleton only: wire this into your Inngest client bootstrap when ready.
// This file defines the 7-step contract for Layer 1 Gatekeeper.

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

// STEP 1: load_signal
async function loadSignalById(signalId: number): Promise<SignalRecord> {
  // TODO: query topic_signals table by id
  throw new Error(`Not implemented: loadSignalById(${signalId})`);
}

// STEP 2: lookup_prior_art
async function lookupPriorArt(_signal: SignalRecord): Promise<PriorArtMatch[]> {
  // TODO: run prior-art SQL against articles/topic_engine_candidates (top 3)
  return [];
}

// STEP 3: check_corroboration_pre_ai
async function checkCorroborationPreAI(_signal: SignalRecord): Promise<CorroborationSummary> {
  // TODO: similarity-based corroboration in last 24h; no event_key dependency
  return {
    similarSignals24h: 0,
    distinctSourceTypes24h: [],
    distinctChatSessions24h: 0
  };
}

// STEP 4: gatekeeper_classify (Gemini 2.0 Flash)
async function classifyWithGatekeeper(
  _signal: SignalRecord,
  _priorArt: PriorArtMatch[],
  _corroboration: CorroborationSummary
): Promise<GatekeeperOutput> {
  // TODO: call Gemini 2.0 Flash with strict JSON contract
  return {
    is_newsworthy: 0,
    is_local: false,
    confidence: 0,
    category: "Other",
    relation_to_archive: "none",
    event_key: "",
    action: "reject",
    next_step: "none",
    policy_flags: [],
    reasoning: "stub"
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
    if (out.action !== "promote") out.next_step = "none";
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
async function persistDecision(_signalId: number, decision: GatekeeperOutput): Promise<PersistedDecision> {
  // TODO: update topic_signals with decision + processed_at timestamp
  return {
    ...decision,
    processed_at: new Date().toISOString()
  };
}

// STEP 7: route_next_step
async function routeNextStep(_signalId: number, decision: PersistedDecision): Promise<void> {
  // TODO: emit research.start or cluster.update events
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
      await step.run("7-route_next_step", async () => routeNextStep(signalId, persisted));

      return {
        ok: true,
        signalId,
        action: persisted.action,
        nextStep: persisted.next_step
      };
    }
  );
}
