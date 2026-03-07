# Dayton Enquirer AI Newsroom Blueprint

Last updated: 2026-03-05
Primary branch: `feature/topic-engine`

## 1) Product Vision (North Star)
Build a fully autonomous, multi-persona local newsroom pipeline where each topic engine can:
- ingest signals (event-driven + scheduled),
- qualify newsworthiness with strict guardrails,
- research/extract/plan/write/review via configurable stage runners,
- publish safely,
- and run an article-page chat assistant with strict RAG boundaries.

## 2) Hard Constraints (Must Never Break)
- Chat assistant must **never** perform external web search.
- User suggestions from chat are **signals**, not approvals.
- Deterministic code guardrails override model output.
- Per-persona autonomy switch (`is_auto_promote_enabled`) can force `watch`.
- Dedupe must prevent redundant spend while still allowing legitimate follow-ups.
- User feedback must never feed model logic directly.
- Retention/archival must prevent state bloat.

## 2.5) Layer Execution Order Policy
Do schema migration review first for each new layer.

Good order of execution per layer:
1. Migration(s) for new enums/checks/tables/indexes
2. Pipeline code/events
3. Admin/observability UI
4. Backfill/seed/test run
5. Verify and only then move on

That keeps code and DB contract aligned and avoids hidden runtime failures.

## 3) End-to-End Architecture (Target)
- Layer 0: Orchestration
  - Inngest event bus + cron.
- Layer 0.5: Quota + Pacing Gate (cost/safety control plane)
  - Enforce per-persona daily caps before expensive stages.
  - Queue promoted items instead of immediate publish.
  - Space releases throughout active hours (newsroom cadence).
  - Prevent runaway loops with backlog caps + retry limits.
- Layer 1: Qualification Gate (cheap/frequent)
  - Prior-art lookup, corroboration check, Gemini classification, deterministic guardrails.
- Layer 2: Research Discovery
  - Tavily queries + source harvesting.
- Layer 3: Evidence Extraction
  - Gemini evidence claims from Layer 2 artifacts.
- Layer 4: Story Planning
  - Structured outline + narrative strategy.
- Layer 5: Draft Writing
  - High-quality longform draft.
- Layer 6: Image Sourcing (cost-prioritized)
  - First source from Postgres image pool/history.
  - Fallback to Google Programmable Search image query (bounded call count).
  - Optional source-conditioned generation only as final fallback.
  - Output attribution metadata for selected image when available.
- Layer 7: Final Review
  - Hallucination/policy/editorial/image/editorial check + decision.
- Layer 8: Publish + Recirculation
  - Publish article + related-article indexing.
- Layer 9: Chat + Suggestion Intake
  - Strict in-bounds RAG + out-of-bounds suggestion UX.

### Layer 0: Orchestration (Inngest)
- Purpose:
  - Route all pipeline events through a single event system.
  - Separate trigger, decision, execution, and scheduling concerns.
- Inputs:
  - Event triggers (webhook/RSS/chat suggestions), scheduled discovery triggers, admin/manual triggers.
- Logic:
  - Receive events, run step functions, emit next-stage events.
  - Apply retry policy and dead-letter behavior for failed jobs.
  - Maintain traceable run lineage by `signalId`/`eventKey`.
- Outputs:
  - Deterministic stage-to-stage events and execution logs.

### Layer 0.5: Quota + Pacing Gate
- Purpose:
  - Prevent runaway API spend and uncontrolled publishing volume.
- Inputs:
  - Candidate promotions from Layer 1 (`promote` intent), persona config, daily counters.
- Logic:
  - Enforce per-persona daily cap before costly downstream stages.
  - Enforce global cap and optional budget ceiling.
  - Queue eligible promoted items instead of immediate release.
  - Release queued items in spaced intervals during active window.
  - Enforce min spacing, max backlog, max retries, and kill switch.
- Outputs:
  - `queued`, `released`, `deferred`, or `rejected` execution decisions.
  - Scheduler release events that trigger downstream execution.

### Layer 1: Qualification Gate
- Purpose:
  - Filter noisy signals and route only high-value local stories.
- Inputs:
  - `topic_signals` records (RSS/webhook/chat), prior-art candidates, corroboration evidence.
- Logic:
  - Pre-LLM prior-art lookup and corroboration checks.
  - LLM scorecard classification (newsworthy/local/confidence/category/relation).
  - Deterministic guardrails enforce duplicate, locality, confidence, and autonomy rules.
- Outputs:
  - `action` (`reject|watch|promote`) and `next_step`.
  - Policy flags and reasoning persisted for admin visibility.

### Layer 2: Research Discovery
- Purpose:
  - Collect verifiable reporting material for approved signals.
- Inputs:
  - Released/promoted `signalId`, signal context.
- Logic:
  - Generate focused research queries.
  - Execute Tavily searches with bounded query/result limits.
  - Normalize, dedupe, rank, and persist top artifacts.
- Outputs:
  - Structured `research_artifacts` for downstream evidence extraction.

### Layer 3: Evidence Extraction
- Purpose:
  - Convert raw research into claim-level evidence.
- Inputs:
  - Layer 2 artifacts for `signalId`.
- Logic:
  - Extract claim-evidence pairs from approved sources only.
  - Validate source references against available artifact URLs.
  - Normalize confidence values and keep bounded claim count.
- Outputs:
  - Evidence artifacts: claim, source URL, quote, confidence, significance.

### Layer 4: Story Planning
- Purpose:
  - Produce a structured plan before drafting.
- Inputs:
  - Signal context + extracted evidence.
- Logic:
  - Build article angle, section map, outline, and narrative order.
  - Include uncertainty notes and missing-information callouts.
  - Validate plan format against stage contract.
- Outputs:
  - Planning artifact ready for deterministic draft generation.

### Layer 5: Draft Writing
- Purpose:
  - Produce publication-grade draft text from approved plan/evidence.
- Inputs:
  - Story plan + evidence artifacts + persona style constraints.
- Logic:
  - Generate draft body/headline/dek candidates from approved evidence.
  - Avoid unsupported claims and mark uncertain statements.
  - Persist draft artifact with traceable source linkage.
- Outputs:
  - Draft package for image sourcing and final review.

### Layer 6: Image Sourcing
- Purpose:
  - Attach a relevant image while keeping costs controlled.
- Inputs:
  - Draft context (`signalId`, section/persona keywords, event key).
- Logic:
  - Step 1: attempt Postgres image retrieval/history reuse.
  - Step 2: if none, run bounded Google Programmable Search image query.
  - Step 3: optional source-conditioned generation only as final fallback.
  - Enforce hard per-article query/generation caps and stage timeout.
- Outputs:
  - Selected image payload: URL, credit/author (if present), source URL, alt text, and selection metadata.
  - Explicit `no_image_selected` outcome when caps or relevance checks fail.

### Layer 7: Final Review
- Purpose:
  - Safety and quality gate before publish.
- Inputs:
  - Draft + evidence + image payload + policy constraints.
- Logic:
  - Verify factual grounding, style, policy compliance, and editorial coherence.
  - Verify image suitability and metadata completeness for display.
  - Produce approve/revise/reject decision with rationale.
- Outputs:
  - Publish-ready article package or revision request with reasons.

### Layer 8: Publish + Recirculation
- Purpose:
  - Publish final article and improve reader navigation.
- Inputs:
  - Approved final package.
- Logic:
  - Persist published article and link to run lineage.
  - Update recirculation/search indexes for related article retrieval.
  - Record audit summary (cost/tokens/stage outcomes).
- Outputs:
  - Public article + related-article discoverability.

### Layer 9: Chat + Suggestion Intake
- Purpose:
  - Provide safe article Q&A and capture potential leads without bypassing editorial gatekeeping.
- Inputs:
  - User chat message, current article, allowed local DB retrieval context.
- Logic:
  - Tone/intent classify, then apply persona response rules.
  - Strict RAG boundary: article + approved DB retrieval only; no external web search.
  - Out-of-bounds flows return graceful response + `yes/no/specify` suggestion options.
  - Suggestions enter signal ingestion path and follow normal qualification rules.
- Outputs:
  - User-facing response + optional recirculation links + optional suggestion signal.

## 4) Model/Provider Plan
- Topic Qualification: Gemini 1.5 Flash / Gemini 2.x Flash (fast + low cost)
- Research Discovery: Tavily API
- Evidence Extraction: Gemini 1.5 Pro (fallback allowed)
- Story Planning: GPT-4o mini
- Draft Writing: Claude 3.5 Sonnet
- Image Sourcing: Postgres -> Google Programmable Search (fallback) -> optional source-conditioned generation
- Final Review: GPT-4o
- Chat runtime: Gemini 2.0 Flash

## 5) System Build Scope
- Quota/pacing control plane with daily caps, scheduler release, spacing, and backlog controls.
- Full multi-stage article pipeline from qualification through publish.
- Persona-level curation controls for activation mode, stage runners, and autonomy.
- Image sourcing stage with DB-first selection and bounded external fallback.
- Strict-RAG article chat with suggestion intake that re-enters the same gatekeeper pipeline.
- Operational observability via admin queueing, policy flags, and audit reporting.

## 6) Core Data Model (Canonical)
### `topic_signals`
Purpose: holding pen for all incoming pings before expensive pipeline work.
Key fields:
- identity: `id`, `persona_id`, `source_type`, `source_name`, `source_url`, `external_id`
- payload: `title`, `snippet`, `section_hint`, `metadata`, `session_hash`
- dedupe: `dedupe_key`, `event_key`, `relation_to_archive`
- decision: `is_newsworthy`, `is_local`, `confidence`, `category`, `action`, `next_step`, `policy_flags`, `reasoning`
- review: `review_decision`, `review_notes`, `processed_at`

### `topic_engines`
Purpose: per-persona operational controls.
Key field:
- `is_auto_promote_enabled` (safety brake)
- plus quota/pacing controls (daily cap, active window, min spacing, backlog cap, autonomy mode).

### `research_artifacts`
Purpose: heavy artifacts from downstream stages.
- stores Tavily results + evidence outputs + image sourcing outputs + later stage artifacts.
- should evolve toward stable run/candidate IDs to reduce duplicate storage on retries.

## 7) Canonical Event Contracts
### `signal.received`
```json
{ "signalId": 123 }
```

### `research.start`
```json
{ "signalId": 123, "trigger": "gatekeeper|admin_manual|scheduled" }
```

### `signal.promoted.queue`
```json
{ "signalId": 123, "personaId": "local-reporter", "eventKey": "...", "trigger": "gatekeeper|admin_manual" }
```

### `scheduler.release`
```json
{ "personaId": "local-reporter", "dateKey": "2026-03-05", "releaseSlot": "2026-03-05T14:00:00Z" }
```

### `evidence.extraction.start`
```json
{ "signalId": 123 }
```

### `story.planning.start`
```json
{ "signalId": 123, "trigger": "research_start|admin_manual|replay" }
```

### `image.sourcing.start`
```json
{ "signalId": 123, "draftId": "...", "personaId": "local-reporter" }
```

### `signal.gatekeeper.route.manual`
```json
{
  "signalId": 123,
  "personaId": "local-reporter",
  "action": "promote|watch|reject",
  "nextStep": "none|research_discovery|cluster_update|story_planning",
  "relationToArchive": "none|duplicate|update|follow_up",
  "eventKey": "...",
  "trigger": "admin_manual"
}
```

## 8) Deterministic Guardrails (Source of Truth)
Guardrails run after model output:
1. If `relation_to_archive == duplicate` => force `action=reject`, `next_step=none`.
2. If low confidence/evidence => demote promote->watch, add `low_evidence`.
3. If not local => `reject`, add `not_local`.
4. Chat-origin signals require corroboration:
   - either non-chat corroboration,
   - or >=2 distinct chat sessions.
5. If `is_auto_promote_enabled=false` and action would promote => force `watch` + `auto_promote_disabled`.
6. `watch/reject` always implies `next_step=none`.
7. Promotion must pass quota gate; if cap/backlog exceeded => queue defer or reject by policy.
8. Scheduler releases must enforce spacing window; no burst publishing.
9. Image stage enforces cost caps (`max queries`, `max generation attempts`) and can publish without image on exhaustion.

## 9) Chat Assistant Requirements (Strict)
- Boundaries:
  - in-bounds: article + approved local DB retrieval only.
  - out-of-bounds: never error bluntly; graceful response + 3 options (`yes/no/specify`) for suggestion flow.
- No external search for chat answers.
- Tone/intent classifier first; response-rule selection second.
- Response rules are persona-specific and admin-editable.
- Support fallback rule with `is_fallback=true`.
- Chat suggestions enter signal queue; they do not bypass worthiness checks.

## 10) Admin UI Scope (Target)
- Topic engine curation:
  - activation mode: `event|scheduled|both`
  - stage runner/provider/model config per persona
  - RSS feed curation per persona
  - autonomy toggle (`is_auto_promote_enabled`)
  - daily article cap per persona
  - active publishing window + timezone
  - minimum spacing minutes
  - queue backlog cap
  - kill switch / autonomy mode controls
- Signals queue:
  - filter by persona/action/review_decision
  - inspect `reasoning`, `policy_flags`, `is_newsworthy`
  - manual promote/reject/watch actions
  - 24h analytics summary
- Chat response rules editor:
  - fields: tone, intent matcher, script, show_poll, is_fallback

## 11) Retention Policy (Current Direction)
- Keep hot operational data lean.
- Archive heavy artifact detail after ~30 days.
- Keep lightweight audit summaries long-term.
- User feedback retained for admin analytics only (not training loop), with purge windows.

## 12) Environment Variables (Baseline)
- Core:
  - `DATABASE_URL`
  - `ADMIN_API_KEY`
- Inngest:
  - `INNGEST_EVENT_URL`
  - `INNGEST_EVENT_KEY`
- Gemini:
  - `GEMINI_API_KEY`
  - optional model overrides (`TOPIC_ENGINE_*`, `GEMINI_MODEL`, etc.)
- Tavily:
  - `TAVILY_API_KEY`
- Image sourcing:
  - `GOOGLE_CSE_API_KEY`
  - `GOOGLE_CSE_CX`

## 13) QA Gates Before Production
1. Signal ingestion auto-triggers gatekeeper path (not only manual).
2. Gatekeeper classification is real (no stub), with prior-art + corroboration working.
3. Safety brake verified on/off for at least 2 personas.
4. Quota/pacing gate verified: daily caps, spacing, backlog behavior, and kill switch.
5. Research and evidence artifacts persist and are queryable by signal.
6. Image sourcing obeys fallback order (DB first, external query second, optional generation last) and cost caps.
7. Image stage can safely publish without image when caps or sources fail.
8. Mock/test-only shortcuts are disabled outside dev.
9. Cluster update route implemented or explicitly disabled with policy.
10. Chat stays in strict RAG boundary and never external-searches.
11. Suggestion flow always offers `yes/no/specify` on out-of-bounds.
12. Retention jobs run safely and are observable.

## 14) Known Risks / Watch Items
- Context drift across long chats.
- Partial implementations mistaken for complete layers.
- Silent spend growth from weak dedupe across retries.
- Shipping with test-mode paths still active.

## 15) Recommended Execution Rhythm
- Review every 2 phases.
- At each review: architecture pass + guardrail pass + regression pass.
- Final release gate: strict pass/fail checklist against this blueprint.

## 16) Copy/Paste Handoff Prompt for New Chats
Use this at the top of any new chat:

```text
You are continuing the Dayton Enquirer AI newsroom implementation.
Use /docs/AI_NEWSROOM_BLUEPRINT.md as the canonical architecture/spec.
Current branch: feature/topic-engine.
First, read the blueprint and report:
1) what is complete,
2) what is partially implemented,
3) what remains for the current phase,
4) exact files you will change.
Do not introduce architecture outside the blueprint unless explicitly approved.
Preserve deterministic guardrails and strict chat RAG boundary.
```

## 17) Fast Reality Check Prompt
If you suspect drift, ask:

```text
Audit this repo against /docs/AI_NEWSROOM_BLUEPRINT.md and produce a pass/fail table for each section, with file references and blockers.
```
