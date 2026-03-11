# Dayton Enquirer Dictionary Substrate Blueprint

Last updated: 2026-03-11
Status: Approved canonical design doc for substrate architecture
Primary branch: `feature/topic-engine`

## 1) Product Vision (North Star)
Build a separate intermittent substrate pipeline that creates and maintains a high-trust civic dictionary for the Dayton Enquirer autonomous newsroom.

This substrate exists to map local reality before the streaming news pipeline makes decisions. It should spend more compute, run less frequently, and produce a read-optimized canonical dictionary that downstream gatekeeper logic can trust for locality, entity normalization, alias resolution, event clustering, and civic context.

The substrate pipeline is a write system.

The newsroom pipeline is a read system.

That separation is mandatory and is the primary anti-poisoning control.

## 2) Hard Constraints (Must Never Break)
- Raw inbound news signals must never write directly into the canonical dictionary.
- Chat-origin signals, article drafts, article chat, and user feedback must never promote dictionary mutations.
- Only approved root sources may seed substrate extraction runs, with permissions constrained by explicit trust-tier policy.
- Every promoted dictionary fact must carry source provenance.
- Canonical records must be published from validated promotion steps only, never directly from LLM output.
- Published snapshots are the only source of truth the newsroom runtime may read.
- Temporal facts must expire or be revalidated; civic officeholders cannot be treated as timeless truth.
- Spatial coverage checks must prefer deterministic jurisdiction or geometry checks over string heuristics when available.
- The news pipeline must read only from published dictionary state, not staging or unreviewed candidates.
- Substrate failures must surface into an explicit review queue; drift must not be silent.

## 2.5) Execution Methodology
Substrate work should execute phase-by-phase, not as a loose layer-first buildout.

The goal is to keep the architecture stable, prevent context drift across long implementation windows, and ensure anti-poisoning contracts are enforced before downstream phases depend on them.

### Delivery Process Per Phase
1. Phase strategy
2. Phase scope and chunk plan
3. Approval gate
4. Chunked implementation
5. Chunk review gates
6. Phase integration review
7. Neon migration/application step
8. Verification
9. Independent review chat
10. Exit criteria check
11. Only then move to the next phase

### Required Rules
- Open a fresh implementation chat for each major substrate phase or similarly scoped delivery unit when helpful for context control.
- Do strategy before code.
- Do not implement until the phase outline is reviewed and approved.
- Once the phase outline is approved, begin coding chunk-by-chunk directly; do not require a separate pre-implementation strategy draft for every chunk unless scope changes or a new risk appears.
- Review each implemented chunk in the actual files before starting the next chunk.
- Review the full phase after all chunks are complete before declaring the phase done.
- Run required migrations in Neon before shipping the phase.
- Verify each phase as safely as possible before moving forward.
- Use an independent review pass before marking a phase complete.

### First-Pass Implementation Policy
Do not use a vague "wire everything first and harden later" approach.

For each phase, the first implementation pass should be the thinnest viable version that is still contract-correct.

Foundation contracts must be correct on first pass, especially:
- promotion boundaries
- provenance
- temporal validity
- review-state semantics
- snapshot visibility
- anti-poisoning rules

Areas that can start thinner and deepen later include:
- crawl sophistication
- extraction sophistication
- freshness automation
- admin polish

### Phase Exit Criteria Standard
Every phase should define its own exit criteria before implementation starts.

At minimum, phase completion should mean:
- schema contract for the phase is correct and applied
- runtime wiring for the phase is connected end to end
- anti-poisoning rules for the phase are enforced
- required observability or audit state exists
- targeted tests and checks for the phase pass
- no known blocker remains that would corrupt downstream phases

This methodology keeps schema, runtime, and promotion rules aligned and avoids poisoning the read path with half-built substrate data.

## 3) End-to-End Architecture (Target)
- Layer S0: Orchestration
  - Inngest cron/manual events for intermittent substrate runs.
- Layer S1: Root Source Registry
  - Approved domains, crawl entry URLs, entity classes, trust tier, recrawl cadence, freshness SLA.
- Layer S2: Ingestion / Crawl Capture
  - Firecrawl or equivalent crawl fetches raw pages and stores immutable artifacts.
- Layer S3: Structured Extraction
  - Heavy LLM extracts entities, aliases, offices, relationships, jurisdictions, and provenance-bound facts into staging.
- Layer S4: Resolution / Merge Proposals
  - Deterministic and LLM-assisted matching decides create vs merge vs alias vs supersede.
- Layer S5: Validation / Policy Gate
  - Strict schema, provenance, temporal, spatial, and anti-generic checks.
- Layer S6: Promotion / Snapshot Publish
  - Validated proposals update canonical dictionary and emit a versioned published snapshot.
- Layer S7: Freshness / Drift Monitoring
  - Detect expired facts, overdue roots, repeated failures, and missing replacements.
- Layer S8: Review Inbox / Dead Letter Queue
  - Human-visible queue for fetch, extraction, merge, validation, or freshness failures.
- Layer S9: Newsroom Read Integration
  - Layer 1 and other newsroom stages consume published dictionary snapshots only.

### Layer S0: Orchestration
- Purpose:
  - Run expensive substrate jobs on a deliberate cadence.
  - Separate trigger, crawl, extraction, merge, validation, promotion, and review concerns.
- Inputs:
  - Cron triggers, manual admin triggers, targeted refresh events, stale-record refresh events.
- Logic:
  - Maintain run lineage by `substrateRunId`, `rootSourceId`, `crawlArtifactId`, and `snapshotId`.
  - Support full runs and targeted diff runs.
- Outputs:
  - Deterministic substrate stage events and execution logs.

### Layer S1: Root Source Registry
- Purpose:
  - Define what sources are allowed to shape the dictionary.
- Inputs:
  - Curated root URLs and domain-level trust metadata.
- Logic:
  - Store source type, trust tier, supported entity classes, crawl cadence, freshness SLA, and failure thresholds.
  - Mark whether source is authoritative, corroborative, or contextual.
- Outputs:
  - Approved crawl targets and source policy metadata for downstream stages.

### Source of Truth Semantics
- Canonical tables are mutable build-state for the substrate pipeline only.
- Published snapshots are immutable release artifacts.
- The newsroom runtime must resolve dictionary reads against a published snapshot, never against mutable canonical head tables directly.
- Promotion writes may update canonical head state first, but a change is not newsroom-visible until included in a published snapshot.
- Rollback means changing the active published snapshot pointer, not rewriting or deleting historical promoted records.

### Layer S2: Ingestion / Crawl Capture
- Purpose:
  - Capture immutable source artifacts before extraction.
- Inputs:
  - Approved root source entries.
- Logic:
  - Fetch content, store raw text/HTML/metadata, hash artifacts, detect diffs, and preserve crawl timestamps.
  - Never mutate canonical data at this stage.
- Outputs:
  - Immutable crawl artifacts in staging.

### Layer S3: Structured Extraction
- Purpose:
  - Convert crawl artifacts into structured candidate facts.
- Inputs:
  - Crawl artifacts, root source policy, entity-class extraction contracts.
- Logic:
  - Use heavy LLM extraction with strict JSON contracts.
  - Extract only canonical, fully-qualified, provenance-backed entities and facts.
  - Reject generic references such as "the mayor" or "police" unless tied to a canonical record proposal.
- Outputs:
  - Staged entity candidates, alias candidates, role/office assertions, relationship assertions, jurisdiction candidates, and extraction diagnostics.

### Layer S4: Resolution / Merge Proposals
- Purpose:
  - Decide whether staged candidates create new canonical records or attach to existing ones.
- Inputs:
  - Staged candidates plus the current canonical head, which is seeded from and reconciled against the active published snapshot.
- Logic:
  - Deterministic match first.
  - Escalate ambiguous cases to LLM adjudication.
  - Resolve proposals against canonical head while preserving snapshot lineage so unpublished mutations never become newsroom-visible accidentally.
  - Produce explicit proposal types such as `create_entity`, `add_alias`, `create_assertion`, `supersede_assertion`, `retire_alias`, `merge_duplicate`.
- Outputs:
  - Merge proposals with confidence, rationale, and affected canonical targets.

### Layer S5: Validation / Policy Gate
- Purpose:
  - Prevent dictionary poisoning before promotion.
- Inputs:
  - Merge proposals and staged facts.
- Logic:
  - Enforce schema validity, source trust rules, provenance minimums, temporal validity requirements, spatial requirements, anti-generic checks, and merge-confidence thresholds.
  - Require second-pass review rules for high-risk merges or officeholder changes when needed.
- Outputs:
  - `approved`, `rejected`, `needs_review`, or `retryable_failure` proposal outcomes.

### Layer S6: Promotion / Snapshot Publish
- Purpose:
  - Publish a new stable dictionary state for newsroom consumers.
- Inputs:
  - Approved proposals only.
- Logic:
  - Apply changes to canonical tables.
  - Emit immutable published snapshot metadata with version number and run lineage.
  - Support rollback to previous snapshots if promotion logic is found faulty.
- Outputs:
  - Canonical dictionary updates and a new published snapshot.

### Layer S7: Freshness / Drift Monitoring
- Purpose:
  - Keep time-sensitive civic facts from going stale.
- Inputs:
  - Canonical assertions, root source freshness policies, recent run outcomes.
- Logic:
  - Detect expiring officeholder assertions, overdue recrawls, stale aliases, missing revalidation, and repeated source failures.
  - Trigger targeted refresh runs before critical facts age out silently.
- Outputs:
  - Refresh candidates, drift alerts, and review queue entries.

### Layer S8: Review Inbox / Dead Letter Queue
- Purpose:
  - Surface substrate failures or unresolved ambiguity for human inspection.
- Inputs:
  - Any failed or blocked step from ingestion through promotion.
- Logic:
  - Track fetch failures, extraction contract failures, merge ambiguity, validation failures, stale high-impact facts, and repeated retry exhaustion.
  - Keep retry counts, severity, source context, and last error payload.
- Outputs:
  - Actionable review items instead of silent drift.

### Layer S9: Newsroom Read Integration
- Purpose:
  - Improve gatekeeper and downstream newsroom quality using published substrate state.
- Inputs:
  - Latest published snapshot and canonical dictionary indexes.
- Logic:
  - Provide entity normalization, alias expansion, jurisdiction checks, canonical event-key hints, officeholder lookup, and local source trust signals.
  - Expose read-only access patterns to Layer 1 and later layers.
- Outputs:
  - Deterministic read models that strengthen the newsroom pipeline without allowing write-back contamination.

## 4) Trust and Anti-Poisoning Model
- Root-source trust is explicit:
  - `authoritative`: official government, court, district, agency, or institution pages.
  - `corroborative`: established local news or partner institution sources.
  - `contextual`: useful for context, not sufficient alone for critical canonical mutations.
- Canonical promotion is provenance-first:
  - every promoted fact must trace to one or more crawl artifacts and source URLs.
- The substrate has staged states:
  - raw artifact -> extracted candidate -> merge proposal -> validated outcome -> promoted canonical snapshot.
- The newsroom pipeline is read-only:
  - inbound signals may reference or suggest dictionary candidates, but may not mutate canonical state.
- Generic entities are not canonical:
  - only fully qualified entities, aliases, or time-bounded assertions may be promoted.
- High-risk assertions require stronger evidence:
  - officeholder changes, jurisdiction changes, and merge collisions may require multiple authoritative artifacts or secondary adjudication.

### Trust-Tier Permissions
- `authoritative` sources:
  - may seed crawls
  - may create or supersede canonical assertions when validation passes
  - may satisfy primary provenance requirements for high-impact civic facts
- `corroborative` sources:
  - may seed crawls when explicitly approved in the root-source registry
  - may support validation, conflict detection, and provenance enrichment
  - should not be the sole basis for high-impact officeholder or jurisdiction mutations if an authoritative source class exists
- `contextual` sources:
  - may seed crawls only when explicitly marked for narrow entity classes or background enrichment
  - may enrich descriptions, aliases, and discovery context
  - may not be the sole basis for critical canonical mutations

If a source tier needs broader permissions, that should be encoded in root-source policy explicitly rather than inferred from domain reputation.

## 5) Temporal Model (Required)
Local civic data is time-bound. The substrate must model assertions with valid time, not just entity existence.

### Temporal Principles
- Entities may be durable, but many facts about them are not.
- Officeholding, board membership, district boundaries, and official titles may change on known schedules.
- Canonical truth for the newsroom means "true as of a known time window", not "true forever".

### Required Temporal Fields
For role/office assertions, memberships, and other time-sensitive facts:
- `effective_start_at`
- `effective_end_at`
- `observed_at`
- `last_verified_at`
- `validity_status` (`current|scheduled|expired|superseded|unknown`)
- `freshness_sla_days`

Optional but strongly preferred where available:
- `term_end_at`
- `next_election_at`
- `next_review_at`
- `superseded_by_assertion_id`

### Temporal Rules
- A current officeholder assertion cannot remain `current` indefinitely without revalidation.
- Assertions that cross freshness SLA without verification should downgrade to `needs_review` or trigger refresh.
- Promotion logic should supersede older assertions rather than overwrite them destructively.
- Layer 1 lookups for "current mayor", "current superintendent", etc. must resolve against active valid-time assertions only.

### Temporal Review Contract
Temporal validity and review state are related but distinct.

- `validity_status` answers whether a fact is considered true in time:
  - `current|scheduled|expired|superseded|unknown`
- `review_status` answers whether the system operationally trusts the current representation:
  - `verified|pending_refresh|needs_review|blocked`

Examples:
- an assertion may be `current` but `pending_refresh` if freshness SLA is nearly exceeded
- an assertion may be `expired` and `verified` if the system confidently knows the term ended
- an assertion may be `unknown` and `needs_review` if the source changed structure and no successor could be resolved

Layer 1 should primarily respect `validity_status`, while substrate operations should use `review_status` to drive refresh and human intervention.

## 6) Spatial Model (Lightweight Geospatial Required)
The substrate must support deterministic locality checks without requiring a full GIS platform on day one.

### Spatial Principles
- Coverage should be checked through structured geography first, text heuristics second.
- Jurisdictions and neighborhoods should be represented as canonical coverage objects.
- Spatial certainty should be explicit so downstream layers know when a locality decision is strong versus approximate.

### Required Spatial Support
For jurisdictions and coverage objects:
- canonical name
- jurisdiction type
- centroid coordinates
- optional GeoJSON polygon or bounding box
- parent jurisdiction
- coverage status

For place-like entities when available:
- normalized address
- optional coordinates
- jurisdiction link
- spatial confidence score

### Spatial Read Path for Layer 1
Preferred order:
1. direct jurisdiction membership
2. point-in-bounds or bbox containment
3. normalized address to jurisdiction mapping
4. alias/name heuristics

This allows the gatekeeper locality decision to become mathematically stronger over time without making spatial completeness a blocker for every entity class.

## 7) Core Data Model (Canonical + Staging)
The substrate should not be a single flat dictionary table. It should separate durable entities from time-bound assertions and from staging/promotion state.

### Canonical Tables (Logical)
#### `dictionary_entities`
Purpose: canonical people, organizations, places, facilities, boards, districts, agencies, venues, and other durable entities.
Key fields:
- identity: `id`, `entity_type`, `canonical_name`, `slug`
- locality: `primary_jurisdiction_id`, `normalized_address`, `lat`, `lng`, `spatial_confidence`
- lifecycle: `status`, `created_at`, `updated_at`, `last_verified_at`
- metadata: `description`, `notes`, `attributes`

#### `dictionary_aliases`
Purpose: alternate names, abbreviations, prior names, spelling variants, shorthand labels.
Key fields:
- identity: `id`, `entity_id`, `alias`, `alias_type`
- lifecycle: `status`, `effective_start_at`, `effective_end_at`
- provenance: `source_count`, `last_verified_at`

#### `dictionary_roles`
Purpose: canonical offices, seats, titles, and role definitions.
Key fields:
- identity: `id`, `role_name`, `role_type`, `jurisdiction_id`
- lifecycle: `status`, `term_pattern`, `last_verified_at`

#### `dictionary_assertions`
Purpose: time-bounded facts such as officeholding, membership, organizational relationships, jurisdiction membership, or facility operation.
Key fields:
- identity: `id`, `assertion_type`, `subject_entity_id`, `object_entity_id`, `role_id`
- temporal: `effective_start_at`, `effective_end_at`, `term_end_at`, `observed_at`, `last_verified_at`, `validity_status`
- review: `review_status`
- confidence: `assertion_confidence`
- lineage: `supersedes_assertion_id`, `snapshot_id`

#### `dictionary_jurisdictions`
Purpose: cities, counties, neighborhoods, districts, coverage zones, and spatial parents.
Key fields:
- identity: `id`, `name`, `jurisdiction_type`, `parent_jurisdiction_id`
- geometry: `centroid_lat`, `centroid_lng`, `bbox`, `geojson`
- lifecycle: `status`, `last_verified_at`

#### `dictionary_provenance`
Purpose: source linkage for every promoted fact and canonical record.
Key fields:
- identity: `id`, `record_type`, `record_id`
- source: `root_source_id`, `crawl_artifact_id`, `source_url`, `source_domain`, `trust_tier`
- temporal: `observed_at`, `captured_at`
- extraction: `substrate_run_id`, `extraction_version`

#### `dictionary_snapshots`
Purpose: immutable published versions consumed by newsroom runtime.
Key fields:
- identity: `id`, `version`, `status`
- lineage: `substrate_run_id`, `created_at`, `published_at`
- metrics: `entity_count`, `assertion_count`, `alias_count`, `change_summary`

### Staging / Runtime Tables (Logical)
#### `dictionary_root_sources`
Purpose: approved substrate entry points and crawl policy.

#### `dictionary_crawl_artifacts`
Purpose: immutable raw fetch artifacts and content hashes.

#### `dictionary_extraction_candidates`
Purpose: staged candidate entities, aliases, jurisdictions, and assertions from LLM extraction.

#### `dictionary_merge_proposals`
Purpose: explicit create/merge/supersede proposals before promotion.

#### `dictionary_validation_results`
Purpose: approval, rejection, retryable failure, or human review outcomes.

#### `dictionary_review_queue`
Purpose: dead letter queue / human inbox for substrate operational exceptions.

#### `dictionary_pipeline_runs`
Purpose: stage-level run audit for cost, latency, status, and lineage.

## 8) Ingestion and Source Strategy
The substrate starts from approved root sources, not from open-ended web discovery.

### Root Source Policy
- Prefer official local government, county, school district, court, agency, and institution domains.
- Support secondary corroborative sources where useful for validation or coverage gaps.
- Store domain-level and URL-level trust metadata separately.

### Ingestion Policy
- Full initial build may crawl broader root sets.
- Maintenance runs should prefer diff detection and targeted refresh.
- Crawl artifacts should be immutable and hash-addressable.
- Repeated layout failures or access failures should create review queue items.

### Early Entity Classes to Prioritize
- jurisdictions and neighborhoods
- local government bodies and agencies
- elected officials and key officeholders
- school districts and major public institutions
- recurring civic venues, boards, and commissions
- canonical aliases and shorthand references used in local reporting

## 9) Extraction Contracts
Heavy LLM extraction is allowed here because this pipeline is intermittent and write-gated.

### Extraction Requirements
- Return strict JSON only.
- Emit fully qualified canonical names only.
- Preserve evidence snippets and source references for every proposed fact.
- Distinguish durable entities from time-bounded assertions.
- Explicitly reject vague entities that cannot stand alone canonically.

### Extraction Outputs
- entity candidates
- alias candidates
- role candidates
- assertion candidates
- jurisdiction candidates
- extraction diagnostics and rejection reasons

### Anti-Generic Rule
Examples like "the mayor", "police", "city hall", or "downtown" should not become new canonical records unless the extraction step can anchor them to an existing or proposed canonical entity with proper provenance.

## 10) Resolution / Merge Strategy
Resolution is where most poisoning risk lives. It needs deterministic rules first and LLM judgment second.

### Preferred Order
1. exact canonical-name match
2. alias match
3. jurisdiction-scoped role/entity match
4. deterministic fuzzy candidate shortlist
5. LLM adjudication for ambiguous proposals

### Proposal Types
- `create_entity`
- `add_alias`
- `update_entity_attributes`
- `create_role`
- `create_assertion`
- `supersede_assertion`
- `retire_alias`
- `merge_duplicate`
- `reject_candidate`
- `needs_review`

### High-Risk Merge Cases
- person-to-person merges with similar names
- officeholder changes
- org renames versus distinct orgs
- neighborhood/place ambiguity
- jurisdiction remapping

These should require stronger provenance or review escalation.

## 11) Validation and Promotion Rules
Promotion should be explicit, versioned, and reversible.

### Validation Checks
- schema contract validity
- source trust eligibility
- provenance completeness
- temporal field completeness for time-sensitive assertions
- spatial minimums for jurisdiction-like entities
- anti-generic naming checks
- duplicate and merge-collision checks
- freshness and recency plausibility

### Promotion Outcomes
- `approved`: promote into canonical tables and next snapshot
- `rejected`: preserve history but do not publish
- `needs_review`: send to review queue
- `retryable_failure`: retry on next run or targeted refresh

### Snapshot Rules
- newsroom runtime reads only from published snapshots
- snapshots are immutable after publish
- rollback should switch readers to a prior snapshot, not rewrite history

## 12) Freshness, Maintenance, and Drift Control
After initial build, the substrate should become mostly diff-driven.

### Maintenance Mode
- run weekly or biweekly by default
- prioritize roots by freshness SLA and criticality
- refresh only changed artifacts when possible
- revalidate time-sensitive assertions on tighter cadence than durable entity metadata

### Drift Triggers
- freshness SLA exceeded
- term end approaching or passed
- authoritative source changed layout/content hash significantly
- repeated extraction failure
- assertion expired without confirmed successor

### Drift Response
- targeted refresh event
- review queue item
- downgrade assertion validity state if unresolved

## 13) Review Inbox / Dead Letter Queue
The substrate must fail loudly and operationally.

### Queue Item Types
- `fetch_failure`
- `artifact_parse_failure`
- `extraction_contract_failure`
- `merge_ambiguity`
- `validation_failure`
- `promotion_blocked`
- `freshness_overdue`
- `expired_high_impact_assertion`

### Required Queue Metadata
- `severity`
- `root_source_id`
- `crawl_artifact_id`
- `affected_record_type`
- `affected_record_id`
- `retry_count`
- `last_error`
- `first_failed_at`
- `last_failed_at`
- `suggested_action`

### Operational Goal
If a high-trust civic source changes structure or stops yielding usable extraction output, the pipeline should create a review item instead of silently allowing the dictionary to decay.

## 14) Newsroom Integration Points
This substrate is valuable only if it materially strengthens the existing newsroom pipeline.

### Layer 1 Gatekeeper Uses
- alias-aware entity normalization for signal titles/snippets
- stronger locality gate using jurisdiction and geometry data
- officeholder and institution recognition from canonical dictionary
- canonical event-key hints and relation-to-archive grouping
- better prior-art clustering by normalized entity sets instead of pure string overlap

### Event-Family Scope
- The substrate should provide stable entity memory, alias memory, officeholder memory, and jurisdiction memory.
- The substrate may also provide event-key hints derived from canonical entities or recurring civic structures, but it is not the primary home for fast-changing event-family state.
- Rolling event-family memory such as recent signal clusters, active story threads, and short-horizon prior-art grouping should remain in the newsroom runtime layer.
- In practice:
  - substrate owns durable reference identity
  - Layer 1 owns rolling event-state and live clustering
  - Layer 1 may use substrate entities to normalize event families, but should not try to store transient event history inside the substrate dictionary

### Downstream Uses
- research query expansion from canonical aliases
- source-domain trust hints for research discovery
- article metadata enrichment
- safer recirculation and topic clustering

### Non-Use Rule
The newsroom runtime may read published dictionary state and emit possible future substrate candidates into a separate suggestion queue, but it must never mutate canonical substrate data inline.

## 15) Model / Provider Strategy
- Ingestion / crawl capture: Firecrawl or equivalent
- Structured extraction: heavy frontier model with strict JSON output
- Merge adjudication: heavy model for ambiguous cases only
- Validation: deterministic code first, optional second-model review for narrow high-risk classes
- Runtime gatekeeper use: cheap read-time heuristics over published substrate data

The substrate is intentionally allowed to spend more API money than Layer 1 because it is the system responsible for building the map that Layer 1 later uses cheaply.

## 16) Build Scope
- Separate substrate schema, pipeline runs, and review inbox
- Approved root-source registry and trust/freshness policy
- Immutable crawl artifact storage
- Strict extraction contracts for civic entities, aliases, roles, assertions, and jurisdictions
- Deterministic-first merge/resolution pipeline with LLM adjudication on ambiguity
- Validation gate and published snapshot system
- Temporal and spatial support sufficient for local-news gatekeeper use
- Review inbox / dead letter workflow for operational failures and drift
- Read-only integration into the newsroom gatekeeper and related runtime stages

## 17) Non-Goals (Initial Phase)
- Full open-ended web crawling of the public internet
- Real-time dictionary mutation from streaming signals
- Full GIS-grade spatial infrastructure
- Perfect coverage of every local entity class before Layer 1 integration starts
- Human review of every extracted fact

The goal is high leverage, not manual curation at internet scale.

## 18) Approved Execution Order
Execution should proceed by substrate phase in dependency order.

### Phase A: Schema Foundation + Contract Vocabulary
- define canonical, staging, runtime, and snapshot table boundaries
- define contract enums/check constraints/status vocabularies
- define provenance, lineage, temporal, review-state, and snapshot primitives
- establish the schema contract the rest of the substrate will live inside

### Phase B: Root Source Registry + Crawl Artifact Ingestion
- implement approved root-source registry and trust-policy storage
- implement crawl artifact capture, hashing, and diff-aware ingestion
- keep ingestion thin but contract-correct

### Phase C: Extraction Candidate Pipeline
- implement strict extraction contracts and candidate persistence
- add extraction diagnostics, provenance capture, and anti-generic enforcement

### Phase D: Merge Proposals + Validation Gate
- implement deterministic-first resolution
- implement ambiguous-case adjudication paths
- implement proposal validation outcomes and review escalation

### Phase E: Promotion + Snapshot Publishing
- implement canonical-head mutation rules
- implement snapshot publish mechanics and active snapshot visibility
- enforce newsroom read boundary against published snapshots only

### Phase F: Freshness / Drift + Review Queue
- implement freshness monitoring, expiration checks, targeted refresh triggers, and review-queue workflows

### Phase G: Layer 1 Read Integration
- integrate published dictionary reads into gatekeeper normalization, locality, alias expansion, and stronger dedupe/event grouping

### Phase H: Maintenance-Mode Scheduling + Operational Polish
- move from initial build posture into weekly or biweekly refresh cadence
- add operational polish, safer retries, reporting, and admin refinements

### First-Pass Priority Classification
Must be contract-hard on first pass:
- Phase A: Schema Foundation + Contract Vocabulary
- Phase D: Merge Proposals + Validation Gate
- Phase E: Promotion + Snapshot Publishing
- Phase G: Layer 1 Read Integration

Can begin thinner, then deepen:
- Phase B: Root Source Registry + Crawl Artifact Ingestion
- Phase C: Extraction Candidate Pipeline
- Phase F: Freshness / Drift + Review Queue
- Phase H: Maintenance-Mode Scheduling + Operational Polish

### Phase A Special Rule
Phase A is not just database setup.

Phase A must lock the contract vocabulary for the substrate, including at minimum:
- source trust tier
- extraction candidate status
- merge proposal type
- validation outcome
- assertion validity status
- assertion review status
- alias/entity lifecycle status
- snapshot status
- review queue item type
- review queue severity
- pipeline run status

Phase A should be treated as the template phase for later substrate work.

## 19) Verification Principles
- Validate schema and promotion constraints before runtime rollout
- Test promotion only from approved staged proposals
- Verify expired assertions stop reading as current
- Verify spatial locality decisions improve when coordinates/jurisdictions are present
- Verify review queue captures broken roots and repeated extraction failures
- Verify newsroom runtime reads only published snapshots
- Verify no chat/article/user-feedback path can mutate canonical substrate state

## 20) Summary
The dictionary substrate is a separate intermittent system whose job is to build and maintain the newsroom's map of local reality.

It is deliberately slower, more expensive, more rigid, and more provenance-heavy than the streaming news pipeline.

If built correctly, it will let Layer 1 and the rest of the newsroom operate faster and more safely without letting noisy runtime signals poison the canonical civic dictionary.
