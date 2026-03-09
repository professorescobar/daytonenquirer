# Migration Consolidation Audit (2026-03-09)

## Purpose
This document consolidates the new migration chain added during the Phase 6 rollout and related hardening work. It is intended as a deployment approval checklist so the team can decide exactly what is **required now** vs **deferred/optional**.

## Scope Reviewed
- `scripts/migrations/20260308_17_layer6_persona_image_controls.sql`
- `scripts/migrations/20260308_18_layer6_image_pipeline_tables.sql`
- `scripts/migrations/20260308_19_layer6_article_image_status_and_placement.sql`
- `scripts/migrations/20260308_20_layer6_indexes_and_contracts.sql`
- `scripts/migrations/20260308_21_articles_slug_uniqueness.sql`
- `scripts/migrations/20260309_22_related_article_search_slug_normalization_fix.sql`
- `scripts/migrations/20260309_23_article_drafts_codex_idempotency_key.sql`
- `scripts/migrations/20260309_24_article_drafts_codex_idempotency_hardening.sql`
- `scripts/migrations/20260309_25_topic_engine_chat_feedback_table.sql`
- `scripts/migrations/20260309_26_runtime_table_bootstrap_replacement.sql`
- `scripts/migrations/20260309_27_newsletter_and_archive_runtime_bootstrap_replacement.sql`
- `scripts/migrations/20260309_28_api_rate_limits_shared_store.sql`

Also verified dependency context in:
- `scripts/migrations/20260304_03_topic_engine_activation_and_dedupe.sql`
- `scripts/migrations/20260304_04_related_article_search.sql`
- `scripts/migrations/20260304_05_gatekeeper_signals.sql`

---

## Executive Summary
- The chain is **not spaghetti**: the migrations are clustered into coherent functional groups.
- Most files are **forward-fixes for real runtime hardening findings** (runtime DDL removal, idempotency, schema contracts, distributed rate-limit storage).
- Main risk now is **operational complexity**, not obvious logical conflict.
- Recommended path: approve in **2 bundles**:
  - Bundle A (Phase 6 + critical compatibility): `17,18,19,20,21,22`
  - Bundle B (non-Phase-6 hardening): `23,24,25,26,27,28`

---

## Classification Matrix

### Required for Phase 6 Core Contract
1. `20260308_17_layer6_persona_image_controls.sql`
- Why: adds persona-level Layer 6 controls/quotas/defaults used by runtime + admin UI.
- Risk if skipped: runtime/UI config mismatch, null field behavior, missing constraints.

2. `20260308_18_layer6_image_pipeline_tables.sql`
- Why: creates `image_pipeline_runs` + `image_candidates` telemetry tables.
- Risk if skipped: Layer 6 persistence failures.

3. `20260308_19_layer6_article_image_status_and_placement.sql`
- Why: introduces article placement contract (`image_status`, `render_class`, `placement_eligible`) + constraints.
- Risk if skipped: frontend/server placement drift and contract violations.

4. `20260308_20_layer6_indexes_and_contracts.sql`
- Why: enforces Layer 6 uniqueness/contract hardening and audit table.
- Risk if skipped: replay/idempotency inconsistencies, weaker integrity/perf.

5. `20260308_21_articles_slug_uniqueness.sql`
- Why: normalizes slug uniqueness to match newer conflict strategy.
- Risk if skipped: runtime `ON CONFLICT ((lower(trim(slug))))` paths can fail.

6. `20260309_22_related_article_search_slug_normalization_fix.sql`
- Why: forward-fix for related-article function because historical migration edits do not rerun.
- Risk if skipped: code appears fixed but DB function remains stale in existing environments.

### Required for Already-Merged Runtime Hardening (outside strict Phase 6)
7. `20260309_23_article_drafts_codex_idempotency_key.sql`
- Why: introduces codex idempotency key column/index for new codex flow.
- Risk if skipped: codex endpoints referencing key fail or lose dedupe guarantees.

8. `20260309_24_article_drafts_codex_idempotency_hardening.sql`
- Why: removes legacy unique `source_url` coupling and hardens normalized idempotency uniqueness.
- Risk if skipped: architecture mismatch, false conflicts, weak idempotency behavior.

9. `20260309_25_topic_engine_chat_feedback_table.sql`
- Why: supports chat feedback persistence without runtime DDL.
- Risk if skipped: degraded observability path or 503 fallback if table expected.

10. `20260309_26_runtime_table_bootstrap_replacement.sql`
- Why: replaces multiple request-path schema bootstrap behaviors with migration-owned tables/columns.
- Risk if skipped: endpoints now returning 503 due to missing tables after runtime-DDL removal.

11. `20260309_27_newsletter_and_archive_runtime_bootstrap_replacement.sql`
- Why: same as 26 for newsletter + archive subsystems.
- Risk if skipped: 503s in newsletter/maintenance paths.

12. `20260309_28_api_rate_limits_shared_store.sql`
- Why: enables deployment-wide rate limiting (multi-instance/serverless-safe).
- Risk if skipped: endpoints can return 503 (current code expects shared table when DB limiter path enabled).

---

## Recommended Execution Order (Strict)
Run in this exact order:
1. `20260308_17_layer6_persona_image_controls.sql`
2. `20260308_18_layer6_image_pipeline_tables.sql`
3. `20260308_19_layer6_article_image_status_and_placement.sql`
4. `20260308_20_layer6_indexes_and_contracts.sql`
5. `20260308_21_articles_slug_uniqueness.sql`
6. `20260309_22_related_article_search_slug_normalization_fix.sql`
7. `20260309_23_article_drafts_codex_idempotency_key.sql`
8. `20260309_24_article_drafts_codex_idempotency_hardening.sql`
9. `20260309_25_topic_engine_chat_feedback_table.sql`
10. `20260309_26_runtime_table_bootstrap_replacement.sql`
11. `20260309_27_newsletter_and_archive_runtime_bootstrap_replacement.sql`
12. `20260309_28_api_rate_limits_shared_store.sql`

---

## Dependency Notes and Gotchas
- `03` must exist before `26` mutates `personas`; this is now satisfied because `20260304_03` includes `CREATE TABLE IF NOT EXISTS personas`.
- `21` is a behavioral pivot: slug uniqueness now uses normalized expression index. Any SQL using `ON CONFLICT (slug)` must be updated (already addressed in current runtime/scripts).
  - Operational note: this migration now builds the normalized index with `CONCURRENTLY` (`uq_articles_slug_norm`) to reduce write-lock contention.
- `22` is intentionally a forward migration to avoid relying on edited historical migration bodies.
- `23` + `24` are complementary; do not run 24 without 23.
- `28` is now required by the current DB-backed limiter implementation.
- `21` contains `CREATE INDEX CONCURRENTLY`; migration runner must execute this file outside a transaction wrapper.

---

## Safety / Reversibility
- These migrations are non-destructive in the sense of no table drops of primary business entities.
- They are **not all trivially reversible**:
  - `21` de-duplicates slugs by rewriting duplicates.
  - `23`/`24` can null duplicate idempotency keys.
  - `20` performs canonicalization/demotion logic in telemetry metadata.
- Recommendation: take a DB snapshot before execution.

---

## Approval Recommendation
Approve all `17–28` as one planned migration wave **only if** deployment window includes rollback/snapshot coverage.

If team wants reduced blast radius:
- Wave 1 (Phase 6 go-live): `17–22`
- Wave 2 (cross-system hardening already in code): `23–28`

Given current runtime code already references artifacts from `23–28`, deferring Wave 2 may require temporary code guards. Without those guards, full chain is the safest operationally consistent path.

---

## Pre-Deploy Verification Checklist
1. Confirm pending migration files exactly match approved list.
2. Snapshot DB.
3. Run migrations in order.
4. Run smoke queries:
- `SELECT to_regclass('public.image_pipeline_runs');`
- `SELECT to_regclass('public.image_candidates');`
- `SELECT to_regclass('public.topic_signals');`
- `SELECT to_regclass('public.topic_engine_chat_feedback');`
- `SELECT to_regclass('public.api_rate_limits');`
5. Verify constraints/indexes exist:
- `uq_image_pipeline_runs_signal_source_event`
- `uq_image_candidates_selected_per_run`
- `uq_articles_slug_norm`
- `uq_article_drafts_codex_idempotency_key_norm`
6. Hit runtime health paths that previously depended on runtime DDL (admin personas/topic-engine/newsletter/maintenance/chat/summarize).

---

## Post-Deploy Ops Note
- `api_rate_limits` retention is implemented in `api/admin-maintenance.js`:
  - Parameters: `apiRateLimitDays` (default `14`), `apiRateLimitBatchSize` (default `5000`)
  - Includes dry-run eligibility counting and non-dry-run pruning metrics.
- Recommended schedule: run maintenance daily with non-dry-run in a low-traffic window.

---

## Decision Log Template
- Approved by:
- Date:
- Scope approved (files):
- Deferred migrations (if any):
- Required compensating code guards (if deferrals exist):
