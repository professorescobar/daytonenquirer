# Phase 6 Full Overview (Captured 2026-03-09)

Source note: This file captures the full Phase 6 overview exactly as provided in chat, for cross-chat audit reference without re-pasting long context.

---

Full checklist (source of turth):

**Phase 6 Strict Execution Checklist (No Commit/Push Until Team Green Light)**

1. **Guardrails First**
1. Confirm branch is `feature/topic-engine`.
2. Confirm rule: no `git add/commit/push` until explicit team green light.
3. Snapshot current status (`git status --short`) and keep unrelated changes untouched.

2. **Migration Block (Do All Before Runtime Code)**
1. Create migration file: `/Users/chasecranmer/dayton_enquirer/scripts/migrations/20260308_17_layer6_persona_image_controls.sql`
- Add persona/topic-engine image config columns:
  - `image_db_enabled`
  - `image_sourcing_enabled`
  - `image_generation_enabled`
  - `image_mode`
  - `image_profile`
  - `image_fallback_asset_url`
  - `image_fallback_cloudinary_public_id`
  - `quota_postgres_image_daily`
  - `quota_sourced_image_daily`
  - `quota_generated_image_daily`
  - `quota_text_only_daily`
  - `layer6_timeout_seconds`
  - `layer6_budget_usd`
  - `exa_max_attempts`
  - `generation_max_attempts`
- Add defaults/check constraints.

2. Create migration file: `/Users/chasecranmer/dayton_enquirer/scripts/migrations/20260308_18_layer6_image_pipeline_tables.sql`
- Create `image_pipeline_runs` table.
- Create `image_candidates` table.
- Include fields for attempts, scores, costs, rejection reasons, and Cloudinary metadata.

3. Create migration file: `/Users/chasecranmer/dayton_enquirer/scripts/migrations/20260308_19_layer6_article_image_status_and_placement.sql`
- Add article fields:
  - `image_status` (`with_image|text_only`)
  - `image_status_changed_at`
  - `render_class` (`with_image|text_only`)
  - `placement_eligible` (jsonb/text[])
- Add non-destructive backfill.

4. Create migration file: `/Users/chasecranmer/dayton_enquirer/scripts/migrations/20260308_20_layer6_indexes_and_contracts.sql`
- Indexes for run lookup, candidate lookup, text-only follow-up sorting.
- Uniqueness for canonical selected image per run/signal.
- Constraint hardening in duplicate-safe/non-destructive style.

5. Self-audit migration safety
- Idempotent.
- No destructive deletes.
- Constraints added as `NOT VALID` then conditionally validated if needed.
- Duplicate-safe index creation where needed.

3. **Persona Config API/UI Wiring**
1. Edit `/Users/chasecranmer/dayton_enquirer/api/admin-personas.js`
- Read/write new config fields.
- Normalize/sanitize defaults and bounds.
- Preserve existing payload shape compatibility.

2. Edit `/Users/chasecranmer/dayton_enquirer/public/admin-settings.js`
- Add controls for new image capability flags and limits.
- Add fallback image input fields.
- Add profile/mode selectors.
- Ensure save/load supports all new fields.

4. **Layer 6 Runtime (Pipeline/Event)**
1. Edit `/Users/chasecranmer/dayton_enquirer/inngest/gatekeeper-pipeline.ts`
- Add Layer 6 types and helpers:
  - Postgres pass 1 + pass 2 candidate search
  - Exa search + contents enrichment
  - Flux generation fallback
  - Gemini Flash context scoring on finalists
  - Everypixel quality scoring (`stock` for Exa, `ugc` for generated)
  - Weighted scoring with trust coefficients
  - hard guard: context `< 5.0` reject
- Add Cloudinary upload persistence for selected/generated.
- Add persistence into `image_pipeline_runs` + `image_candidates`.
- Add final outcome logic:
  - selected image OR text-only fallback (always publish path available)

2. In same file, add/ensure event handlers:
- `createImageSourcingStartFunction` for `image.sourcing.start`.

3. Ensure chain from Layer 5:
- Story/Draft completion should emit `image.sourcing.start`.

4. Edit `/Users/chasecranmer/dayton_enquirer/inngest/functions.ts`
- Register new Layer 6 function.

5. If scheduler/manual routing touches next stage, update:
- `/Users/chasecranmer/dayton_enquirer/api/topic-engine-scheduled.js` (only if needed)
- keep routing consistent and non-recursive.

5. **Publish Contract + Placement Enforcement**
1. Edit publish flow file(s) where article rows are finalized (project-specific publish endpoint/module).
- Write `image_status`, `render_class`, `placement_eligible`.
- If no image: enforce `text_only` placement eligibility.

2. Edit frontend retrieval/render filters:
- `/Users/chasecranmer/dayton_enquirer/api/article.js` (if placement filtering happens server-side)
- `/Users/chasecranmer/dayton_enquirer/public/script.js` and/or section rendering code
- Ensure image-required slots never render text-only articles.

6. **Admin Follow-Up Workflow**
1. Edit `/Users/chasecranmer/dayton_enquirer/api/admin-articles.js` (or relevant list endpoint)
- expose `image_status`, `image_status_changed_at`.
- add filtering for text-only follow-up queue.
- sorting: `image_status_changed_at DESC`, then `published_at DESC`.

2. Edit `/Users/chasecranmer/dayton_enquirer/public/admin-settings.js` and/or article editor JS
- add badges: `With Image` / `Text-Only`.
- add filter toggle for text-only.

3. Add emergency replace endpoint:
- new file: `/Users/chasecranmer/dayton_enquirer/api/admin-emergency-image-replace.js` (or route style matching your app)
- behavior:
  - confirmation required client-side
  - fallback order: persona fallback image -> Postgres candidate -> remove image (text-only)
  - immediate save + invalidate cache paths
  - write immutable audit log entry.

4. Add emergency replace button + confirm modal in editor UI script/page.

7. **Audit Logging**
1. Add/extend audit persistence location (existing admin audit table or new migration table if needed).
2. Log:
- actor id/key
- article id
- previous image
- new image
- reason code
- timestamp.

8. **Config Defaults to Enforce**
1. `exa_max_attempts = 3`
2. `generation_max_attempts = 2`
3. `layer6_timeout_seconds = 90`
4. `layer6_budget_usd = 0.20`
5. Defaults:
- db enabled true
- sourcing enabled true
- generation enabled false
- mode manual for new personas
- profile professional

9. **Verification Block (Run Before Team Review)**
1. Type/script checks for touched runtime files.
2. SQL dry checks for new columns/tables/indexes.
3. Functional tests for one signal each outcome:
- Postgres-selected
- Exa-selected
- Generated-selected
- Persona fallback image
- Text-only fallback
4. Verify DB rows for `image_pipeline_runs` and `image_candidates`.
5. Verify placement enforcement (text-only excluded from image-required slots).
6. Verify emergency replace flow updates status + queue ordering.

10. **Team Audit Gate (Mandatory Stop)**
1. Present:
- changed file list
- migration list
- outcome screenshots/SQL outputs
- known risks/open questions
2. Wait for explicit team “green light”.
3. Only then:
- stage exact files
- commit
- push.

11. **Commit/Push Block (After Green Light Only)**
1. `git add` only approved files.
2. commit with Phase 6 message.
3. push `feature/topic-engine`.
4. final handoff note includes migration order and Neon run order.

*************************************
Condensed operator prompt version::
*************************************
Implement Phase 6 for /Users/chasecranmer/dayton_enquirer on branch feature/topic-engine.

Hard rule:
- Do NOT git add/commit/push until explicit team green light after audit review.

Execution order (strict):

1) Migrations first
- Create:
  - scripts/migrations/20260308_17_layer6_persona_image_controls.sql
  - scripts/migrations/20260308_18_layer6_image_pipeline_tables.sql
  - scripts/migrations/20260308_19_layer6_article_image_status_and_placement.sql
  - scripts/migrations/20260308_20_layer6_indexes_and_contracts.sql
- Add persona image config fields:
  image_db_enabled, image_sourcing_enabled, image_generation_enabled, image_mode(manual|auto), image_profile(professional|creative|cheap),
  image_fallback_asset_url, image_fallback_cloudinary_public_id,
  quota_postgres_image_daily, quota_sourced_image_daily, quota_generated_image_daily, quota_text_only_daily,
  layer6_timeout_seconds(default 90), layer6_budget_usd(default 0.20), exa_max_attempts(default 3), generation_max_attempts(default 2).
- Add image_pipeline_runs + image_candidates tables.
- Add article fields: image_status(with_image|text_only), image_status_changed_at, render_class(with_image|text_only), placement_eligible.
- Keep migrations idempotent, duplicate-safe, non-destructive.

2) Persona API/UI wiring
- Update api/admin-personas.js to read/write all new fields with normalization/defaults.
- Update public/admin-settings.js persona settings UI for these controls.

3) Layer 6 runtime/event chain
- Update inngest/gatekeeper-pipeline.ts:
  - Add Layer 6 flow:
    Postgres pass1 -> Postgres pass2(broader) -> Exa search+contents -> Flux fallback -> persona fallback image -> text-only publish.
  - Always continue pipeline; no recursion, no duplicate article creation.
  - Validate finalists with Gemini Flash context and Everypixel quality:
    - Exa images: Everypixel stock model
    - Flux images: Everypixel ugc model
    - DB images: Gemini context check on finalists
  - Hard reject if context score < 5.0.
  - Weighted score by tier + trust coefficients.
  - Persist image_pipeline_runs + image_candidates metadata including rejection reasons/costs/latency.
  - Upload selected/generated images to Cloudinary and persist Cloudinary metadata.
  - Emit/consume image.sourcing.start as Layer 6 event.
- Register function in inngest/functions.ts.

4) Publish placement contract
- Ensure publish payload/data sets image_status, render_class, placement_eligible.
- Enforce frontend/server placement rules:
  - image-required slots: with_image only
  - text-only slots: sidebar/extra headlines only
- Never infer from missing image URL alone.

5) Admin follow-up + emergency replace
- Add text-only badges/filters and sorting by image_status_changed_at DESC then published_at DESC.
- Add emergency image replace action in editor with confirm modal:
  - fallback order: persona fallback -> postgres replacement -> remove image(text_only)
  - immediate update, cache invalidation, audit log entry.
- Keep status labels simple: with_image/text_only only.

6) Verification before review
- Run type/script checks for touched runtime files.
- Run SQL checks for schema/constraints/indexes.
- Validate outcomes for:
  - postgres-selected
  - exa-selected
  - generated-selected
  - persona

**
More:
**
Implement Phase 6 (Image Sourcing/Generation Waterfall) for /Users/chasecranmer/dayton_enquirer on branch feature/topic-engine.

Hard rule:
- Do NOT git add, commit, or push anything until I explicitly give “green light.”
- Keep unrelated local changes untouched.

Execution order (strict):
1) Migrations first
2) Pipeline/runtime
3) Admin API + UI
4) Frontend placement enforcement
5) Tests/audit report
6) Stop for sign-off

Phase 6 requirements:
- Waterfall: Postgres pass 1 -> Postgres pass 2 (broader) -> Exa Search+Contents -> Flux generation -> persona fallback image -> text-only publish.
- If no image is selected, always publish text-only (do not block breaking news).
- No recursive spawning/new topic creation from Layer 6.
- Persist Layer 6 telemetry/candidates/results, including score components, rejection reasons, attempts, estimated cost, latency, and Cloudinary metadata.
- Upload selected/generated images to Cloudinary and persist asset metadata.

Persona config fields (DB-backed; not separate endpoints):
- image_db_enabled (default true)
- image_sourcing_enabled (default true)
- image_generation_enabled (default false)
- image_mode (manual|auto, default manual for new personas)
- image_profile (professional|creative|cheap, default professional)
- image_fallback_asset_url (nullable)
- image_fallback_cloudinary_public_id (nullable)
- quota_postgres_image_daily
- quota_sourced_image_daily
- quota_generated_image_daily
- quota_text_only_daily
- layer6_timeout_seconds (default 90)
- layer6_budget_usd (default 0.20)
- exa_max_attempts (default 3)
- generation_max_attempts (default 2)

Scoring:
- Use Gemini Flash for context relevance on finalist images.
- Use Everypixel stock model for Exa-sourced images.
- Use Everypixel UGC model for generated images.
- DB images: context-first (Gemini finalist check), trust is high.
- Apply weighted scoring + trust coefficients per tier.
- Hard reject if context score < 5.0.
- Tie-break within 0.05 score delta: earlier tier wins.

Placement contract (hard):
- Persist image_status (with_image|text_only), image_status_changed_at, render_class, placement_eligible.
- Text-only articles must NEVER render in main/top/carousel/grid image-required slots.
- Text-only can appear in sidebar/extra-headlines areas.

Admin requirements:
- Persona settings UI controls for the above image flags/quotas.
- Article badges/filter for with_image vs text_only.
- Text-only follow-up sort: image_status_changed_at DESC, then published_at DESC.
- Add “Emergency Replace Now” in article editor with confirm modal:
  replacement order = persona fallback -> postgres replacement -> remove image (text_only).
- Log immutable audit entry for emergency replacements (who/when/why/old/new image).

Create migrations in this style:
- non-destructive
- idempotent
- duplicate-safe index/constraint handling

Suggested migration files:
- 20260308_17_layer6_persona_image_controls.sql
- 20260308_18_layer6_image_pipeline_tables.sql
- 20260308_19_layer6_article_image_status_and_placement.sql
- 20260308_20_layer6_indexes_and_contracts.sql

After implementation, provide:
1) exact files changed
2) migration audit summary
3) runtime flow summary
4) verification commands/queries run
5) known risks/open questions
Then STOP and wait for my green light before any git add/commit/push.
