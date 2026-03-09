-- Phase 6: Layer 6 indexes and hardening contracts
-- Safe to run multiple times. Duplicate-safe and non-destructive.

DO $$
BEGIN
  -- Run/candidate lookup (only when prerequisite tables exist)
  IF to_regclass('public.image_pipeline_runs') IS NOT NULL THEN
    -- De-dupe canonical idempotency keys without stripping sourceEventId provenance.
    -- Keep newest canonical row per (signal_id, sourceEventId), demote older canonicals.
    WITH ranked_canonical_events AS (
      SELECT
        id,
        ROW_NUMBER() OVER (
          PARTITION BY signal_id, diagnostics->>'sourceEventId'
          ORDER BY COALESCE(updated_at, created_at, started_at) DESC, id DESC
        ) AS rn
      FROM image_pipeline_runs
      WHERE diagnostics ? 'sourceEventId'
        AND signal_id IS NOT NULL
        AND jsonb_typeof(COALESCE(diagnostics, '{}'::jsonb)) = 'object'
        AND COALESCE(diagnostics->>'idempotencyCanonical', 'false') = 'true'
        AND trim(COALESCE(diagnostics->>'sourceEventId', '')) <> ''
    )
    UPDATE image_pipeline_runs r
    SET
      diagnostics = jsonb_set(COALESCE(r.diagnostics, '{}'::jsonb), '{idempotencyCanonical}', 'false'::jsonb, true),
      updated_at = NOW()
    FROM ranked_canonical_events e
    WHERE r.id = e.id
      AND e.rn > 1;

    CREATE INDEX IF NOT EXISTS idx_image_pipeline_runs_signal_started
      ON image_pipeline_runs (signal_id, started_at DESC);

    CREATE INDEX IF NOT EXISTS idx_image_pipeline_runs_persona_started
      ON image_pipeline_runs (persona_id, started_at DESC);

    CREATE INDEX IF NOT EXISTS idx_image_pipeline_runs_signal_source_event
      ON image_pipeline_runs (signal_id, (diagnostics->>'sourceEventId'))
      WHERE diagnostics ? 'sourceEventId'
        AND signal_id IS NOT NULL
        AND jsonb_typeof(COALESCE(diagnostics, '{}'::jsonb)) = 'object'
        AND COALESCE(diagnostics->>'idempotencyCanonical', 'false') = 'true'
        AND trim(COALESCE(diagnostics->>'sourceEventId', '')) <> '';

    CREATE UNIQUE INDEX IF NOT EXISTS uq_image_pipeline_runs_signal_source_event
      ON image_pipeline_runs (signal_id, (diagnostics->>'sourceEventId'))
      WHERE diagnostics ? 'sourceEventId'
        AND signal_id IS NOT NULL
        AND jsonb_typeof(COALESCE(diagnostics, '{}'::jsonb)) = 'object'
        AND COALESCE(diagnostics->>'idempotencyCanonical', 'false') = 'true'
        AND trim(COALESCE(diagnostics->>'sourceEventId', '')) <> '';
  END IF;

  IF to_regclass('public.image_candidates') IS NOT NULL THEN
    -- De-dupe legacy selected candidates before enforcing uniqueness.
    WITH ranked_selected AS (
      SELECT
        id,
        ROW_NUMBER() OVER (
          PARTITION BY run_id
          ORDER BY
            COALESCE(selected_rank, 999999) ASC,
            COALESCE(weighted_score, -1) DESC,
            created_at ASC,
            id ASC
        ) AS rn
      FROM image_candidates
      WHERE is_selected = TRUE
        AND run_id IS NOT NULL
    )
    UPDATE image_candidates c
    SET
      is_selected = FALSE,
      selected_rank = NULL,
      updated_at = NOW()
    FROM ranked_selected r
    WHERE c.id = r.id
      AND r.rn > 1;

    IF to_regclass('public.image_pipeline_runs') IS NOT NULL THEN
      WITH canonical_selected AS (
        SELECT
          run_id,
          id,
          candidate_tier,
          image_url,
          image_credit,
          source_url,
          cloudinary_public_id,
          cloudinary_secure_url,
          cloudinary_asset_metadata
        FROM image_candidates
        WHERE is_selected = TRUE
          AND run_id IS NOT NULL
      )
      UPDATE image_pipeline_runs r
      SET
        selected_candidate_id = c.id,
        selected_tier = c.candidate_tier,
        selected_image_url = COALESCE(c.cloudinary_secure_url, c.image_url),
        selected_image_credit = c.image_credit,
        selected_source_url = c.source_url,
        selected_cloudinary_public_id = c.cloudinary_public_id,
        selected_cloudinary_secure_url = c.cloudinary_secure_url,
        selected_cloudinary_asset_metadata = COALESCE(c.cloudinary_asset_metadata, '{}'::jsonb),
        updated_at = NOW()
      FROM canonical_selected c
      WHERE r.id = c.run_id
        AND (
          r.selected_candidate_id IS DISTINCT FROM c.id
          OR r.selected_tier IS DISTINCT FROM c.candidate_tier
          OR r.selected_image_url IS DISTINCT FROM COALESCE(c.cloudinary_secure_url, c.image_url)
          OR r.selected_image_credit IS DISTINCT FROM c.image_credit
          OR r.selected_source_url IS DISTINCT FROM c.source_url
          OR r.selected_cloudinary_public_id IS DISTINCT FROM c.cloudinary_public_id
          OR r.selected_cloudinary_secure_url IS DISTINCT FROM c.cloudinary_secure_url
          OR COALESCE(r.selected_cloudinary_asset_metadata, '{}'::jsonb) IS DISTINCT FROM COALESCE(c.cloudinary_asset_metadata, '{}'::jsonb)
        );

      UPDATE image_pipeline_runs r
      SET
        selected_candidate_id = NULL,
        selected_tier = NULL,
        selected_image_url = NULL,
        selected_image_credit = NULL,
        selected_source_url = NULL,
        selected_cloudinary_public_id = NULL,
        selected_cloudinary_secure_url = NULL,
        selected_cloudinary_asset_metadata = '{}'::jsonb,
        updated_at = NOW()
      WHERE r.selected_candidate_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM image_candidates c
          WHERE c.id = r.selected_candidate_id
            AND c.run_id = r.id
            AND c.is_selected = TRUE
        );
    END IF;

    CREATE INDEX IF NOT EXISTS idx_image_candidates_run_score
      ON image_candidates (run_id, weighted_score DESC, created_at DESC);

    CREATE INDEX IF NOT EXISTS idx_image_candidates_signal_created
      ON image_candidates (signal_id, created_at DESC)
      WHERE signal_id IS NOT NULL;

    CREATE INDEX IF NOT EXISTS idx_image_candidates_rejected
      ON image_candidates (run_id, rejected, created_at DESC);

    -- Canonical selected image uniqueness per run.
    CREATE UNIQUE INDEX IF NOT EXISTS uq_image_candidates_selected_per_run
      ON image_candidates (run_id)
      WHERE is_selected = TRUE;
  END IF;

  IF to_regclass('public.articles') IS NOT NULL THEN
    -- Text-only follow-up sorting
    CREATE INDEX IF NOT EXISTS idx_articles_text_only_followup
      ON articles (image_status_changed_at DESC, pub_date DESC)
      WHERE image_status = 'text_only' AND COALESCE(status, 'published') = 'published';
  END IF;
END $$;

-- Do not enforce one selected candidate across all runs for a signal;
-- retries/reprocessing must be allowed.
DROP INDEX IF EXISTS uq_image_candidates_selected_per_signal;

CREATE TABLE IF NOT EXISTS topic_engine_image_replace_audit (
  id BIGSERIAL PRIMARY KEY,
  actor_key TEXT,
  article_id BIGINT NOT NULL,
  reason_code TEXT NOT NULL,
  previous_image TEXT,
  new_image TEXT,
  method TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_topic_engine_image_replace_audit_article_created
  ON topic_engine_image_replace_audit (article_id, created_at DESC);

DO $$
BEGIN
  IF to_regclass('public.image_pipeline_runs') IS NULL OR to_regclass('public.image_candidates') IS NULL THEN
    RETURN;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_pipeline_runs_selected_candidate_fk'
  ) THEN
    ALTER TABLE image_pipeline_runs
      ADD CONSTRAINT image_pipeline_runs_selected_candidate_fk
      FOREIGN KEY (selected_candidate_id)
      REFERENCES image_candidates(id)
      DEFERRABLE INITIALLY DEFERRED
      NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_candidates_run_id_id_uniq'
  ) THEN
    ALTER TABLE image_candidates
      ADD CONSTRAINT image_candidates_run_id_id_uniq
      UNIQUE (run_id, id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_pipeline_runs_selected_candidate_same_run_fk'
  ) THEN
    ALTER TABLE image_pipeline_runs
      ADD CONSTRAINT image_pipeline_runs_selected_candidate_same_run_fk
      FOREIGN KEY (id, selected_candidate_id)
      REFERENCES image_candidates(run_id, id)
      DEFERRABLE INITIALLY DEFERRED
      NOT VALID;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_pipeline_runs_terminal_outcome_chk'
  ) THEN
    ALTER TABLE image_pipeline_runs
      DROP CONSTRAINT image_pipeline_runs_terminal_outcome_chk;
  END IF;

  ALTER TABLE image_pipeline_runs
    ADD CONSTRAINT image_pipeline_runs_terminal_outcome_chk
    CHECK (
      status NOT IN ('completed', 'timed_out')
      OR final_outcome IN ('postgres_selected', 'exa_selected', 'generated_selected', 'persona_fallback', 'text_only')
    ) NOT VALID;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'image_candidates_selected_has_url_chk'
  ) THEN
    ALTER TABLE image_candidates
      ADD CONSTRAINT image_candidates_selected_has_url_chk
      CHECK (NOT is_selected OR length(trim(COALESCE(image_url, ''))) > 0) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM image_pipeline_runs
    WHERE status IN ('completed', 'timed_out')
      AND final_outcome NOT IN ('postgres_selected', 'exa_selected', 'generated_selected', 'persona_fallback', 'text_only')
  ) THEN
    ALTER TABLE image_pipeline_runs VALIDATE CONSTRAINT image_pipeline_runs_terminal_outcome_chk;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM image_candidates
    WHERE is_selected = TRUE
      AND length(trim(COALESCE(image_url, ''))) = 0
  ) THEN
    ALTER TABLE image_candidates VALIDATE CONSTRAINT image_candidates_selected_has_url_chk;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM image_pipeline_runs r
    WHERE r.selected_candidate_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM image_candidates c
        WHERE c.id = r.selected_candidate_id
          AND c.run_id = r.id
      )
  ) THEN
    ALTER TABLE image_pipeline_runs VALIDATE CONSTRAINT image_pipeline_runs_selected_candidate_same_run_fk;
  END IF;
END $$;
