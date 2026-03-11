-- Phase A / Chunk 1: substrate schema namespace + contract vocabulary
-- Safe to run multiple times.

CREATE SCHEMA IF NOT EXISTS dictionary;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'source_trust_tier'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.source_trust_tier AS TEXT
      CHECK (VALUE IN ('authoritative', 'corroborative', 'contextual'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'extraction_candidate_status'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.extraction_candidate_status AS TEXT
      CHECK (VALUE IN ('pending', 'extracted', 'rejected', 'needs_review', 'failed'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'merge_proposal_type'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.merge_proposal_type AS TEXT
      CHECK (VALUE IN (
        'create_entity',
        'add_alias',
        'create_assertion',
        'supersede_assertion',
        'retire_alias',
        'merge_duplicate'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'validation_outcome'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.validation_outcome AS TEXT
      CHECK (VALUE IN ('approved', 'rejected', 'needs_review', 'retryable_failure'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'assertion_validity_status'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.assertion_validity_status AS TEXT
      CHECK (VALUE IN ('current', 'scheduled', 'expired', 'superseded', 'unknown'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'assertion_review_status'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.assertion_review_status AS TEXT
      CHECK (VALUE IN ('verified', 'pending_refresh', 'needs_review', 'blocked'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'canonical_record_status'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.canonical_record_status AS TEXT
      CHECK (VALUE IN ('active', 'retired', 'superseded', 'blocked'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'snapshot_status'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.snapshot_status AS TEXT
      CHECK (VALUE IN ('building', 'published', 'superseded', 'rolled_back'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'review_queue_item_type'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.review_queue_item_type AS TEXT
      CHECK (VALUE IN (
        'fetch_failure',
        'artifact_parse_failure',
        'extraction_contract_failure',
        'merge_ambiguity',
        'validation_failure',
        'promotion_blocked',
        'freshness_overdue',
        'expired_high_impact_assertion'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'review_queue_severity'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.review_queue_severity AS TEXT
      CHECK (VALUE IN ('low', 'medium', 'high', 'critical'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'pipeline_run_status'
      AND n.nspname = 'dictionary'
  ) THEN
    CREATE DOMAIN dictionary.pipeline_run_status AS TEXT
      CHECK (VALUE IN ('queued', 'running', 'succeeded', 'failed', 'partial', 'needs_review'));
  END IF;
END $$;

COMMENT ON SCHEMA dictionary IS
  'Dictionary substrate schema. Canonical head is mutable build-state for the substrate only; newsroom reads must resolve against published snapshots only.';

COMMENT ON DOMAIN dictionary.source_trust_tier IS
  'Trust policy for approved root sources. This is source authority, not record lifecycle or review state.';

COMMENT ON DOMAIN dictionary.extraction_candidate_status IS
  'Lifecycle state for staged extraction candidates prior to merge proposal generation.';

COMMENT ON DOMAIN dictionary.merge_proposal_type IS
  'Explicit promotion-intent action emitted by merge resolution before validation and promotion.';

COMMENT ON DOMAIN dictionary.validation_outcome IS
  'Validation gate result for a merge proposal or staged fact. Only approved outcomes may progress toward promotion.';

COMMENT ON DOMAIN dictionary.assertion_validity_status IS
  'Truth-time state of an assertion. Separate from review status and record lifecycle.';

COMMENT ON DOMAIN dictionary.assertion_review_status IS
  'Operational trust state for an assertion. Separate from temporal validity.';

COMMENT ON DOMAIN dictionary.canonical_record_status IS
  'Lifecycle state for durable canonical records such as entities, aliases, roles, and jurisdictions.';

COMMENT ON DOMAIN dictionary.snapshot_status IS
  'Publication lifecycle for immutable newsroom-readable snapshots.';

COMMENT ON DOMAIN dictionary.review_queue_item_type IS
  'Operational exception class for substrate review queue items.';

COMMENT ON DOMAIN dictionary.review_queue_severity IS
  'Operator urgency for substrate review queue items.';

COMMENT ON DOMAIN dictionary.pipeline_run_status IS
  'Execution state for substrate pipeline runs and stage-level audits.';
