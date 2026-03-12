-- Phase D / Chunk 1: merge proposal + validation gate schema contract
-- Safe to run multiple times.

DO $$
DECLARE
  merge_proposal_constraint_name TEXT;
  merge_proposal_constraint_def TEXT;
BEGIN
  SELECT c.conname, pg_get_constraintdef(c.oid)
  INTO merge_proposal_constraint_name, merge_proposal_constraint_def
  FROM pg_constraint c
  JOIN pg_type t
    ON t.oid = c.contypid
  JOIN pg_namespace n
    ON n.oid = t.typnamespace
  WHERE n.nspname = 'dictionary'
    AND t.typname = 'merge_proposal_type'
    AND c.contype = 'c'
  LIMIT 1;

  IF merge_proposal_constraint_def IS NULL
     OR merge_proposal_constraint_def NOT LIKE '%create_role%'
     OR merge_proposal_constraint_def NOT LIKE '%create_jurisdiction%'
  THEN
    IF merge_proposal_constraint_name IS NOT NULL THEN
      EXECUTE format(
        'ALTER DOMAIN dictionary.merge_proposal_type DROP CONSTRAINT %I',
        merge_proposal_constraint_name
      );
    END IF;

    ALTER DOMAIN dictionary.merge_proposal_type
      ADD CONSTRAINT dictionary_merge_proposal_type_check
      CHECK (VALUE IN (
        'create_entity',
        'add_alias',
        'create_role',
        'create_assertion',
        'create_jurisdiction',
        'supersede_assertion',
        'retire_alias',
        'merge_duplicate'
      ));
  END IF;
END $$;

ALTER TABLE dictionary.dictionary_merge_proposals
  ADD COLUMN IF NOT EXISTS proposal_key TEXT;

UPDATE dictionary.dictionary_merge_proposals
SET proposal_key = 'legacy:' || id::text
WHERE proposal_key IS NULL;

ALTER TABLE dictionary.dictionary_merge_proposals
  ALTER COLUMN proposal_key SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dictionary_merge_proposals_proposal_key_nonempty_chk'
      AND conrelid = 'dictionary.dictionary_merge_proposals'::regclass
  ) THEN
    ALTER TABLE dictionary.dictionary_merge_proposals
      ADD CONSTRAINT dictionary_merge_proposals_proposal_key_nonempty_chk
      CHECK (length(trim(proposal_key)) > 0);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_merge_proposals_proposal_key
  ON dictionary.dictionary_merge_proposals (proposal_key);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_validation_results_proposal_validator
  ON dictionary.dictionary_validation_results (
    merge_proposal_id,
    COALESCE(validator_name, '')
  );

CREATE OR REPLACE FUNCTION dictionary.enforce_merge_proposal_target_reference()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.target_record_type IS NULL OR NEW.target_record_id IS NULL THEN
    RETURN NEW;
  END IF;

  IF NOT dictionary.canonical_record_exists(NEW.target_record_type, NEW.target_record_id) THEN
    RAISE EXCEPTION 'dictionary_merge_proposals must reference an existing canonical target record';
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dictionary_merge_proposals_target_reference
  ON dictionary.dictionary_merge_proposals;

CREATE TRIGGER trg_dictionary_merge_proposals_target_reference
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_merge_proposals
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_merge_proposal_target_reference();

COMMENT ON COLUMN dictionary.dictionary_merge_proposals.proposal_key IS
  'Deterministic Phase D identity for retry-safe merge proposal persistence. This key must remain stable across reruns for the same candidate-to-canonical resolution outcome.';

COMMENT ON INDEX uq_dictionary_merge_proposals_proposal_key IS
  'Ensures one durable Phase D proposal identity per deterministic resolution outcome.';

COMMENT ON INDEX uq_dictionary_validation_results_proposal_validator IS
  'Prevents duplicate validator outcomes for the same proposal and validator identity.';

COMMENT ON FUNCTION dictionary.enforce_merge_proposal_target_reference() IS
  'Prevents merge proposals from pointing at non-existent canonical head records during deterministic resolution.';
