-- Phase C / Chunk 1: extraction candidate contract + idempotency key
-- Safe to run multiple times.

ALTER TABLE dictionary.dictionary_extraction_candidates
  ADD COLUMN IF NOT EXISTS candidate_key TEXT;

UPDATE dictionary.dictionary_extraction_candidates
SET candidate_key = 'legacy:' || id::text
WHERE candidate_key IS NULL;

ALTER TABLE dictionary.dictionary_extraction_candidates
  ALTER COLUMN candidate_key SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dictionary_extraction_candidates_candidate_key_nonempty_chk'
      AND conrelid = 'dictionary.dictionary_extraction_candidates'::regclass
  ) THEN
    ALTER TABLE dictionary.dictionary_extraction_candidates
      ADD CONSTRAINT dictionary_extraction_candidates_candidate_key_nonempty_chk
      CHECK (length(trim(candidate_key)) > 0);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_extraction_candidates_artifact_contract_key
  ON dictionary.dictionary_extraction_candidates (
    crawl_artifact_id,
    extraction_version,
    candidate_type,
    candidate_key
  );

CREATE INDEX IF NOT EXISTS idx_dictionary_extraction_candidates_root_status
  ON dictionary.dictionary_extraction_candidates (root_source_id, status, created_at DESC);

COMMENT ON COLUMN dictionary.dictionary_extraction_candidates.candidate_key IS
  'Deterministic extraction-stage identity key scoped to crawl_artifact_id + extraction_version + candidate_type. Used for retry-safe candidate persistence and stable Phase D inputs.';

COMMENT ON COLUMN dictionary.dictionary_extraction_candidates.candidate_payload IS
  'Phase C payload contract. Typed candidates must use normalized vocab and include minimum required fields per candidate_type; jurisdiction_hint is a non-authoritative scoping hint only.';

COMMENT ON COLUMN dictionary.dictionary_extraction_candidates.rejection_reason IS
  'Machine-usable Phase C rejection reason for item-level extraction rejection. Artifact-level contract failures belong in dictionary_review_queue as extraction_contract_failure.';
