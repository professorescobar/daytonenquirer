-- Phase A / Chunk 6: cross-table contract hardening
-- Safe to run multiple times.

CREATE OR REPLACE FUNCTION dictionary.canonical_record_exists(record_type TEXT, record_id UUID)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
  SELECT CASE
    WHEN record_type = 'entity' THEN EXISTS (
      SELECT 1 FROM dictionary.dictionary_entities WHERE id = record_id
    )
    WHEN record_type = 'alias' THEN EXISTS (
      SELECT 1 FROM dictionary.dictionary_aliases WHERE id = record_id
    )
    WHEN record_type = 'role' THEN EXISTS (
      SELECT 1 FROM dictionary.dictionary_roles WHERE id = record_id
    )
    WHEN record_type = 'assertion' THEN EXISTS (
      SELECT 1 FROM dictionary.dictionary_assertions WHERE id = record_id
    )
    WHEN record_type = 'jurisdiction' THEN EXISTS (
      SELECT 1 FROM dictionary.dictionary_jurisdictions WHERE id = record_id
    )
    ELSE FALSE
  END;
$$;

CREATE OR REPLACE FUNCTION dictionary.enforce_provenance_record_reference()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT dictionary.canonical_record_exists(NEW.record_type, NEW.record_id) THEN
    RAISE EXCEPTION 'dictionary_provenance must reference an existing canonical record';
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.enforce_snapshot_record_reference()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT dictionary.canonical_record_exists(NEW.record_type, NEW.record_id) THEN
    RAISE EXCEPTION 'dictionary_snapshot_records must reference an existing canonical record';
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.enforce_extraction_candidate_lineage()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  artifact_root_source_id UUID;
  artifact_run_id UUID;
BEGIN
  SELECT root_source_id, substrate_run_id
  INTO artifact_root_source_id, artifact_run_id
  FROM dictionary.dictionary_crawl_artifacts
  WHERE id = NEW.crawl_artifact_id;

  IF artifact_root_source_id IS DISTINCT FROM NEW.root_source_id THEN
    RAISE EXCEPTION 'extraction candidate root_source_id must match crawl artifact root_source_id';
  END IF;

  IF artifact_run_id IS DISTINCT FROM NEW.substrate_run_id THEN
    RAISE EXCEPTION 'extraction candidate substrate_run_id must match crawl artifact substrate_run_id';
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.enforce_merge_proposal_lineage()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  candidate_run_id UUID;
BEGIN
  SELECT substrate_run_id
  INTO candidate_run_id
  FROM dictionary.dictionary_extraction_candidates
  WHERE id = NEW.extraction_candidate_id;

  IF candidate_run_id IS DISTINCT FROM NEW.substrate_run_id THEN
    RAISE EXCEPTION 'merge proposal substrate_run_id must match extraction candidate substrate_run_id';
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.enforce_validation_result_lineage()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  proposal_run_id UUID;
BEGIN
  SELECT substrate_run_id
  INTO proposal_run_id
  FROM dictionary.dictionary_merge_proposals
  WHERE id = NEW.merge_proposal_id;

  IF proposal_run_id IS DISTINCT FROM NEW.substrate_run_id THEN
    RAISE EXCEPTION 'validation result substrate_run_id must match merge proposal substrate_run_id';
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.enforce_review_queue_lineage()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  artifact_root_source_id UUID;
BEGIN
  IF NEW.crawl_artifact_id IS NOT NULL AND NEW.root_source_id IS NOT NULL THEN
    SELECT root_source_id
    INTO artifact_root_source_id
    FROM dictionary.dictionary_crawl_artifacts
    WHERE id = NEW.crawl_artifact_id;

    IF artifact_root_source_id IS DISTINCT FROM NEW.root_source_id THEN
      RAISE EXCEPTION 'review queue root_source_id must match crawl artifact root_source_id';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dictionary_provenance_record_reference
  ON dictionary.dictionary_provenance;

CREATE TRIGGER trg_dictionary_provenance_record_reference
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_provenance
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_provenance_record_reference();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_records_reference
  ON dictionary.dictionary_snapshot_records;

CREATE TRIGGER trg_dictionary_snapshot_records_reference
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_snapshot_records
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_snapshot_record_reference();

DROP TRIGGER IF EXISTS trg_dictionary_extraction_candidates_lineage
  ON dictionary.dictionary_extraction_candidates;

CREATE TRIGGER trg_dictionary_extraction_candidates_lineage
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_extraction_candidates
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_extraction_candidate_lineage();

DROP TRIGGER IF EXISTS trg_dictionary_merge_proposals_lineage
  ON dictionary.dictionary_merge_proposals;

CREATE TRIGGER trg_dictionary_merge_proposals_lineage
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_merge_proposals
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_merge_proposal_lineage();

DROP TRIGGER IF EXISTS trg_dictionary_validation_results_lineage
  ON dictionary.dictionary_validation_results;

CREATE TRIGGER trg_dictionary_validation_results_lineage
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_validation_results
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_validation_result_lineage();

DROP TRIGGER IF EXISTS trg_dictionary_review_queue_lineage
  ON dictionary.dictionary_review_queue;

CREATE TRIGGER trg_dictionary_review_queue_lineage
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_review_queue
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_review_queue_lineage();

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_provenance_record_source_identity
  ON dictionary.dictionary_provenance (
    record_type,
    record_id,
    COALESCE(root_source_id, '00000000-0000-0000-0000-000000000000'::uuid),
    COALESCE(crawl_artifact_id, '00000000-0000-0000-0000-000000000000'::uuid),
    COALESCE(source_url, ''),
    extraction_version
  );

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_records_snapshot_type
  ON dictionary.dictionary_snapshot_records (snapshot_id, record_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_review_queue_open_root_item
  ON dictionary.dictionary_review_queue (root_source_id, item_type, last_failed_at DESC)
  WHERE resolved_at IS NULL
    AND root_source_id IS NOT NULL;

COMMENT ON FUNCTION dictionary.canonical_record_exists(TEXT, UUID) IS
  'Helper for schema-level contract enforcement across polymorphic canonical record references.';

COMMENT ON FUNCTION dictionary.enforce_provenance_record_reference() IS
  'Prevents provenance rows from referencing non-existent canonical records.';

COMMENT ON FUNCTION dictionary.enforce_snapshot_record_reference() IS
  'Prevents snapshot membership from referencing non-existent canonical records.';
