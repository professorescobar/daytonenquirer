-- Phase E / Chunk 1: promotion ledger + immutable snapshot payload contract
-- Safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS dictionary.dictionary_promotion_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  substrate_run_id UUID NOT NULL REFERENCES dictionary.dictionary_pipeline_runs(id),
  merge_proposal_id UUID NOT NULL REFERENCES dictionary.dictionary_merge_proposals(id),
  validation_result_id UUID NOT NULL REFERENCES dictionary.dictionary_validation_results(id),
  snapshot_id UUID REFERENCES dictionary.dictionary_snapshots(id),
  promotion_outcome TEXT NOT NULL,
  created_record_type TEXT,
  created_record_id UUID,
  affected_record_type TEXT,
  affected_record_id UUID,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_promotion_results_outcome_chk
    CHECK (promotion_outcome IN ('promoted', 'no_op')),
  CONSTRAINT dictionary_promotion_results_created_record_type_chk
    CHECK (
      created_record_type IS NULL
      OR created_record_type IN ('entity', 'alias', 'role', 'assertion', 'jurisdiction')
    ),
  CONSTRAINT dictionary_promotion_results_affected_record_type_chk
    CHECK (
      affected_record_type IS NULL
      OR affected_record_type IN ('entity', 'alias', 'role', 'assertion', 'jurisdiction')
    ),
  CONSTRAINT dictionary_promotion_results_created_record_pair_chk
    CHECK (
      (created_record_type IS NULL AND created_record_id IS NULL)
      OR (created_record_type IS NOT NULL AND created_record_id IS NOT NULL)
    ),
  CONSTRAINT dictionary_promotion_results_affected_record_pair_chk
    CHECK (
      (affected_record_type IS NULL AND affected_record_id IS NULL)
      OR (affected_record_type IS NOT NULL AND affected_record_id IS NOT NULL)
    ),
  CONSTRAINT dictionary_promotion_results_details_object_chk
    CHECK (jsonb_typeof(details) = 'object')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_promotion_results_merge_proposal
  ON dictionary.dictionary_promotion_results (merge_proposal_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_promotion_results_validation_result
  ON dictionary.dictionary_promotion_results (validation_result_id);

CREATE INDEX IF NOT EXISTS idx_dictionary_promotion_results_run_created
  ON dictionary.dictionary_promotion_results (substrate_run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_promotion_results_snapshot_created
  ON dictionary.dictionary_promotion_results (snapshot_id, created_at DESC)
  WHERE snapshot_id IS NOT NULL;

CREATE OR REPLACE FUNCTION dictionary.enforce_promotion_result_references()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  first_pass_validator_name CONSTANT TEXT := 'phase_d_first_pass_validator_v1';
  validation_merge_proposal_id UUID;
  validation_validator_name TEXT;
  validation_outcome dictionary.validation_outcome;
  latest_effective_validation_id UUID;
BEGIN
  SELECT
    vr.merge_proposal_id,
    vr.validator_name,
    vr.outcome
  INTO
    validation_merge_proposal_id,
    validation_validator_name,
    validation_outcome
  FROM dictionary.dictionary_validation_results vr
  WHERE vr.id = NEW.validation_result_id;

  IF validation_merge_proposal_id IS DISTINCT FROM NEW.merge_proposal_id THEN
    RAISE EXCEPTION 'promotion result validation_result_id must reference the same merge proposal';
  END IF;

  IF validation_outcome IS DISTINCT FROM 'approved' THEN
    RAISE EXCEPTION 'promotion results may only reference approved validation outcomes';
  END IF;

  IF validation_validator_name IS DISTINCT FROM first_pass_validator_name THEN
    RAISE EXCEPTION 'promotion results may only reference the Phase D first-pass validator';
  END IF;

  SELECT vr.id
  INTO latest_effective_validation_id
  FROM dictionary.dictionary_validation_results vr
  WHERE vr.merge_proposal_id = NEW.merge_proposal_id
    AND vr.validator_name = first_pass_validator_name
  ORDER BY vr.created_at DESC, vr.id DESC
  LIMIT 1;

  IF latest_effective_validation_id IS DISTINCT FROM NEW.validation_result_id THEN
    RAISE EXCEPTION 'promotion results must reference the latest effective approved first-pass validation result';
  END IF;

  IF NEW.created_record_type IS NOT NULL
     AND NOT dictionary.canonical_record_exists(NEW.created_record_type, NEW.created_record_id)
  THEN
    RAISE EXCEPTION 'promotion result created record must reference an existing canonical record';
  END IF;

  IF NEW.affected_record_type IS NOT NULL
     AND NOT dictionary.canonical_record_exists(NEW.affected_record_type, NEW.affected_record_id)
  THEN
    RAISE EXCEPTION 'promotion result affected record must reference an existing canonical record';
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dictionary_promotion_results_references
  ON dictionary.dictionary_promotion_results;

CREATE TRIGGER trg_dictionary_promotion_results_references
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_promotion_results
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_promotion_result_references();

CREATE TABLE IF NOT EXISTS dictionary.dictionary_snapshot_entities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_id UUID NOT NULL REFERENCES dictionary.dictionary_snapshots(id),
  canonical_record_id UUID NOT NULL,
  entity_type TEXT NOT NULL,
  canonical_name TEXT NOT NULL,
  slug TEXT NOT NULL,
  primary_jurisdiction_id UUID,
  normalized_address TEXT,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  spatial_confidence DOUBLE PRECISION,
  status dictionary.canonical_record_status NOT NULL,
  description TEXT,
  notes TEXT,
  attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,

  CONSTRAINT dictionary_snapshot_entities_record_unique_chk
    UNIQUE (snapshot_id, canonical_record_id),
  CONSTRAINT dictionary_snapshot_entities_type_nonempty_chk
    CHECK (length(trim(entity_type)) > 0),
  CONSTRAINT dictionary_snapshot_entities_canonical_name_nonempty_chk
    CHECK (length(trim(canonical_name)) > 0),
  CONSTRAINT dictionary_snapshot_entities_slug_nonempty_chk
    CHECK (length(trim(slug)) > 0),
  CONSTRAINT dictionary_snapshot_entities_normalized_address_nonempty_chk
    CHECK (normalized_address IS NULL OR length(trim(normalized_address)) > 0),
  CONSTRAINT dictionary_snapshot_entities_lat_chk
    CHECK (lat IS NULL OR (lat >= -90 AND lat <= 90)),
  CONSTRAINT dictionary_snapshot_entities_lng_chk
    CHECK (lng IS NULL OR (lng >= -180 AND lng <= 180)),
  CONSTRAINT dictionary_snapshot_entities_lat_lng_pair_chk
    CHECK (
      (lat IS NULL AND lng IS NULL)
      OR (lat IS NOT NULL AND lng IS NOT NULL)
    ),
  CONSTRAINT dictionary_snapshot_entities_spatial_confidence_chk
    CHECK (spatial_confidence IS NULL OR (spatial_confidence >= 0 AND spatial_confidence <= 1)),
  CONSTRAINT dictionary_snapshot_entities_spatial_requires_jurisdiction_chk
    CHECK (
      (
        lat IS NULL
        AND lng IS NULL
        AND spatial_confidence IS NULL
      )
      OR primary_jurisdiction_id IS NOT NULL
    ),
  CONSTRAINT dictionary_snapshot_entities_attributes_object_chk
    CHECK (jsonb_typeof(attributes) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_entities_snapshot
  ON dictionary.dictionary_snapshot_entities (snapshot_id, canonical_name);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_entities_canonical_record
  ON dictionary.dictionary_snapshot_entities (canonical_record_id, snapshot_id);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_snapshot_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_id UUID NOT NULL REFERENCES dictionary.dictionary_snapshots(id),
  canonical_record_id UUID NOT NULL,
  entity_id UUID NOT NULL,
  alias TEXT NOT NULL,
  alias_type TEXT NOT NULL,
  status dictionary.canonical_record_status NOT NULL,
  effective_start_at TIMESTAMPTZ,
  effective_end_at TIMESTAMPTZ,
  source_count INTEGER NOT NULL,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,

  CONSTRAINT dictionary_snapshot_aliases_record_unique_chk
    UNIQUE (snapshot_id, canonical_record_id),
  CONSTRAINT dictionary_snapshot_aliases_alias_nonempty_chk
    CHECK (length(trim(alias)) > 0),
  CONSTRAINT dictionary_snapshot_aliases_type_nonempty_chk
    CHECK (length(trim(alias_type)) > 0),
  CONSTRAINT dictionary_snapshot_aliases_source_count_nonnegative_chk
    CHECK (source_count >= 0),
  CONSTRAINT dictionary_snapshot_aliases_effective_window_chk
    CHECK (
      effective_end_at IS NULL
      OR effective_start_at IS NULL
      OR effective_end_at >= effective_start_at
    )
);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_aliases_snapshot
  ON dictionary.dictionary_snapshot_aliases (snapshot_id, alias);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_aliases_entity_snapshot
  ON dictionary.dictionary_snapshot_aliases (entity_id, snapshot_id);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_snapshot_roles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_id UUID NOT NULL REFERENCES dictionary.dictionary_snapshots(id),
  canonical_record_id UUID NOT NULL,
  role_name TEXT NOT NULL,
  role_type TEXT NOT NULL,
  jurisdiction_id UUID,
  status dictionary.canonical_record_status NOT NULL,
  notes TEXT,
  term_pattern JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,

  CONSTRAINT dictionary_snapshot_roles_record_unique_chk
    UNIQUE (snapshot_id, canonical_record_id),
  CONSTRAINT dictionary_snapshot_roles_name_nonempty_chk
    CHECK (length(trim(role_name)) > 0),
  CONSTRAINT dictionary_snapshot_roles_type_nonempty_chk
    CHECK (length(trim(role_type)) > 0),
  CONSTRAINT dictionary_snapshot_roles_term_pattern_object_chk
    CHECK (jsonb_typeof(term_pattern) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_roles_snapshot
  ON dictionary.dictionary_snapshot_roles (snapshot_id, role_name);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_roles_canonical_record
  ON dictionary.dictionary_snapshot_roles (canonical_record_id, snapshot_id);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_snapshot_jurisdictions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_id UUID NOT NULL REFERENCES dictionary.dictionary_snapshots(id),
  canonical_record_id UUID NOT NULL,
  name TEXT NOT NULL,
  jurisdiction_type TEXT NOT NULL,
  parent_jurisdiction_id UUID,
  centroid_lat DOUBLE PRECISION,
  centroid_lng DOUBLE PRECISION,
  bbox JSONB,
  geojson JSONB,
  status dictionary.canonical_record_status NOT NULL,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,

  CONSTRAINT dictionary_snapshot_jurisdictions_record_unique_chk
    UNIQUE (snapshot_id, canonical_record_id),
  CONSTRAINT dictionary_snapshot_jurisdictions_name_nonempty_chk
    CHECK (length(trim(name)) > 0),
  CONSTRAINT dictionary_snapshot_jurisdictions_type_nonempty_chk
    CHECK (length(trim(jurisdiction_type)) > 0),
  CONSTRAINT dictionary_snapshot_jurisdictions_centroid_lat_chk
    CHECK (centroid_lat IS NULL OR (centroid_lat >= -90 AND centroid_lat <= 90)),
  CONSTRAINT dictionary_snapshot_jurisdictions_centroid_lng_chk
    CHECK (centroid_lng IS NULL OR (centroid_lng >= -180 AND centroid_lng <= 180)),
  CONSTRAINT dictionary_snapshot_jurisdictions_centroid_pair_chk
    CHECK (
      (centroid_lat IS NULL AND centroid_lng IS NULL)
      OR (centroid_lat IS NOT NULL AND centroid_lng IS NOT NULL)
    ),
  CONSTRAINT dictionary_snapshot_jurisdictions_bbox_type_chk
    CHECK (bbox IS NULL OR jsonb_typeof(bbox) = 'object'),
  CONSTRAINT dictionary_snapshot_jurisdictions_geojson_type_chk
    CHECK (geojson IS NULL OR jsonb_typeof(geojson) = 'object'),
  CONSTRAINT dictionary_snapshot_jurisdictions_parent_not_self_chk
    CHECK (parent_jurisdiction_id IS NULL OR parent_jurisdiction_id <> canonical_record_id)
);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_jurisdictions_snapshot
  ON dictionary.dictionary_snapshot_jurisdictions (snapshot_id, name);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_jurisdictions_canonical_record
  ON dictionary.dictionary_snapshot_jurisdictions (canonical_record_id, snapshot_id);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_snapshot_assertions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_id UUID NOT NULL REFERENCES dictionary.dictionary_snapshots(id),
  canonical_record_id UUID NOT NULL,
  assertion_type TEXT NOT NULL,
  subject_entity_id UUID NOT NULL,
  object_entity_id UUID,
  role_id UUID,
  effective_start_at TIMESTAMPTZ,
  effective_end_at TIMESTAMPTZ,
  term_end_at TIMESTAMPTZ,
  observed_at TIMESTAMPTZ,
  last_verified_at TIMESTAMPTZ,
  freshness_sla_days INTEGER,
  next_election_at TIMESTAMPTZ,
  next_review_at TIMESTAMPTZ,
  validity_status dictionary.assertion_validity_status NOT NULL,
  review_status dictionary.assertion_review_status NOT NULL,
  assertion_confidence DOUBLE PRECISION,
  supersedes_assertion_id UUID,
  superseded_by_assertion_id UUID,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,

  CONSTRAINT dictionary_snapshot_assertions_record_unique_chk
    UNIQUE (snapshot_id, canonical_record_id),
  CONSTRAINT dictionary_snapshot_assertions_type_nonempty_chk
    CHECK (length(trim(assertion_type)) > 0),
  CONSTRAINT dictionary_snapshot_assertions_target_present_chk
    CHECK (object_entity_id IS NOT NULL OR role_id IS NOT NULL),
  CONSTRAINT dictionary_snapshot_assertions_subject_object_distinct_chk
    CHECK (object_entity_id IS NULL OR object_entity_id <> subject_entity_id),
  CONSTRAINT dictionary_snapshot_assertions_effective_window_chk
    CHECK (
      effective_end_at IS NULL
      OR effective_start_at IS NULL
      OR effective_end_at >= effective_start_at
    ),
  CONSTRAINT dictionary_snapshot_assertions_term_end_window_chk
    CHECK (
      term_end_at IS NULL
      OR effective_start_at IS NULL
      OR term_end_at >= effective_start_at
    ),
  CONSTRAINT dictionary_snapshot_assertions_freshness_sla_days_chk
    CHECK (freshness_sla_days IS NULL OR freshness_sla_days > 0),
  CONSTRAINT dictionary_snapshot_assertions_confidence_chk
    CHECK (assertion_confidence IS NULL OR (assertion_confidence >= 0 AND assertion_confidence <= 1)),
  CONSTRAINT dictionary_snapshot_assertions_supersedes_not_self_chk
    CHECK (supersedes_assertion_id IS NULL OR supersedes_assertion_id <> canonical_record_id),
  CONSTRAINT dictionary_snapshot_assertions_superseded_by_not_self_chk
    CHECK (superseded_by_assertion_id IS NULL OR superseded_by_assertion_id <> canonical_record_id),
  CONSTRAINT dictionary_snapshot_assertions_supersession_pair_distinct_chk
    CHECK (
      supersedes_assertion_id IS NULL
      OR superseded_by_assertion_id IS NULL
      OR supersedes_assertion_id <> superseded_by_assertion_id
    )
);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_assertions_snapshot
  ON dictionary.dictionary_snapshot_assertions (snapshot_id, assertion_type);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_assertions_subject_snapshot
  ON dictionary.dictionary_snapshot_assertions (subject_entity_id, snapshot_id);

CREATE OR REPLACE FUNCTION dictionary.enforce_snapshot_payload_references()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_TABLE_NAME = 'dictionary_snapshot_entities' THEN
    IF NEW.primary_jurisdiction_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM dictionary.dictionary_snapshot_jurisdictions sj
         WHERE sj.snapshot_id = NEW.snapshot_id
           AND sj.canonical_record_id = NEW.primary_jurisdiction_id
       )
    THEN
      RAISE EXCEPTION 'snapshot entity primary_jurisdiction_id must reference a jurisdiction payload row in the same snapshot';
    END IF;

    RETURN NEW;
  END IF;

  IF TG_TABLE_NAME = 'dictionary_snapshot_aliases' THEN
    IF NOT EXISTS (
      SELECT 1
      FROM dictionary.dictionary_snapshot_entities se
      WHERE se.snapshot_id = NEW.snapshot_id
        AND se.canonical_record_id = NEW.entity_id
    ) THEN
      RAISE EXCEPTION 'snapshot alias entity_id must reference an entity payload row in the same snapshot';
    END IF;

    RETURN NEW;
  END IF;

  IF TG_TABLE_NAME = 'dictionary_snapshot_roles' THEN
    IF NEW.jurisdiction_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM dictionary.dictionary_snapshot_jurisdictions sj
         WHERE sj.snapshot_id = NEW.snapshot_id
           AND sj.canonical_record_id = NEW.jurisdiction_id
       )
    THEN
      RAISE EXCEPTION 'snapshot role jurisdiction_id must reference a jurisdiction payload row in the same snapshot';
    END IF;

    RETURN NEW;
  END IF;

  IF TG_TABLE_NAME = 'dictionary_snapshot_jurisdictions' THEN
    IF NEW.parent_jurisdiction_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM dictionary.dictionary_snapshot_jurisdictions sj
         WHERE sj.snapshot_id = NEW.snapshot_id
           AND sj.canonical_record_id = NEW.parent_jurisdiction_id
       )
    THEN
      RAISE EXCEPTION 'snapshot jurisdiction parent_jurisdiction_id must reference a jurisdiction payload row in the same snapshot';
    END IF;

    RETURN NEW;
  END IF;

  IF TG_TABLE_NAME = 'dictionary_snapshot_assertions' THEN
    IF NOT EXISTS (
      SELECT 1
      FROM dictionary.dictionary_snapshot_entities se
      WHERE se.snapshot_id = NEW.snapshot_id
        AND se.canonical_record_id = NEW.subject_entity_id
    ) THEN
      RAISE EXCEPTION 'snapshot assertion subject_entity_id must reference an entity payload row in the same snapshot';
    END IF;

    IF NEW.object_entity_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM dictionary.dictionary_snapshot_entities se
         WHERE se.snapshot_id = NEW.snapshot_id
           AND se.canonical_record_id = NEW.object_entity_id
       )
    THEN
      RAISE EXCEPTION 'snapshot assertion object_entity_id must reference an entity payload row in the same snapshot';
    END IF;

    IF NEW.role_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM dictionary.dictionary_snapshot_roles sr
         WHERE sr.snapshot_id = NEW.snapshot_id
           AND sr.canonical_record_id = NEW.role_id
       )
    THEN
      RAISE EXCEPTION 'snapshot assertion role_id must reference a role payload row in the same snapshot';
    END IF;

    IF NEW.supersedes_assertion_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM dictionary.dictionary_snapshot_assertions sa
         WHERE sa.snapshot_id = NEW.snapshot_id
           AND sa.canonical_record_id = NEW.supersedes_assertion_id
       )
    THEN
      RAISE EXCEPTION 'snapshot assertion supersedes_assertion_id must reference an assertion payload row in the same snapshot';
    END IF;

    IF NEW.superseded_by_assertion_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM dictionary.dictionary_snapshot_assertions sa
         WHERE sa.snapshot_id = NEW.snapshot_id
           AND sa.canonical_record_id = NEW.superseded_by_assertion_id
       )
    THEN
      RAISE EXCEPTION 'snapshot assertion superseded_by_assertion_id must reference an assertion payload row in the same snapshot';
    END IF;

    RETURN NEW;
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.guard_snapshot_payload_rows()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  target_snapshot_id UUID;
  snapshot_status dictionary.snapshot_status;
BEGIN
  target_snapshot_id := COALESCE(NEW.snapshot_id, OLD.snapshot_id);

  SELECT status
  INTO snapshot_status
  FROM dictionary.dictionary_snapshots
  WHERE id = target_snapshot_id;

  IF snapshot_status IS DISTINCT FROM 'building' THEN
    RAISE EXCEPTION 'snapshot payload rows are mutable only while the snapshot is building';
  END IF;

  RETURN COALESCE(NEW, OLD);
END;
$$;

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_entities_guard
  ON dictionary.dictionary_snapshot_entities;

CREATE TRIGGER trg_dictionary_snapshot_entities_guard
  BEFORE INSERT OR UPDATE OR DELETE ON dictionary.dictionary_snapshot_entities
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.guard_snapshot_payload_rows();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_entities_references
  ON dictionary.dictionary_snapshot_entities;

CREATE CONSTRAINT TRIGGER trg_dictionary_snapshot_entities_references
  AFTER INSERT OR UPDATE ON dictionary.dictionary_snapshot_entities
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_snapshot_payload_references();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_aliases_guard
  ON dictionary.dictionary_snapshot_aliases;

CREATE TRIGGER trg_dictionary_snapshot_aliases_guard
  BEFORE INSERT OR UPDATE OR DELETE ON dictionary.dictionary_snapshot_aliases
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.guard_snapshot_payload_rows();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_aliases_references
  ON dictionary.dictionary_snapshot_aliases;

CREATE CONSTRAINT TRIGGER trg_dictionary_snapshot_aliases_references
  AFTER INSERT OR UPDATE ON dictionary.dictionary_snapshot_aliases
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_snapshot_payload_references();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_roles_guard
  ON dictionary.dictionary_snapshot_roles;

CREATE TRIGGER trg_dictionary_snapshot_roles_guard
  BEFORE INSERT OR UPDATE OR DELETE ON dictionary.dictionary_snapshot_roles
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.guard_snapshot_payload_rows();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_roles_references
  ON dictionary.dictionary_snapshot_roles;

CREATE CONSTRAINT TRIGGER trg_dictionary_snapshot_roles_references
  AFTER INSERT OR UPDATE ON dictionary.dictionary_snapshot_roles
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_snapshot_payload_references();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_jurisdictions_guard
  ON dictionary.dictionary_snapshot_jurisdictions;

CREATE TRIGGER trg_dictionary_snapshot_jurisdictions_guard
  BEFORE INSERT OR UPDATE OR DELETE ON dictionary.dictionary_snapshot_jurisdictions
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.guard_snapshot_payload_rows();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_jurisdictions_references
  ON dictionary.dictionary_snapshot_jurisdictions;

CREATE CONSTRAINT TRIGGER trg_dictionary_snapshot_jurisdictions_references
  AFTER INSERT OR UPDATE ON dictionary.dictionary_snapshot_jurisdictions
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_snapshot_payload_references();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_assertions_guard
  ON dictionary.dictionary_snapshot_assertions;

CREATE TRIGGER trg_dictionary_snapshot_assertions_guard
  BEFORE INSERT OR UPDATE OR DELETE ON dictionary.dictionary_snapshot_assertions
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.guard_snapshot_payload_rows();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_assertions_references
  ON dictionary.dictionary_snapshot_assertions;

CREATE CONSTRAINT TRIGGER trg_dictionary_snapshot_assertions_references
  AFTER INSERT OR UPDATE ON dictionary.dictionary_snapshot_assertions
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_snapshot_payload_references();

CREATE OR REPLACE VIEW dictionary.phase_e_latest_first_pass_validations AS
SELECT DISTINCT ON (vr.merge_proposal_id)
  vr.id,
  vr.substrate_run_id,
  vr.merge_proposal_id,
  vr.outcome,
  vr.validator_name,
  vr.details,
  vr.created_at
FROM dictionary.dictionary_validation_results vr
WHERE vr.validator_name = 'phase_d_first_pass_validator_v1'
ORDER BY vr.merge_proposal_id, vr.created_at DESC, vr.id DESC;

CREATE OR REPLACE VIEW dictionary.phase_e_promotable_merge_proposals AS
SELECT
  mp.id AS merge_proposal_id,
  mp.substrate_run_id,
  lv.substrate_run_id AS validation_substrate_run_id,
  mp.extraction_candidate_id,
  mp.proposal_key,
  mp.proposal_type,
  mp.target_record_type,
  mp.target_record_id,
  mp.proposal_confidence,
  mp.rationale,
  mp.proposal_payload,
  lv.id AS validation_result_id,
  lv.created_at AS validation_created_at,
  ec.root_source_id,
  ec.crawl_artifact_id,
  ec.extraction_version,
  ec.candidate_type,
  ec.candidate_payload
FROM dictionary.dictionary_merge_proposals mp
JOIN dictionary.phase_e_latest_first_pass_validations lv
  ON lv.merge_proposal_id = mp.id
 AND lv.outcome = 'approved'
JOIN dictionary.dictionary_extraction_candidates ec
  ON ec.id = mp.extraction_candidate_id
LEFT JOIN dictionary.dictionary_promotion_results pr
  ON pr.merge_proposal_id = mp.id
WHERE pr.id IS NULL;

CREATE OR REPLACE VIEW dictionary.published_entities AS
SELECT
  se.canonical_record_id AS id,
  se.entity_type,
  se.canonical_name,
  se.slug,
  se.primary_jurisdiction_id,
  se.normalized_address,
  se.lat,
  se.lng,
  se.spatial_confidence,
  se.status,
  se.description,
  se.notes,
  se.attributes,
  se.last_verified_at,
  se.created_at,
  se.updated_at
FROM dictionary.dictionary_snapshot_entities se
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = se.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = se.snapshot_id
 AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_aliases AS
SELECT
  sa.canonical_record_id AS id,
  sa.entity_id,
  sa.alias,
  sa.alias_type,
  sa.status,
  sa.effective_start_at,
  sa.effective_end_at,
  sa.source_count,
  sa.last_verified_at,
  sa.created_at,
  sa.updated_at
FROM dictionary.dictionary_snapshot_aliases sa
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sa.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sa.snapshot_id
 AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_roles AS
SELECT
  sr.canonical_record_id AS id,
  sr.role_name,
  sr.role_type,
  sr.jurisdiction_id,
  sr.status,
  sr.notes,
  sr.term_pattern,
  sr.last_verified_at,
  sr.created_at,
  sr.updated_at
FROM dictionary.dictionary_snapshot_roles sr
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sr.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sr.snapshot_id
 AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_jurisdictions AS
SELECT
  sj.canonical_record_id AS id,
  sj.name,
  sj.jurisdiction_type,
  sj.parent_jurisdiction_id,
  sj.centroid_lat,
  sj.centroid_lng,
  sj.bbox,
  sj.geojson,
  sj.status,
  sj.last_verified_at,
  sj.created_at,
  sj.updated_at
FROM dictionary.dictionary_snapshot_jurisdictions sj
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sj.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sj.snapshot_id
 AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_assertions AS
SELECT
  sa.canonical_record_id AS id,
  sa.assertion_type,
  sa.subject_entity_id,
  sa.object_entity_id,
  sa.role_id,
  sa.effective_start_at,
  sa.effective_end_at,
  sa.term_end_at,
  sa.observed_at,
  sa.last_verified_at,
  sa.freshness_sla_days,
  sa.next_election_at,
  sa.next_review_at,
  sa.validity_status,
  sa.review_status,
  sa.assertion_confidence,
  sa.supersedes_assertion_id,
  sa.superseded_by_assertion_id,
  sa.snapshot_id,
  sa.notes,
  sa.created_at,
  sa.updated_at
FROM dictionary.dictionary_snapshot_assertions sa
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sa.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sa.snapshot_id
 AND s.status = 'published';

COMMENT ON TABLE dictionary.dictionary_promotion_results IS
  'Idempotent Phase E promotion ledger. Each merge proposal may promote at most once, and only from an approved validation result.';

COMMENT ON FUNCTION dictionary.enforce_promotion_result_references() IS
  'Ensures promotion results point at the same merge proposal as their validation row, require approved validation, and reference real canonical records when present.';

COMMENT ON TABLE dictionary.dictionary_snapshot_entities IS
  'Immutable entity payload rows built from canonical head at publish time. Newsroom reads must resolve through these snapshot artifacts, not live canonical head rows.';

COMMENT ON TABLE dictionary.dictionary_snapshot_aliases IS
  'Immutable alias payload rows built from canonical head at publish time.';

COMMENT ON TABLE dictionary.dictionary_snapshot_roles IS
  'Immutable role payload rows built from canonical head at publish time.';

COMMENT ON TABLE dictionary.dictionary_snapshot_jurisdictions IS
  'Immutable jurisdiction payload rows built from canonical head at publish time.';

COMMENT ON TABLE dictionary.dictionary_snapshot_assertions IS
  'Immutable assertion payload rows built from canonical head at publish time.';

COMMENT ON FUNCTION dictionary.guard_snapshot_payload_rows() IS
  'Prevents snapshot payload mutation after publish by allowing writes only while the target snapshot remains in building state.';

COMMENT ON FUNCTION dictionary.enforce_snapshot_payload_references() IS
  'Enforces same-snapshot referential integrity across immutable snapshot payload rows so published newsroom state cannot contain dangling entity, role, jurisdiction, or assertion references.';

COMMENT ON VIEW dictionary.phase_e_latest_first_pass_validations IS
  'Deterministic latest validation row per merge proposal for the Phase D first-pass validator, ordered by created_at then id descending.';

COMMENT ON VIEW dictionary.phase_e_promotable_merge_proposals IS
  'Phase E first-pass promotable proposals: latest first-pass validation outcome approved, with no existing promotion ledger row.';

COMMENT ON VIEW dictionary.published_entities IS
  'Newsroom-readable entity payloads scoped to the active published snapshot artifact only.';

COMMENT ON VIEW dictionary.published_aliases IS
  'Newsroom-readable alias payloads scoped to the active published snapshot artifact only.';

COMMENT ON VIEW dictionary.published_roles IS
  'Newsroom-readable role payloads scoped to the active published snapshot artifact only.';

COMMENT ON VIEW dictionary.published_jurisdictions IS
  'Newsroom-readable jurisdiction payloads scoped to the active published snapshot artifact only.';

COMMENT ON VIEW dictionary.published_assertions IS
  'Newsroom-readable assertion payloads scoped to the active published snapshot artifact only.';

COMMENT ON TABLE dictionary.dictionary_assertions IS
  'Time-bounded canonical facts. Validity status answers truth-in-time; review status answers operational trust; snapshot_id records latest publication lineage while newsroom reads resolve through immutable snapshot payload rows.';
