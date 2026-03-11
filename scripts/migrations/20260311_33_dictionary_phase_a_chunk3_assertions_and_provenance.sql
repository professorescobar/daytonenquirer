-- Phase A / Chunk 3: assertions + provenance foundation
-- Safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS dictionary.dictionary_assertions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  assertion_type TEXT NOT NULL,
  subject_entity_id UUID NOT NULL REFERENCES dictionary.dictionary_entities(id),
  object_entity_id UUID REFERENCES dictionary.dictionary_entities(id),
  role_id UUID REFERENCES dictionary.dictionary_roles(id),
  effective_start_at TIMESTAMPTZ,
  effective_end_at TIMESTAMPTZ,
  term_end_at TIMESTAMPTZ,
  observed_at TIMESTAMPTZ,
  last_verified_at TIMESTAMPTZ,
  freshness_sla_days INTEGER,
  next_election_at TIMESTAMPTZ,
  next_review_at TIMESTAMPTZ,
  validity_status dictionary.assertion_validity_status NOT NULL DEFAULT 'unknown',
  review_status dictionary.assertion_review_status NOT NULL DEFAULT 'needs_review',
  assertion_confidence DOUBLE PRECISION,
  supersedes_assertion_id UUID REFERENCES dictionary.dictionary_assertions(id),
  superseded_by_assertion_id UUID REFERENCES dictionary.dictionary_assertions(id),
  snapshot_id UUID,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_assertions_type_nonempty_chk
    CHECK (length(trim(assertion_type)) > 0),
  CONSTRAINT dictionary_assertions_target_present_chk
    CHECK (object_entity_id IS NOT NULL OR role_id IS NOT NULL),
  CONSTRAINT dictionary_assertions_subject_object_distinct_chk
    CHECK (object_entity_id IS NULL OR object_entity_id <> subject_entity_id),
  CONSTRAINT dictionary_assertions_effective_window_chk
    CHECK (
      effective_end_at IS NULL
      OR effective_start_at IS NULL
      OR effective_end_at >= effective_start_at
    ),
  CONSTRAINT dictionary_assertions_term_end_window_chk
    CHECK (
      term_end_at IS NULL
      OR effective_start_at IS NULL
      OR term_end_at >= effective_start_at
    ),
  CONSTRAINT dictionary_assertions_freshness_sla_days_chk
    CHECK (freshness_sla_days IS NULL OR freshness_sla_days > 0),
  CONSTRAINT dictionary_assertions_confidence_chk
    CHECK (assertion_confidence IS NULL OR (assertion_confidence >= 0 AND assertion_confidence <= 1)),
  CONSTRAINT dictionary_assertions_supersedes_not_self_chk
    CHECK (supersedes_assertion_id IS NULL OR supersedes_assertion_id <> id),
  CONSTRAINT dictionary_assertions_superseded_by_not_self_chk
    CHECK (superseded_by_assertion_id IS NULL OR superseded_by_assertion_id <> id),
  CONSTRAINT dictionary_assertions_supersession_pair_distinct_chk
    CHECK (
      supersedes_assertion_id IS NULL
      OR superseded_by_assertion_id IS NULL
      OR supersedes_assertion_id <> superseded_by_assertion_id
    )
);

CREATE INDEX IF NOT EXISTS idx_dictionary_assertions_subject_type
  ON dictionary.dictionary_assertions (subject_entity_id, lower(assertion_type), updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_assertions_object_type
  ON dictionary.dictionary_assertions (object_entity_id, lower(assertion_type), updated_at DESC)
  WHERE object_entity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dictionary_assertions_role_validity
  ON dictionary.dictionary_assertions (role_id, validity_status, review_status, updated_at DESC)
  WHERE role_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dictionary_assertions_validity_review
  ON dictionary.dictionary_assertions (validity_status, review_status, last_verified_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_dictionary_assertions_snapshot_id
  ON dictionary.dictionary_assertions (snapshot_id)
  WHERE snapshot_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS dictionary.dictionary_provenance (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  record_type TEXT NOT NULL,
  record_id UUID NOT NULL,
  root_source_id UUID,
  crawl_artifact_id UUID,
  source_url TEXT,
  source_domain TEXT,
  trust_tier dictionary.source_trust_tier NOT NULL,
  observed_at TIMESTAMPTZ,
  captured_at TIMESTAMPTZ NOT NULL,
  substrate_run_id UUID NOT NULL,
  extraction_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_provenance_record_type_chk
    CHECK (record_type IN ('entity', 'alias', 'role', 'assertion', 'jurisdiction')),
  CONSTRAINT dictionary_provenance_source_url_nonempty_chk
    CHECK (source_url IS NULL OR length(trim(source_url)) > 0),
  CONSTRAINT dictionary_provenance_source_domain_nonempty_chk
    CHECK (source_domain IS NULL OR length(trim(source_domain)) > 0),
  CONSTRAINT dictionary_provenance_extraction_version_nonempty_chk
    CHECK (length(trim(extraction_version)) > 0),
  CONSTRAINT dictionary_provenance_source_locator_present_chk
    CHECK (
      root_source_id IS NOT NULL
      OR crawl_artifact_id IS NOT NULL
      OR source_url IS NOT NULL
    ),
  CONSTRAINT dictionary_provenance_source_domain_when_url_chk
    CHECK (source_url IS NULL OR source_domain IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_dictionary_provenance_record
  ON dictionary.dictionary_provenance (record_type, record_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_provenance_root_source
  ON dictionary.dictionary_provenance (root_source_id, captured_at DESC)
  WHERE root_source_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dictionary_provenance_crawl_artifact
  ON dictionary.dictionary_provenance (crawl_artifact_id, captured_at DESC)
  WHERE crawl_artifact_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dictionary_provenance_run
  ON dictionary.dictionary_provenance (substrate_run_id, captured_at DESC);

COMMENT ON TABLE dictionary.dictionary_assertions IS
  'Time-bounded canonical facts. Validity status answers truth-in-time; review status answers operational trust; snapshot_id is lineage only until snapshot publish mechanics are added.';

COMMENT ON TABLE dictionary.dictionary_provenance IS
  'Source linkage for promoted canonical records and assertions. Each promoted fact must be traceable to substrate run lineage and source provenance.';
