-- Phase A / Chunk 5: staging + runtime foundation
-- Safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS dictionary.dictionary_root_sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_name TEXT NOT NULL,
  source_type TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  root_url TEXT NOT NULL,
  trust_tier dictionary.source_trust_tier NOT NULL,
  supported_entity_classes JSONB NOT NULL DEFAULT '[]'::jsonb,
  domain_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  url_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  crawl_cadence_days INTEGER,
  freshness_sla_days INTEGER,
  failure_threshold INTEGER NOT NULL DEFAULT 3,
  enabled BOOLEAN NOT NULL DEFAULT true,
  notes TEXT,
  last_crawled_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_root_sources_name_nonempty_chk
    CHECK (length(trim(source_name)) > 0),
  CONSTRAINT dictionary_root_sources_type_nonempty_chk
    CHECK (length(trim(source_type)) > 0),
  CONSTRAINT dictionary_root_sources_domain_nonempty_chk
    CHECK (length(trim(source_domain)) > 0),
  CONSTRAINT dictionary_root_sources_root_url_nonempty_chk
    CHECK (length(trim(root_url)) > 0),
  CONSTRAINT dictionary_root_sources_supported_entity_classes_array_chk
    CHECK (jsonb_typeof(supported_entity_classes) = 'array'),
  CONSTRAINT dictionary_root_sources_domain_metadata_object_chk
    CHECK (jsonb_typeof(domain_metadata) = 'object'),
  CONSTRAINT dictionary_root_sources_url_metadata_object_chk
    CHECK (jsonb_typeof(url_metadata) = 'object'),
  CONSTRAINT dictionary_root_sources_crawl_cadence_days_chk
    CHECK (crawl_cadence_days IS NULL OR crawl_cadence_days > 0),
  CONSTRAINT dictionary_root_sources_freshness_sla_days_chk
    CHECK (freshness_sla_days IS NULL OR freshness_sla_days > 0),
  CONSTRAINT dictionary_root_sources_failure_threshold_chk
    CHECK (failure_threshold > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_root_sources_root_url
  ON dictionary.dictionary_root_sources (lower(root_url));

CREATE INDEX IF NOT EXISTS idx_dictionary_root_sources_enabled_trust
  ON dictionary.dictionary_root_sources (enabled, trust_tier, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_root_sources_domain
  ON dictionary.dictionary_root_sources (lower(source_domain), enabled, updated_at DESC);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_pipeline_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  parent_run_id UUID,
  stage_name TEXT NOT NULL,
  trigger_type TEXT NOT NULL,
  status dictionary.pipeline_run_status NOT NULL DEFAULT 'queued',
  root_source_id UUID,
  crawl_artifact_id UUID,
  snapshot_id UUID,
  attempt INTEGER NOT NULL DEFAULT 1,
  input_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  output_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_pipeline_runs_stage_name_nonempty_chk
    CHECK (length(trim(stage_name)) > 0),
  CONSTRAINT dictionary_pipeline_runs_trigger_type_nonempty_chk
    CHECK (length(trim(trigger_type)) > 0),
  CONSTRAINT dictionary_pipeline_runs_attempt_positive_chk
    CHECK (attempt > 0),
  CONSTRAINT dictionary_pipeline_runs_input_payload_object_chk
    CHECK (jsonb_typeof(input_payload) = 'object'),
  CONSTRAINT dictionary_pipeline_runs_output_payload_object_chk
    CHECK (jsonb_typeof(output_payload) = 'object'),
  CONSTRAINT dictionary_pipeline_runs_metrics_object_chk
    CHECK (jsonb_typeof(metrics) = 'object'),
  CONSTRAINT dictionary_pipeline_runs_error_payload_object_chk
    CHECK (jsonb_typeof(error_payload) = 'object'),
  CONSTRAINT dictionary_pipeline_runs_end_after_start_chk
    CHECK (ended_at IS NULL OR started_at IS NULL OR ended_at >= started_at)
);

CREATE INDEX IF NOT EXISTS idx_dictionary_pipeline_runs_status_stage
  ON dictionary.dictionary_pipeline_runs (status, lower(stage_name), created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_pipeline_runs_root_source
  ON dictionary.dictionary_pipeline_runs (root_source_id, created_at DESC)
  WHERE root_source_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS dictionary.dictionary_crawl_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  root_source_id UUID NOT NULL REFERENCES dictionary.dictionary_root_sources(id),
  substrate_run_id UUID NOT NULL REFERENCES dictionary.dictionary_pipeline_runs(id),
  prior_artifact_id UUID REFERENCES dictionary.dictionary_crawl_artifacts(id),
  source_url TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  http_status INTEGER,
  content_type TEXT,
  raw_html TEXT,
  extracted_text TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_crawl_artifacts_source_url_nonempty_chk
    CHECK (length(trim(source_url)) > 0),
  CONSTRAINT dictionary_crawl_artifacts_source_domain_nonempty_chk
    CHECK (length(trim(source_domain)) > 0),
  CONSTRAINT dictionary_crawl_artifacts_content_hash_nonempty_chk
    CHECK (length(trim(content_hash)) > 0),
  CONSTRAINT dictionary_crawl_artifacts_http_status_chk
    CHECK (http_status IS NULL OR (http_status >= 100 AND http_status <= 599)),
  CONSTRAINT dictionary_crawl_artifacts_metadata_object_chk
    CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT dictionary_crawl_artifacts_prior_not_self_chk
    CHECK (prior_artifact_id IS NULL OR prior_artifact_id <> id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_crawl_artifacts_run_url
  ON dictionary.dictionary_crawl_artifacts (substrate_run_id, lower(source_url));

CREATE INDEX IF NOT EXISTS idx_dictionary_crawl_artifacts_root_fetched
  ON dictionary.dictionary_crawl_artifacts (root_source_id, fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_crawl_artifacts_hash
  ON dictionary.dictionary_crawl_artifacts (content_hash, fetched_at DESC);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_extraction_candidates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  substrate_run_id UUID NOT NULL REFERENCES dictionary.dictionary_pipeline_runs(id),
  root_source_id UUID NOT NULL REFERENCES dictionary.dictionary_root_sources(id),
  crawl_artifact_id UUID NOT NULL REFERENCES dictionary.dictionary_crawl_artifacts(id),
  candidate_type TEXT NOT NULL,
  status dictionary.extraction_candidate_status NOT NULL DEFAULT 'pending',
  candidate_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_snippets JSONB NOT NULL DEFAULT '[]'::jsonb,
  diagnostics JSONB NOT NULL DEFAULT '[]'::jsonb,
  extraction_version TEXT NOT NULL,
  rejection_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_extraction_candidates_type_nonempty_chk
    CHECK (length(trim(candidate_type)) > 0),
  CONSTRAINT dictionary_extraction_candidates_type_vocab_chk
    CHECK (candidate_type IN ('entity', 'alias', 'role', 'assertion', 'jurisdiction', 'diagnostic')),
  CONSTRAINT dictionary_extraction_candidates_payload_object_chk
    CHECK (jsonb_typeof(candidate_payload) = 'object'),
  CONSTRAINT dictionary_extraction_candidates_evidence_array_chk
    CHECK (jsonb_typeof(evidence_snippets) = 'array'),
  CONSTRAINT dictionary_extraction_candidates_diagnostics_array_chk
    CHECK (jsonb_typeof(diagnostics) = 'array'),
  CONSTRAINT dictionary_extraction_candidates_extraction_version_nonempty_chk
    CHECK (length(trim(extraction_version)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_dictionary_extraction_candidates_artifact_status
  ON dictionary.dictionary_extraction_candidates (crawl_artifact_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_extraction_candidates_run_type
  ON dictionary.dictionary_extraction_candidates (substrate_run_id, lower(candidate_type), created_at DESC);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_merge_proposals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  substrate_run_id UUID NOT NULL REFERENCES dictionary.dictionary_pipeline_runs(id),
  extraction_candidate_id UUID NOT NULL REFERENCES dictionary.dictionary_extraction_candidates(id),
  proposal_type dictionary.merge_proposal_type NOT NULL,
  target_record_type TEXT,
  target_record_id UUID,
  proposal_confidence DOUBLE PRECISION,
  rationale TEXT,
  proposal_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_merge_proposals_target_record_type_chk
    CHECK (
      target_record_type IS NULL
      OR target_record_type IN ('entity', 'alias', 'role', 'assertion', 'jurisdiction')
    ),
  CONSTRAINT dictionary_merge_proposals_target_pair_chk
    CHECK (
      (target_record_type IS NULL AND target_record_id IS NULL)
      OR (target_record_type IS NOT NULL AND target_record_id IS NOT NULL)
    ),
  CONSTRAINT dictionary_merge_proposals_confidence_chk
    CHECK (proposal_confidence IS NULL OR (proposal_confidence >= 0 AND proposal_confidence <= 1)),
  CONSTRAINT dictionary_merge_proposals_payload_object_chk
    CHECK (jsonb_typeof(proposal_payload) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_dictionary_merge_proposals_candidate
  ON dictionary.dictionary_merge_proposals (extraction_candidate_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_merge_proposals_target
  ON dictionary.dictionary_merge_proposals (target_record_type, target_record_id, created_at DESC)
  WHERE target_record_type IS NOT NULL;

CREATE TABLE IF NOT EXISTS dictionary.dictionary_validation_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  substrate_run_id UUID NOT NULL REFERENCES dictionary.dictionary_pipeline_runs(id),
  merge_proposal_id UUID NOT NULL REFERENCES dictionary.dictionary_merge_proposals(id),
  outcome dictionary.validation_outcome NOT NULL,
  validator_name TEXT,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_validation_results_validator_name_nonempty_chk
    CHECK (validator_name IS NULL OR length(trim(validator_name)) > 0),
  CONSTRAINT dictionary_validation_results_details_object_chk
    CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_dictionary_validation_results_proposal
  ON dictionary.dictionary_validation_results (merge_proposal_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_validation_results_outcome
  ON dictionary.dictionary_validation_results (outcome, created_at DESC);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_review_queue (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  item_type dictionary.review_queue_item_type NOT NULL,
  severity dictionary.review_queue_severity NOT NULL,
  root_source_id UUID REFERENCES dictionary.dictionary_root_sources(id),
  crawl_artifact_id UUID REFERENCES dictionary.dictionary_crawl_artifacts(id),
  pipeline_run_id UUID REFERENCES dictionary.dictionary_pipeline_runs(id),
  affected_record_type TEXT,
  affected_record_id UUID,
  retry_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  suggested_action TEXT,
  first_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_review_queue_affected_record_type_chk
    CHECK (
      affected_record_type IS NULL
      OR affected_record_type IN (
        'root_source',
        'crawl_artifact',
        'entity',
        'alias',
        'role',
        'assertion',
        'jurisdiction',
        'extraction_candidate',
        'merge_proposal',
        'validation_result',
        'pipeline_run',
        'snapshot'
      )
    ),
  CONSTRAINT dictionary_review_queue_affected_record_pair_chk
    CHECK (
      (affected_record_type IS NULL AND affected_record_id IS NULL)
      OR (affected_record_type IS NOT NULL AND affected_record_id IS NOT NULL)
    ),
  CONSTRAINT dictionary_review_queue_retry_count_nonnegative_chk
    CHECK (retry_count >= 0),
  CONSTRAINT dictionary_review_queue_failure_window_chk
    CHECK (last_failed_at >= first_failed_at),
  CONSTRAINT dictionary_review_queue_resolution_window_chk
    CHECK (resolved_at IS NULL OR resolved_at >= last_failed_at)
);

CREATE INDEX IF NOT EXISTS idx_dictionary_review_queue_item_open
  ON dictionary.dictionary_review_queue (severity, item_type, last_failed_at DESC)
  WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_dictionary_review_queue_root_source
  ON dictionary.dictionary_review_queue (root_source_id, last_failed_at DESC)
  WHERE root_source_id IS NOT NULL;

CREATE OR REPLACE FUNCTION dictionary.guard_immutable_crawl_artifacts()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'crawl artifacts are immutable after insert';
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.enforce_enabled_root_source()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  target_root_source_id UUID;
  source_enabled BOOLEAN;
BEGIN
  target_root_source_id := NEW.root_source_id;

  IF target_root_source_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT enabled
  INTO source_enabled
  FROM dictionary.dictionary_root_sources
  WHERE id = target_root_source_id;

  IF source_enabled IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'only enabled root sources may seed substrate staging and runtime records';
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dictionary_crawl_artifacts_immutable
  ON dictionary.dictionary_crawl_artifacts;

CREATE TRIGGER trg_dictionary_crawl_artifacts_immutable
  BEFORE UPDATE OR DELETE ON dictionary.dictionary_crawl_artifacts
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.guard_immutable_crawl_artifacts();

DROP TRIGGER IF EXISTS trg_dictionary_pipeline_runs_root_source_enabled
  ON dictionary.dictionary_pipeline_runs;

CREATE TRIGGER trg_dictionary_pipeline_runs_root_source_enabled
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_pipeline_runs
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_enabled_root_source();

DROP TRIGGER IF EXISTS trg_dictionary_crawl_artifacts_root_source_enabled
  ON dictionary.dictionary_crawl_artifacts;

CREATE TRIGGER trg_dictionary_crawl_artifacts_root_source_enabled
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_crawl_artifacts
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_enabled_root_source();

DROP TRIGGER IF EXISTS trg_dictionary_extraction_candidates_root_source_enabled
  ON dictionary.dictionary_extraction_candidates;

CREATE TRIGGER trg_dictionary_extraction_candidates_root_source_enabled
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_extraction_candidates
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_enabled_root_source();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dictionary_pipeline_runs_parent_run_id_fkey'
      AND conrelid = 'dictionary.dictionary_pipeline_runs'::regclass
  ) THEN
    ALTER TABLE dictionary.dictionary_pipeline_runs
      ADD CONSTRAINT dictionary_pipeline_runs_parent_run_id_fkey
      FOREIGN KEY (parent_run_id)
      REFERENCES dictionary.dictionary_pipeline_runs(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dictionary_pipeline_runs_root_source_id_fkey'
      AND conrelid = 'dictionary.dictionary_pipeline_runs'::regclass
  ) THEN
    ALTER TABLE dictionary.dictionary_pipeline_runs
      ADD CONSTRAINT dictionary_pipeline_runs_root_source_id_fkey
      FOREIGN KEY (root_source_id)
      REFERENCES dictionary.dictionary_root_sources(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dictionary_pipeline_runs_crawl_artifact_id_fkey'
      AND conrelid = 'dictionary.dictionary_pipeline_runs'::regclass
  ) THEN
    ALTER TABLE dictionary.dictionary_pipeline_runs
      ADD CONSTRAINT dictionary_pipeline_runs_crawl_artifact_id_fkey
      FOREIGN KEY (crawl_artifact_id)
      REFERENCES dictionary.dictionary_crawl_artifacts(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dictionary_pipeline_runs_snapshot_id_fkey'
      AND conrelid = 'dictionary.dictionary_pipeline_runs'::regclass
  ) THEN
    ALTER TABLE dictionary.dictionary_pipeline_runs
      ADD CONSTRAINT dictionary_pipeline_runs_snapshot_id_fkey
      FOREIGN KEY (snapshot_id)
      REFERENCES dictionary.dictionary_snapshots(id);
  END IF;
END $$;

COMMENT ON TABLE dictionary.dictionary_root_sources IS
  'Approved substrate entry points and trust/freshness policy. Only registered root sources may seed substrate ingestion.';

COMMENT ON TABLE dictionary.dictionary_pipeline_runs IS
  'Stage-level run audit and lineage for substrate execution. This is operational state, not newsroom-readable dictionary state.';

COMMENT ON TABLE dictionary.dictionary_crawl_artifacts IS
  'Immutable raw fetch artifacts captured before extraction. These artifacts are staging inputs and never canonical newsroom state.';

COMMENT ON TABLE dictionary.dictionary_extraction_candidates IS
  'Staged extraction outputs and diagnostics. Candidates are never canonical until later validation and promotion phases.';

COMMENT ON TABLE dictionary.dictionary_merge_proposals IS
  'Explicit create/merge/supersede proposals emitted before validation and promotion.';

COMMENT ON TABLE dictionary.dictionary_validation_results IS
  'Validation outcomes for merge proposals. Approval here does not itself publish newsroom-visible state.';

COMMENT ON TABLE dictionary.dictionary_review_queue IS
  'Operational inbox for substrate failures, ambiguity, freshness issues, and promotion blocks.';
