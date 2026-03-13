-- Phase D lineage + Phase E run-scope fix
-- Safe to run multiple times.

CREATE OR REPLACE VIEW dictionary.phase_e_latest_first_pass_validations AS
SELECT DISTINCT ON (vr.merge_proposal_id)
  vr.id,
  vr.substrate_run_id,
  vr.merge_proposal_id,
  vr.outcome,
  vr.validator_name,
  vr.details,
  vr.created_at,
  NULLIF(vr.details->>'phase_d_pipeline_run_id', '')::uuid AS phase_d_pipeline_run_id
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
  ec.candidate_payload,
  lv.phase_d_pipeline_run_id
FROM dictionary.dictionary_merge_proposals mp
JOIN dictionary.phase_e_latest_first_pass_validations lv
  ON lv.merge_proposal_id = mp.id
 AND lv.outcome = 'approved'
JOIN dictionary.dictionary_extraction_candidates ec
  ON ec.id = mp.extraction_candidate_id
LEFT JOIN dictionary.dictionary_promotion_results pr
  ON pr.merge_proposal_id = mp.id
WHERE pr.id IS NULL;

CREATE OR REPLACE FUNCTION dictionary.phase_e_promote_artifact_run(
  p_phase_e_run_id UUID,
  p_root_source_id UUID,
  p_crawl_artifact_id UUID,
  p_validation_substrate_run_id UUID
)
RETURNS TABLE (
  merge_proposal_id UUID,
  validation_result_id UUID,
  proposal_type dictionary.merge_proposal_type,
  promotion_outcome TEXT,
  created_record_type TEXT,
  created_record_id UUID,
  affected_record_type TEXT,
  affected_record_id UUID
)
LANGUAGE plpgsql
AS $$
DECLARE
  proposal_row RECORD;
BEGIN
  FOR proposal_row IN
    SELECT
      p.merge_proposal_id,
      p.validation_result_id
    FROM dictionary.phase_e_promotable_merge_proposals p
    JOIN dictionary.dictionary_merge_proposals mp
      ON mp.id = p.merge_proposal_id
    WHERE p.root_source_id = p_root_source_id
      AND p.crawl_artifact_id = p_crawl_artifact_id
      AND p.phase_d_pipeline_run_id = p_validation_substrate_run_id
    ORDER BY p.validation_created_at ASC, p.merge_proposal_id ASC
    FOR UPDATE OF mp
  LOOP
    RETURN QUERY
    SELECT *
    FROM dictionary.phase_e_promote_merge_proposal(
      p_phase_e_run_id,
      proposal_row.merge_proposal_id,
      proposal_row.validation_result_id
    );
  END LOOP;
END;
$$;

COMMENT ON FUNCTION dictionary.phase_e_promote_artifact_run(UUID, UUID, UUID, UUID) IS
  'Promotes the full Phase E artifact/run batch inside one statement transaction so canonical head mutation cannot partially commit for a failing artifact-run, with the fourth argument interpreted as the Phase D pipeline run id captured on latest validation details.';

CREATE OR REPLACE FUNCTION dictionary.phase_e_promote_and_publish_artifact_run(
  p_phase_e_run_id UUID,
  p_root_source_id UUID,
  p_crawl_artifact_id UUID,
  p_validation_substrate_run_id UUID
)
RETURNS TABLE (
  snapshot_id UUID,
  snapshot_version BIGINT,
  promoted_count BIGINT,
  no_op_count BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
  promotion_row RECORD;
  previous_snapshot RECORD;
  new_snapshot_id UUID;
  new_snapshot_version BIGINT;
  promoted_total BIGINT := 0;
  no_op_total BIGINT := 0;
  entity_total BIGINT := 0;
  assertion_total BIGINT := 0;
  alias_total BIGINT := 0;
BEGIN
  PERFORM pg_advisory_xact_lock(
    dictionary.phase_e_identity_lock_key(ARRAY['phase_e_snapshot_publish'])
  );

  SELECT
    s.id,
    s.version
  INTO previous_snapshot
  FROM dictionary.dictionary_active_snapshot a
  JOIN dictionary.dictionary_snapshots s
    ON s.id = a.snapshot_id
  WHERE a.slot = 'newsroom'
    AND s.status = 'published'
  LIMIT 1;

  FOR promotion_row IN
    SELECT *
    FROM dictionary.phase_e_promote_artifact_run(
      p_phase_e_run_id,
      p_root_source_id,
      p_crawl_artifact_id,
      p_validation_substrate_run_id
    )
  LOOP
    IF promotion_row.promotion_outcome = 'promoted' THEN
      promoted_total := promoted_total + 1;
    ELSE
      no_op_total := no_op_total + 1;
    END IF;
  END LOOP;

  IF promoted_total = 0 THEN
    RETURN QUERY
    SELECT
      NULL::uuid,
      NULL::bigint,
      promoted_total,
      no_op_total;
    RETURN;
  END IF;

  SELECT COALESCE(MAX(version), 0) + 1
  INTO new_snapshot_version
  FROM dictionary.dictionary_snapshots;

  INSERT INTO dictionary.dictionary_snapshots (
    version,
    status,
    substrate_run_id,
    entity_count,
    assertion_count,
    alias_count,
    change_summary,
    created_at
  )
  VALUES (
    new_snapshot_version,
    'building',
    p_phase_e_run_id,
    0,
    0,
    0,
    '{}'::jsonb,
    NOW()
  )
  RETURNING id
  INTO new_snapshot_id;

  INSERT INTO dictionary.dictionary_snapshot_records (snapshot_id, record_type, record_id, created_at)
  SELECT new_snapshot_id, 'jurisdiction', j.id, NOW()
  FROM dictionary.dictionary_jurisdictions j;

  INSERT INTO dictionary.dictionary_snapshot_records (snapshot_id, record_type, record_id, created_at)
  SELECT new_snapshot_id, 'entity', e.id, NOW()
  FROM dictionary.dictionary_entities e;

  INSERT INTO dictionary.dictionary_snapshot_records (snapshot_id, record_type, record_id, created_at)
  SELECT new_snapshot_id, 'role', r.id, NOW()
  FROM dictionary.dictionary_roles r;

  INSERT INTO dictionary.dictionary_snapshot_records (snapshot_id, record_type, record_id, created_at)
  SELECT new_snapshot_id, 'alias', a.id, NOW()
  FROM dictionary.dictionary_aliases a;

  INSERT INTO dictionary.dictionary_snapshot_records (snapshot_id, record_type, record_id, created_at)
  SELECT new_snapshot_id, 'assertion', a.id, NOW()
  FROM dictionary.dictionary_assertions a;

  INSERT INTO dictionary.dictionary_snapshot_jurisdictions (
    snapshot_id,
    canonical_record_id,
    name,
    jurisdiction_type,
    parent_jurisdiction_id,
    centroid_lat,
    centroid_lng,
    bbox,
    geojson,
    status,
    last_verified_at,
    created_at,
    updated_at
  )
  SELECT
    new_snapshot_id,
    j.id,
    j.name,
    j.jurisdiction_type,
    j.parent_jurisdiction_id,
    j.centroid_lat,
    j.centroid_lng,
    j.bbox,
    j.geojson,
    j.status,
    j.last_verified_at,
    j.created_at,
    j.updated_at
  FROM dictionary.dictionary_jurisdictions j;

  INSERT INTO dictionary.dictionary_snapshot_entities (
    snapshot_id,
    canonical_record_id,
    entity_type,
    canonical_name,
    slug,
    primary_jurisdiction_id,
    normalized_address,
    lat,
    lng,
    spatial_confidence,
    status,
    description,
    notes,
    attributes,
    last_verified_at,
    created_at,
    updated_at
  )
  SELECT
    new_snapshot_id,
    e.id,
    e.entity_type,
    e.canonical_name,
    e.slug,
    e.primary_jurisdiction_id,
    e.normalized_address,
    e.lat,
    e.lng,
    e.spatial_confidence,
    e.status,
    e.description,
    e.notes,
    e.attributes,
    e.last_verified_at,
    e.created_at,
    e.updated_at
  FROM dictionary.dictionary_entities e;

  INSERT INTO dictionary.dictionary_snapshot_roles (
    snapshot_id,
    canonical_record_id,
    role_name,
    role_type,
    jurisdiction_id,
    status,
    notes,
    term_pattern,
    last_verified_at,
    created_at,
    updated_at
  )
  SELECT
    new_snapshot_id,
    r.id,
    r.role_name,
    r.role_type,
    r.jurisdiction_id,
    r.status,
    r.notes,
    r.term_pattern,
    r.last_verified_at,
    r.created_at,
    r.updated_at
  FROM dictionary.dictionary_roles r;

  INSERT INTO dictionary.dictionary_snapshot_aliases (
    snapshot_id,
    canonical_record_id,
    entity_id,
    alias,
    alias_type,
    status,
    effective_start_at,
    effective_end_at,
    source_count,
    last_verified_at,
    created_at,
    updated_at
  )
  SELECT
    new_snapshot_id,
    a.id,
    a.entity_id,
    a.alias,
    a.alias_type,
    a.status,
    a.effective_start_at,
    a.effective_end_at,
    a.source_count,
    a.last_verified_at,
    a.created_at,
    a.updated_at
  FROM dictionary.dictionary_aliases a;

  INSERT INTO dictionary.dictionary_snapshot_assertions (
    snapshot_id,
    canonical_record_id,
    assertion_type,
    subject_entity_id,
    object_entity_id,
    role_id,
    effective_start_at,
    effective_end_at,
    term_end_at,
    observed_at,
    last_verified_at,
    freshness_sla_days,
    next_election_at,
    next_review_at,
    validity_status,
    review_status,
    assertion_confidence,
    supersedes_assertion_id,
    superseded_by_assertion_id,
    notes,
    created_at,
    updated_at
  )
  SELECT
    new_snapshot_id,
    a.id,
    a.assertion_type,
    a.subject_entity_id,
    a.object_entity_id,
    a.role_id,
    a.effective_start_at,
    a.effective_end_at,
    a.term_end_at,
    a.observed_at,
    a.last_verified_at,
    a.freshness_sla_days,
    a.next_election_at,
    a.next_review_at,
    a.validity_status,
    a.review_status,
    a.assertion_confidence,
    a.supersedes_assertion_id,
    a.superseded_by_assertion_id,
    a.notes,
    a.created_at,
    a.updated_at
  FROM dictionary.dictionary_assertions a;

  UPDATE dictionary.dictionary_assertions
  SET snapshot_id = new_snapshot_id
  WHERE id IN (
    SELECT created_record_id
    FROM dictionary.dictionary_promotion_results
    WHERE substrate_run_id = p_phase_e_run_id
      AND created_record_type = 'assertion'
      AND created_record_id IS NOT NULL

    UNION

    SELECT affected_record_id
    FROM dictionary.dictionary_promotion_results
    WHERE substrate_run_id = p_phase_e_run_id
      AND affected_record_type = 'assertion'
      AND affected_record_id IS NOT NULL
  );

  SELECT COUNT(*) INTO entity_total
  FROM dictionary.dictionary_snapshot_entities se
  WHERE se.snapshot_id = new_snapshot_id;

  SELECT COUNT(*) INTO assertion_total
  FROM dictionary.dictionary_snapshot_assertions sa
  WHERE sa.snapshot_id = new_snapshot_id;

  SELECT COUNT(*) INTO alias_total
  FROM dictionary.dictionary_snapshot_aliases sal
  WHERE sal.snapshot_id = new_snapshot_id;

  UPDATE dictionary.dictionary_snapshots
  SET
    status = 'published',
    entity_count = entity_total,
    assertion_count = assertion_total,
    alias_count = alias_total,
    change_summary = jsonb_build_object(
      'phase_e_run_id', p_phase_e_run_id,
      'root_source_id', p_root_source_id,
      'crawl_artifact_id', p_crawl_artifact_id,
      'phase_d_pipeline_run_id', p_validation_substrate_run_id,
      'previous_snapshot_id', previous_snapshot.id,
      'previous_snapshot_version', previous_snapshot.version,
      'promoted_count', promoted_total,
      'no_op_count', no_op_total
    ),
    published_at = NOW()
  WHERE id = new_snapshot_id;

  IF previous_snapshot.id IS NOT NULL THEN
    UPDATE dictionary.dictionary_snapshots
    SET status = 'superseded'
    WHERE id = previous_snapshot.id
      AND status = 'published';
  END IF;

  INSERT INTO dictionary.dictionary_active_snapshot (slot, snapshot_id, activated_at)
  VALUES ('newsroom', new_snapshot_id, NOW())
  ON CONFLICT (slot)
  DO UPDATE SET
    snapshot_id = EXCLUDED.snapshot_id,
    activated_at = EXCLUDED.activated_at;

  UPDATE dictionary.dictionary_promotion_results pr
  SET snapshot_id = new_snapshot_id
  WHERE pr.substrate_run_id = p_phase_e_run_id
    AND pr.snapshot_id IS NULL;

  RETURN QUERY
  SELECT
    new_snapshot_id,
    new_snapshot_version,
    promoted_total,
    no_op_total;
END;
$$;

COMMENT ON FUNCTION dictionary.phase_e_promote_and_publish_artifact_run(UUID, UUID, UUID, UUID) IS
  'Applies the Phase E artifact-run promotion batch and publishes the resulting immutable snapshot in one statement transaction, with the fourth argument interpreted as the Phase D pipeline run id captured on latest validation details.';
