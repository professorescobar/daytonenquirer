-- Phase E / Chunk 3 follow-up: canonical identity locking for concurrent promotion
-- Safe to run multiple times.

CREATE OR REPLACE FUNCTION dictionary.phase_e_identity_lock_key(parts TEXT[])
RETURNS BIGINT
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT ('x' || substr(md5(array_to_string(COALESCE(parts, ARRAY[]::text[]), '||')), 1, 16))::bit(64)::bigint
$$;

CREATE OR REPLACE FUNCTION dictionary.phase_e_promote_merge_proposal(
  p_phase_e_run_id UUID,
  p_merge_proposal_id UUID,
  p_validation_result_id UUID
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
  existing_result RECORD;
  target_row RECORD;
  exact_row RECORD;
  v_promotion_outcome TEXT := 'promoted';
  v_created_record_type TEXT := NULL;
  v_created_record_id UUID := NULL;
  v_affected_record_type TEXT := NULL;
  v_affected_record_id UUID := NULL;
  v_observed_at TIMESTAMPTZ := NULL;
  v_effective_start_at TIMESTAMPTZ := NULL;
  v_effective_end_at TIMESTAMPTZ := NULL;
  v_term_end_at TIMESTAMPTZ := NULL;
  v_role_id UUID := NULL;
  v_subject_entity_id UUID := NULL;
  v_object_entity_id UUID := NULL;
BEGIN
  PERFORM 1
  FROM dictionary.dictionary_merge_proposals mp
  WHERE mp.id = p_merge_proposal_id
  FOR UPDATE;

  SELECT
    pr.merge_proposal_id,
    pr.validation_result_id,
    mp.proposal_type,
    pr.promotion_outcome,
    pr.created_record_type,
    pr.created_record_id,
    pr.affected_record_type,
    pr.affected_record_id
  INTO existing_result
  FROM dictionary.dictionary_promotion_results pr
  JOIN dictionary.dictionary_merge_proposals mp
    ON mp.id = pr.merge_proposal_id
  WHERE pr.merge_proposal_id = p_merge_proposal_id;

  IF FOUND THEN
    RETURN QUERY
    SELECT
      existing_result.merge_proposal_id,
      existing_result.validation_result_id,
      existing_result.proposal_type,
      existing_result.promotion_outcome,
      existing_result.created_record_type,
      existing_result.created_record_id,
      existing_result.affected_record_type,
      existing_result.affected_record_id;
    RETURN;
  END IF;

  SELECT
    p.*,
    ca.source_url,
    ca.source_domain,
    ca.fetched_at,
    rs.trust_tier,
    rs.freshness_sla_days
  INTO proposal_row
  FROM dictionary.phase_e_promotable_merge_proposals p
  JOIN dictionary.dictionary_crawl_artifacts ca
    ON ca.id = p.crawl_artifact_id
  JOIN dictionary.dictionary_root_sources rs
    ON rs.id = p.root_source_id
  WHERE p.merge_proposal_id = p_merge_proposal_id
    AND p.validation_result_id = p_validation_result_id
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Phase E promotable proposal % with validation % was not found', p_merge_proposal_id, p_validation_result_id;
  END IF;

  v_observed_at := COALESCE(
    NULLIF(proposal_row.candidate_payload->>'observed_at', '')::timestamptz,
    proposal_row.fetched_at
  );

  IF proposal_row.proposal_type = 'merge_duplicate' THEN
    v_promotion_outcome := 'no_op';
    v_affected_record_type := proposal_row.target_record_type;
    v_affected_record_id := proposal_row.target_record_id;

  ELSIF proposal_row.proposal_type = 'create_entity' THEN
    PERFORM pg_advisory_xact_lock(
      dictionary.phase_e_identity_lock_key(ARRAY[
        'entity',
        lower(COALESCE(proposal_row.candidate_payload->>'entity_kind', '')),
        lower(COALESCE(proposal_row.candidate_payload->>'canonical_name', ''))
      ])
    );

    SELECT e.id
    INTO exact_row
    FROM dictionary.dictionary_entities e
    WHERE lower(e.canonical_name) = lower(proposal_row.candidate_payload->>'canonical_name')
      AND lower(e.entity_type) = lower(proposal_row.candidate_payload->>'entity_kind')
      AND e.status = 'active'
    ORDER BY e.updated_at DESC, e.created_at DESC, e.id ASC
    LIMIT 1;

    IF exact_row.id IS NOT NULL THEN
      v_promotion_outcome := 'no_op';
      v_affected_record_type := 'entity';
      v_affected_record_id := exact_row.id;
    ELSE
      INSERT INTO dictionary.dictionary_entities (
        entity_type,
        canonical_name,
        slug,
        status,
        description,
        attributes,
        last_verified_at,
        created_at,
        updated_at
      )
      VALUES (
        proposal_row.candidate_payload->>'entity_kind',
        proposal_row.candidate_payload->>'canonical_name',
        dictionary.phase_e_slugify_entity_name(
          proposal_row.candidate_payload->>'canonical_name',
          proposal_row.proposal_key
        ),
        'active',
        NULLIF(proposal_row.candidate_payload->>'description', ''),
        jsonb_build_object(
          'source_mentions',
          COALESCE(proposal_row.candidate_payload->'source_mentions', '[]'::jsonb)
        ),
        proposal_row.fetched_at,
        NOW(),
        NOW()
      )
      RETURNING id
      INTO v_created_record_id;

      v_created_record_type := 'entity';
    END IF;

  ELSIF proposal_row.proposal_type = 'add_alias' THEN
    IF proposal_row.target_record_type IS DISTINCT FROM 'entity'
       OR proposal_row.target_record_id IS NULL
    THEN
      RAISE EXCEPTION 'Phase E add_alias proposal % is missing target entity', proposal_row.merge_proposal_id;
    END IF;

    PERFORM pg_advisory_xact_lock(
      dictionary.phase_e_identity_lock_key(ARRAY[
        'alias',
        proposal_row.target_record_id::text,
        lower(COALESCE(proposal_row.candidate_payload->>'alias_kind', '')),
        lower(COALESCE(proposal_row.candidate_payload->>'alias_name', ''))
      ])
    );

    SELECT da.id
    INTO exact_row
    FROM dictionary.dictionary_aliases da
    WHERE da.entity_id = proposal_row.target_record_id
      AND lower(da.alias) = lower(proposal_row.candidate_payload->>'alias_name')
      AND lower(da.alias_type) = lower(proposal_row.candidate_payload->>'alias_kind')
      AND da.effective_start_at IS NULL
      AND da.effective_end_at IS NULL
      AND da.status = 'active'
    ORDER BY da.updated_at DESC, da.created_at DESC, da.id ASC
    LIMIT 1;

    IF exact_row.id IS NOT NULL THEN
      v_promotion_outcome := 'no_op';
      v_affected_record_type := 'alias';
      v_affected_record_id := exact_row.id;
    ELSE
      INSERT INTO dictionary.dictionary_aliases (
        entity_id,
        alias,
        alias_type,
        status,
        source_count,
        last_verified_at,
        created_at,
        updated_at
      )
      VALUES (
        proposal_row.target_record_id,
        proposal_row.candidate_payload->>'alias_name',
        proposal_row.candidate_payload->>'alias_kind',
        'active',
        1,
        proposal_row.fetched_at,
        NOW(),
        NOW()
      )
      RETURNING id
      INTO v_created_record_id;

      v_created_record_type := 'alias';
    END IF;

  ELSIF proposal_row.proposal_type = 'create_role' THEN
    PERFORM pg_advisory_xact_lock(
      dictionary.phase_e_identity_lock_key(ARRAY[
        'role',
        lower(COALESCE(proposal_row.candidate_payload->>'role_kind', '')),
        lower(COALESCE(proposal_row.candidate_payload->>'role_name', '')),
        COALESCE(NULLIF(proposal_row.proposal_payload->>'resolved_jurisdiction_id', ''), 'null')
      ])
    );

    SELECT r.id
    INTO exact_row
    FROM dictionary.dictionary_roles r
    WHERE lower(r.role_name) = lower(proposal_row.candidate_payload->>'role_name')
      AND lower(r.role_type) = lower(proposal_row.candidate_payload->>'role_kind')
      AND (
        (NULLIF(proposal_row.proposal_payload->>'resolved_jurisdiction_id', '')::uuid IS NULL AND r.jurisdiction_id IS NULL)
        OR r.jurisdiction_id = NULLIF(proposal_row.proposal_payload->>'resolved_jurisdiction_id', '')::uuid
      )
      AND r.status = 'active'
    ORDER BY r.updated_at DESC, r.created_at DESC, r.id ASC
    LIMIT 1;

    IF exact_row.id IS NOT NULL THEN
      v_promotion_outcome := 'no_op';
      v_affected_record_type := 'role';
      v_affected_record_id := exact_row.id;
    ELSE
      INSERT INTO dictionary.dictionary_roles (
        role_name,
        role_type,
        jurisdiction_id,
        status,
        term_pattern,
        last_verified_at,
        created_at,
        updated_at
      )
      VALUES (
        proposal_row.candidate_payload->>'role_name',
        proposal_row.candidate_payload->>'role_kind',
        NULLIF(proposal_row.proposal_payload->>'resolved_jurisdiction_id', '')::uuid,
        'active',
        jsonb_build_object(
          'governing_body_name',
          NULLIF(proposal_row.candidate_payload->>'governing_body_name', ''),
          'is_time_bounded',
          COALESCE((proposal_row.candidate_payload->>'is_time_bounded')::boolean, false)
        ),
        proposal_row.fetched_at,
        NOW(),
        NOW()
      )
      RETURNING id
      INTO v_created_record_id;

      v_created_record_type := 'role';
    END IF;

  ELSIF proposal_row.proposal_type = 'create_jurisdiction' THEN
    PERFORM pg_advisory_xact_lock(
      dictionary.phase_e_identity_lock_key(ARRAY[
        'jurisdiction',
        lower(COALESCE(proposal_row.candidate_payload->>'jurisdiction_type', '')),
        lower(COALESCE(proposal_row.candidate_payload->>'canonical_name', '')),
        COALESCE(NULLIF(proposal_row.proposal_payload->>'resolved_parent_jurisdiction_id', ''), 'null')
      ])
    );

    SELECT j.id
    INTO exact_row
    FROM dictionary.dictionary_jurisdictions j
    WHERE lower(j.name) = lower(proposal_row.candidate_payload->>'canonical_name')
      AND lower(j.jurisdiction_type) = lower(proposal_row.candidate_payload->>'jurisdiction_type')
      AND (
        (NULLIF(proposal_row.proposal_payload->>'resolved_parent_jurisdiction_id', '')::uuid IS NULL AND j.parent_jurisdiction_id IS NULL)
        OR j.parent_jurisdiction_id = NULLIF(proposal_row.proposal_payload->>'resolved_parent_jurisdiction_id', '')::uuid
      )
      AND j.status = 'active'
    ORDER BY j.updated_at DESC, j.created_at DESC, j.id ASC
    LIMIT 1;

    IF exact_row.id IS NOT NULL THEN
      v_promotion_outcome := 'no_op';
      v_affected_record_type := 'jurisdiction';
      v_affected_record_id := exact_row.id;
    ELSE
      INSERT INTO dictionary.dictionary_jurisdictions (
        name,
        jurisdiction_type,
        parent_jurisdiction_id,
        status,
        last_verified_at,
        created_at,
        updated_at
      )
      VALUES (
        proposal_row.candidate_payload->>'canonical_name',
        proposal_row.candidate_payload->>'jurisdiction_type',
        NULLIF(proposal_row.proposal_payload->>'resolved_parent_jurisdiction_id', '')::uuid,
        'active',
        proposal_row.fetched_at,
        NOW(),
        NOW()
      )
      RETURNING id
      INTO v_created_record_id;

      v_created_record_type := 'jurisdiction';
    END IF;

  ELSIF proposal_row.proposal_type IN ('create_assertion', 'supersede_assertion') THEN
    v_subject_entity_id := NULLIF(proposal_row.proposal_payload->>'resolved_subject_entity_id', '')::uuid;
    v_object_entity_id := NULLIF(proposal_row.proposal_payload->>'resolved_object_entity_id', '')::uuid;
    v_role_id := NULLIF(proposal_row.proposal_payload->>'resolved_role_id', '')::uuid;
    v_effective_start_at := NULLIF(proposal_row.candidate_payload->>'effective_start_at', '')::timestamptz;
    v_effective_end_at := NULLIF(proposal_row.candidate_payload->>'effective_end_at', '')::timestamptz;
    v_term_end_at := NULLIF(proposal_row.candidate_payload->>'term_end_at', '')::timestamptz;

    IF v_subject_entity_id IS NULL OR (v_object_entity_id IS NULL AND v_role_id IS NULL) THEN
      RAISE EXCEPTION 'Phase E assertion proposal % is missing resolved canonical references', proposal_row.merge_proposal_id;
    END IF;

    PERFORM pg_advisory_xact_lock(
      dictionary.phase_e_identity_lock_key(ARRAY[
        'assertion',
        lower(COALESCE(proposal_row.candidate_payload->>'assertion_type', '')),
        COALESCE(v_subject_entity_id::text, 'null'),
        COALESCE(v_object_entity_id::text, 'null'),
        COALESCE(v_role_id::text, 'null'),
        COALESCE(v_effective_start_at::text, 'null'),
        COALESCE(v_effective_end_at::text, 'null'),
        COALESCE(v_term_end_at::text, 'null')
      ])
    );

    SELECT a.id
    INTO exact_row
    FROM dictionary.dictionary_assertions a
    WHERE lower(a.assertion_type) = lower(proposal_row.candidate_payload->>'assertion_type')
      AND a.subject_entity_id = v_subject_entity_id
      AND (
        (v_object_entity_id IS NULL AND a.object_entity_id IS NULL)
        OR a.object_entity_id = v_object_entity_id
      )
      AND (
        (v_role_id IS NULL AND a.role_id IS NULL)
        OR a.role_id = v_role_id
      )
      AND a.effective_start_at IS NOT DISTINCT FROM v_effective_start_at
      AND a.effective_end_at IS NOT DISTINCT FROM v_effective_end_at
      AND a.term_end_at IS NOT DISTINCT FROM v_term_end_at
    ORDER BY a.updated_at DESC, a.created_at DESC, a.id ASC
    LIMIT 1;

    IF exact_row.id IS NOT NULL THEN
      v_promotion_outcome := 'no_op';
      v_affected_record_type := 'assertion';
      v_affected_record_id := exact_row.id;
    ELSE
      IF proposal_row.proposal_type = 'supersede_assertion' THEN
        IF proposal_row.target_record_type IS DISTINCT FROM 'assertion'
           OR proposal_row.target_record_id IS NULL
        THEN
          RAISE EXCEPTION 'Phase E supersede_assertion proposal % is missing target assertion', proposal_row.merge_proposal_id;
        END IF;

        SELECT
          a.id,
          a.superseded_by_assertion_id
        INTO target_row
        FROM dictionary.dictionary_assertions a
        WHERE a.id = proposal_row.target_record_id
        FOR UPDATE
        LIMIT 1;

        IF target_row.id IS NULL THEN
          RAISE EXCEPTION 'Phase E supersede_assertion target % no longer exists', proposal_row.target_record_id;
        END IF;

        IF target_row.superseded_by_assertion_id IS NOT NULL THEN
          RAISE EXCEPTION 'Phase E supersede_assertion target % is already superseded', proposal_row.target_record_id;
        END IF;
      END IF;

      INSERT INTO dictionary.dictionary_assertions (
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
        next_review_at,
        validity_status,
        review_status,
        assertion_confidence,
        supersedes_assertion_id,
        created_at,
        updated_at
      )
      VALUES (
        proposal_row.candidate_payload->>'assertion_type',
        v_subject_entity_id,
        v_object_entity_id,
        v_role_id,
        v_effective_start_at,
        v_effective_end_at,
        v_term_end_at,
        NULLIF(proposal_row.candidate_payload->>'observed_at', '')::timestamptz,
        proposal_row.fetched_at,
        proposal_row.freshness_sla_days,
        CASE
          WHEN proposal_row.freshness_sla_days IS NULL THEN NULL
          ELSE proposal_row.fetched_at + make_interval(days => proposal_row.freshness_sla_days)
        END,
        dictionary.phase_e_assertion_validity_status(
          v_effective_start_at,
          v_effective_end_at,
          v_term_end_at,
          NULLIF(proposal_row.candidate_payload->>'observed_at', '')::timestamptz
        ),
        'verified',
        NULLIF(proposal_row.candidate_payload->>'assertion_confidence', '')::double precision,
        CASE
          WHEN proposal_row.proposal_type = 'supersede_assertion' THEN proposal_row.target_record_id
          ELSE NULL
        END,
        NOW(),
        NOW()
      )
      RETURNING id
      INTO v_created_record_id;

      v_created_record_type := 'assertion';

      IF proposal_row.proposal_type = 'supersede_assertion' THEN
        UPDATE dictionary.dictionary_assertions
        SET
          superseded_by_assertion_id = v_created_record_id,
          validity_status = 'superseded',
          review_status = 'verified',
          last_verified_at = proposal_row.fetched_at,
          updated_at = NOW()
        WHERE id = proposal_row.target_record_id;

        v_affected_record_type := 'assertion';
        v_affected_record_id := proposal_row.target_record_id;
      END IF;
    END IF;

  ELSE
    RAISE EXCEPTION 'Unsupported Phase E promotion type: %', proposal_row.proposal_type;
  END IF;

  IF v_created_record_id IS NOT NULL THEN
    INSERT INTO dictionary.dictionary_provenance (
      record_type,
      record_id,
      root_source_id,
      crawl_artifact_id,
      source_url,
      source_domain,
      trust_tier,
      observed_at,
      captured_at,
      substrate_run_id,
      extraction_version,
      created_at
    )
    VALUES (
      v_created_record_type,
      v_created_record_id,
      proposal_row.root_source_id,
      proposal_row.crawl_artifact_id,
      proposal_row.source_url,
      proposal_row.source_domain,
      proposal_row.trust_tier,
      v_observed_at,
      proposal_row.fetched_at,
      p_phase_e_run_id,
      proposal_row.extraction_version,
      NOW()
    )
    ON CONFLICT DO NOTHING;
  END IF;

  IF proposal_row.proposal_type = 'supersede_assertion'
     AND v_promotion_outcome = 'promoted'
     AND v_affected_record_id IS NOT NULL
  THEN
    INSERT INTO dictionary.dictionary_provenance (
      record_type,
      record_id,
      root_source_id,
      crawl_artifact_id,
      source_url,
      source_domain,
      trust_tier,
      observed_at,
      captured_at,
      substrate_run_id,
      extraction_version,
      created_at
    )
    VALUES (
      'assertion',
      v_affected_record_id,
      proposal_row.root_source_id,
      proposal_row.crawl_artifact_id,
      proposal_row.source_url,
      proposal_row.source_domain,
      proposal_row.trust_tier,
      v_observed_at,
      proposal_row.fetched_at,
      p_phase_e_run_id,
      proposal_row.extraction_version,
      NOW()
    )
    ON CONFLICT DO NOTHING;
  END IF;

  INSERT INTO dictionary.dictionary_promotion_results (
    substrate_run_id,
    merge_proposal_id,
    validation_result_id,
    snapshot_id,
    promotion_outcome,
    created_record_type,
    created_record_id,
    affected_record_type,
    affected_record_id,
    details,
    created_at
  )
  VALUES (
    p_phase_e_run_id,
    proposal_row.merge_proposal_id,
    proposal_row.validation_result_id,
    NULL,
    v_promotion_outcome,
    v_created_record_type,
    v_created_record_id,
    v_affected_record_type,
    v_affected_record_id,
    jsonb_build_object(
      'proposal_key', proposal_row.proposal_key,
      'proposal_type', proposal_row.proposal_type,
      'validation_substrate_run_id', proposal_row.validation_substrate_run_id,
      'promotion_phase', 'phase_e_chunk3_canonical_mutation',
      'provenance_write_count',
      CASE
        WHEN v_created_record_id IS NOT NULL AND proposal_row.proposal_type = 'supersede_assertion' AND v_affected_record_id IS NOT NULL THEN 2
        WHEN v_created_record_id IS NOT NULL THEN 1
        ELSE 0
      END
    ),
    NOW()
  );

  RETURN QUERY
  SELECT
    proposal_row.merge_proposal_id,
    proposal_row.validation_result_id,
    proposal_row.proposal_type,
    v_promotion_outcome,
    v_created_record_type,
    v_created_record_id,
    v_affected_record_type,
    v_affected_record_id;
END;
$$;

COMMENT ON FUNCTION dictionary.phase_e_identity_lock_key(TEXT[]) IS
  'Builds deterministic advisory-lock keys for canonical identity surfaces that need protection from concurrent Phase E promotion races.';
