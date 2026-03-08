-- Prompt layers constraint hardening (non-destructive).
-- Replaces strict enumerated stage/section constraints with permissive non-empty checks.
-- Safe to run multiple times.

DO $$
BEGIN
  IF to_regclass('public.topic_engine_prompt_layers') IS NULL THEN
    RETURN;
  END IF;

  -- Drop old strict constraints if present.
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'topic_engine_prompt_layers_stage_name_chk'
  ) THEN
    ALTER TABLE topic_engine_prompt_layers
      DROP CONSTRAINT topic_engine_prompt_layers_stage_name_chk;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'topic_engine_prompt_layers_section_value_chk'
  ) THEN
    ALTER TABLE topic_engine_prompt_layers
      DROP CONSTRAINT topic_engine_prompt_layers_section_value_chk;
  END IF;

  -- Ensure global rows do not carry empty-string section artifacts.
  UPDATE topic_engine_prompt_layers
  SET section = NULL
  WHERE scope_type = 'global'
    AND section IS NOT NULL
    AND trim(section) = '';

  -- Normalize malformed section rows with blank section into deterministic legacy placeholders
  -- so section non-empty validation can pass without deleting operator-authored rows.
  UPDATE topic_engine_prompt_layers
  SET section = 'legacy-section-' || id::text
  WHERE scope_type = 'section'
    AND (section IS NULL OR trim(section) = '');

  -- Recreate permissive checks.
  ALTER TABLE topic_engine_prompt_layers
    ADD CONSTRAINT topic_engine_prompt_layers_stage_name_chk
    CHECK (length(trim(stage_name)) > 0) NOT VALID;

  ALTER TABLE topic_engine_prompt_layers
    VALIDATE CONSTRAINT topic_engine_prompt_layers_stage_name_chk;

  ALTER TABLE topic_engine_prompt_layers
    ADD CONSTRAINT topic_engine_prompt_layers_section_value_chk
    CHECK (section IS NULL OR length(trim(section)) > 0) NOT VALID;

  ALTER TABLE topic_engine_prompt_layers
    VALIDATE CONSTRAINT topic_engine_prompt_layers_section_value_chk;
END $$;
