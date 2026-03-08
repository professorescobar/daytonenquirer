-- Phase 1 prompt layers hardening:
-- Deduplicate existing rows before enforcing unique indexes.
-- Safe to run multiple times.

DO $$
BEGIN
  IF to_regclass('public.topic_engine_prompt_layers') IS NULL THEN
    RETURN;
  END IF;

  -- 1) Remove duplicate global rows per stage, keep newest/highest-version row.
  WITH ranked AS (
    SELECT
      id,
      ROW_NUMBER() OVER (
        PARTITION BY stage_name
        ORDER BY
          version DESC,
          updated_at DESC,
          created_at DESC,
          id DESC
      ) AS rn
    FROM topic_engine_prompt_layers
    WHERE scope_type = 'global'
  )
  DELETE FROM topic_engine_prompt_layers tpl
  USING ranked r
  WHERE tpl.id = r.id
    AND r.rn > 1;

  -- 2) Remove duplicate section rows per (stage, section), keep newest/highest-version row.
  WITH ranked AS (
    SELECT
      id,
      ROW_NUMBER() OVER (
        PARTITION BY stage_name, section
        ORDER BY
          version DESC,
          updated_at DESC,
          created_at DESC,
          id DESC
      ) AS rn
    FROM topic_engine_prompt_layers
    WHERE scope_type = 'section'
  )
  DELETE FROM topic_engine_prompt_layers tpl
  USING ranked r
  WHERE tpl.id = r.id
    AND r.rn > 1;
END $$;

-- 3) Ensure unique indexes exist after dedupe (only if table exists).
DO $$
BEGIN
  IF to_regclass('public.topic_engine_prompt_layers') IS NULL THEN
    RETURN;
  END IF;

  CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_engine_prompt_layers_global_stage
    ON topic_engine_prompt_layers (stage_name)
    WHERE scope_type = 'global';

  CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_engine_prompt_layers_section_stage_section
    ON topic_engine_prompt_layers (stage_name, section)
    WHERE scope_type = 'section';
END $$;
