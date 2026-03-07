-- Phase 1: Prompt Layers (global + section guidance)
-- Idempotent for repeated runs and resilient when table exists in partially-manual form.
-- Seed behavior is first-install only (existing rows are not overwritten by migration).

CREATE TABLE IF NOT EXISTS topic_engine_prompt_layers (
  id BIGSERIAL PRIMARY KEY,
  stage_name TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  section TEXT,
  prompt_template TEXT NOT NULL DEFAULT '',
  version INT NOT NULL DEFAULT 1
    CHECK (version >= 1),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE topic_engine_prompt_layers
  ADD COLUMN IF NOT EXISTS stage_name TEXT;

ALTER TABLE topic_engine_prompt_layers
  ADD COLUMN IF NOT EXISTS scope_type TEXT;

ALTER TABLE topic_engine_prompt_layers
  ADD COLUMN IF NOT EXISTS section TEXT;

ALTER TABLE topic_engine_prompt_layers
  ADD COLUMN IF NOT EXISTS prompt_template TEXT NOT NULL DEFAULT '';

ALTER TABLE topic_engine_prompt_layers
  ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;

ALTER TABLE topic_engine_prompt_layers
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE topic_engine_prompt_layers
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Normalize legacy/manual rows before applying strict NOT NULL constraints.
UPDATE topic_engine_prompt_layers
SET prompt_template = ''
WHERE prompt_template IS NULL;

UPDATE topic_engine_prompt_layers
SET version = 1
WHERE version IS NULL;

UPDATE topic_engine_prompt_layers
SET created_at = NOW()
WHERE created_at IS NULL;

UPDATE topic_engine_prompt_layers
SET updated_at = NOW()
WHERE updated_at IS NULL;

DELETE FROM topic_engine_prompt_layers
WHERE stage_name IS NULL
   OR scope_type IS NULL;

-- Remove invalid legacy/manual rows prior to CHECK validation.
DELETE FROM topic_engine_prompt_layers
WHERE stage_name NOT IN (
    'topic_qualification',
    'research_discovery',
    'evidence_extraction',
    'story_planning',
    'draft_writing',
    'final_review'
  )
  OR scope_type NOT IN ('global', 'section')
  OR (
    scope_type = 'global'
    AND section IS NOT NULL
  )
  OR (
    scope_type = 'section'
    AND (
      section IS NULL
      OR section NOT IN (
        'local',
        'national',
        'world',
        'business',
        'sports',
        'health',
        'entertainment',
        'technology'
      )
    )
  );

ALTER TABLE topic_engine_prompt_layers
  ALTER COLUMN stage_name SET NOT NULL;

ALTER TABLE topic_engine_prompt_layers
  ALTER COLUMN scope_type SET NOT NULL;

ALTER TABLE topic_engine_prompt_layers
  ALTER COLUMN prompt_template SET NOT NULL;

ALTER TABLE topic_engine_prompt_layers
  ALTER COLUMN prompt_template SET DEFAULT '';

ALTER TABLE topic_engine_prompt_layers
  ALTER COLUMN version SET NOT NULL;

ALTER TABLE topic_engine_prompt_layers
  ALTER COLUMN created_at SET NOT NULL;

ALTER TABLE topic_engine_prompt_layers
  ALTER COLUMN updated_at SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'topic_engine_prompt_layers_version_chk'
  ) THEN
    ALTER TABLE topic_engine_prompt_layers
      ADD CONSTRAINT topic_engine_prompt_layers_version_chk
      CHECK (version >= 1) NOT VALID;
    ALTER TABLE topic_engine_prompt_layers
      VALIDATE CONSTRAINT topic_engine_prompt_layers_version_chk;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'topic_engine_prompt_layers_scope_type_chk'
  ) THEN
    ALTER TABLE topic_engine_prompt_layers
      ADD CONSTRAINT topic_engine_prompt_layers_scope_type_chk
      CHECK (scope_type IN ('global', 'section')) NOT VALID;
    ALTER TABLE topic_engine_prompt_layers
      VALIDATE CONSTRAINT topic_engine_prompt_layers_scope_type_chk;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'topic_engine_prompt_layers_stage_name_chk'
  ) THEN
    ALTER TABLE topic_engine_prompt_layers
      ADD CONSTRAINT topic_engine_prompt_layers_stage_name_chk
      CHECK (
        stage_name IN (
          'topic_qualification',
          'research_discovery',
          'evidence_extraction',
          'story_planning',
          'draft_writing',
          'final_review'
        )
      ) NOT VALID;
    ALTER TABLE topic_engine_prompt_layers
      VALIDATE CONSTRAINT topic_engine_prompt_layers_stage_name_chk;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'topic_engine_prompt_layers_scope_section_shape_chk'
  ) THEN
    ALTER TABLE topic_engine_prompt_layers
      ADD CONSTRAINT topic_engine_prompt_layers_scope_section_shape_chk
      CHECK (
        (scope_type = 'global' AND section IS NULL)
        OR
        (scope_type = 'section' AND section IS NOT NULL)
      ) NOT VALID;
    ALTER TABLE topic_engine_prompt_layers
      VALIDATE CONSTRAINT topic_engine_prompt_layers_scope_section_shape_chk;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'topic_engine_prompt_layers_section_value_chk'
  ) THEN
    ALTER TABLE topic_engine_prompt_layers
      ADD CONSTRAINT topic_engine_prompt_layers_section_value_chk
      CHECK (
        section IS NULL
        OR section IN (
          'local',
          'national',
          'world',
          'business',
          'sports',
          'health',
          'entertainment',
          'technology'
        )
      ) NOT VALID;
    ALTER TABLE topic_engine_prompt_layers
      VALIDATE CONSTRAINT topic_engine_prompt_layers_section_value_chk;
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_engine_prompt_layers_global_stage
  ON topic_engine_prompt_layers (stage_name)
  WHERE scope_type = 'global';

CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_engine_prompt_layers_section_stage_section
  ON topic_engine_prompt_layers (stage_name, section)
  WHERE scope_type = 'section';

CREATE INDEX IF NOT EXISTS idx_topic_engine_prompt_layers_lookup
  ON topic_engine_prompt_layers (scope_type, stage_name, section);

CREATE OR REPLACE FUNCTION set_topic_engine_prompt_layers_audit_fields()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := NOW();
  NEW.version := COALESCE(OLD.version, 0) + 1;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_topic_engine_prompt_layers_audit_fields
  ON topic_engine_prompt_layers;

CREATE TRIGGER trg_topic_engine_prompt_layers_audit_fields
  BEFORE UPDATE ON topic_engine_prompt_layers
  FOR EACH ROW
  EXECUTE FUNCTION set_topic_engine_prompt_layers_audit_fields();

INSERT INTO topic_engine_prompt_layers (stage_name, scope_type, section, prompt_template, version, updated_at)
VALUES
  (
    'topic_qualification',
    'global',
    NULL,
    'Classify conservative-first for local newsroom relevance. Prefer watch over promote when evidence is thin, and keep event keys stable across related signals.',
    1,
    NOW()
  ),
  (
    'research_discovery',
    'global',
    NULL,
    'Generate focused reporting queries that surface verifiable documents and high-signal local sources before broad explainers.',
    1,
    NOW()
  ),
  (
    'evidence_extraction',
    'global',
    NULL,
    'Extract only source-grounded claims with clear why-it-matters framing and concise evidence quotes suitable for downstream planning.',
    1,
    NOW()
  ),
  (
    'story_planning',
    'global',
    NULL,
    'Build publication-ready plans with concrete section flow, explicit uncertainty callouts, and clear reader impact ordering.',
    1,
    NOW()
  ),
  (
    'draft_writing',
    'global',
    NULL,
    'Write with disciplined local journalism voice: precise, sourced, and clear about uncertainty, avoiding speculative filler.',
    1,
    NOW()
  ),
  (
    'final_review',
    'global',
    NULL,
    'Review for factual grounding, policy alignment, and editorial clarity; escalate uncertain claims and preserve attribution rigor.',
    1,
    NOW()
  )
ON CONFLICT DO NOTHING;
