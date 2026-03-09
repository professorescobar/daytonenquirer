-- Phase 6: persona/topic-engine image controls
-- Safe to run multiple times. Non-destructive.

DO $$
BEGIN
  IF to_regclass('public.personas') IS NULL THEN
    RETURN;
  END IF;

  ALTER TABLE personas ADD COLUMN IF NOT EXISTS image_db_enabled BOOLEAN;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS image_sourcing_enabled BOOLEAN;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS image_generation_enabled BOOLEAN;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS image_mode TEXT;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS image_profile TEXT;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS image_fallback_asset_url TEXT;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS image_fallback_cloudinary_public_id TEXT;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS quota_postgres_image_daily INTEGER;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS quota_sourced_image_daily INTEGER;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS quota_generated_image_daily INTEGER;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS quota_text_only_daily INTEGER;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS layer6_timeout_seconds INTEGER;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS layer6_budget_usd NUMERIC(10,4);
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS exa_max_attempts INTEGER;
  ALTER TABLE personas ADD COLUMN IF NOT EXISTS generation_max_attempts INTEGER;
END $$;

DO $$
BEGIN
  IF to_regclass('public.personas') IS NULL THEN
    RETURN;
  END IF;

  UPDATE personas SET image_db_enabled = TRUE WHERE image_db_enabled IS NULL;
  UPDATE personas SET image_sourcing_enabled = TRUE WHERE image_sourcing_enabled IS NULL;
  UPDATE personas SET image_generation_enabled = FALSE WHERE image_generation_enabled IS NULL;
  UPDATE personas SET image_mode = 'manual' WHERE image_mode IS NULL OR trim(image_mode) = '';
  UPDATE personas SET image_profile = 'professional' WHERE image_profile IS NULL OR trim(image_profile) = '';
  UPDATE personas SET quota_postgres_image_daily = 200 WHERE quota_postgres_image_daily IS NULL OR quota_postgres_image_daily < 0;
  UPDATE personas SET quota_sourced_image_daily = 120 WHERE quota_sourced_image_daily IS NULL OR quota_sourced_image_daily < 0;
  UPDATE personas SET quota_generated_image_daily = 30 WHERE quota_generated_image_daily IS NULL OR quota_generated_image_daily < 0;
  UPDATE personas SET quota_text_only_daily = 400 WHERE quota_text_only_daily IS NULL OR quota_text_only_daily < 0;
  UPDATE personas SET layer6_timeout_seconds = 90 WHERE layer6_timeout_seconds IS NULL OR layer6_timeout_seconds < 15;
  UPDATE personas SET layer6_budget_usd = 0.20 WHERE layer6_budget_usd IS NULL OR layer6_budget_usd < 0;
  UPDATE personas SET exa_max_attempts = 3 WHERE exa_max_attempts IS NULL OR exa_max_attempts < 1;
  UPDATE personas SET generation_max_attempts = 2 WHERE generation_max_attempts IS NULL OR generation_max_attempts < 1;

  ALTER TABLE personas ALTER COLUMN image_db_enabled SET DEFAULT TRUE;
  ALTER TABLE personas ALTER COLUMN image_sourcing_enabled SET DEFAULT TRUE;
  ALTER TABLE personas ALTER COLUMN image_generation_enabled SET DEFAULT FALSE;
  ALTER TABLE personas ALTER COLUMN image_mode SET DEFAULT 'manual';
  ALTER TABLE personas ALTER COLUMN image_profile SET DEFAULT 'professional';
  ALTER TABLE personas ALTER COLUMN layer6_timeout_seconds SET DEFAULT 90;
  ALTER TABLE personas ALTER COLUMN layer6_budget_usd SET DEFAULT 0.20;
  ALTER TABLE personas ALTER COLUMN exa_max_attempts SET DEFAULT 3;
  ALTER TABLE personas ALTER COLUMN generation_max_attempts SET DEFAULT 2;
  ALTER TABLE personas ALTER COLUMN quota_postgres_image_daily SET DEFAULT 200;
  ALTER TABLE personas ALTER COLUMN quota_sourced_image_daily SET DEFAULT 120;
  ALTER TABLE personas ALTER COLUMN quota_generated_image_daily SET DEFAULT 30;
  ALTER TABLE personas ALTER COLUMN quota_text_only_daily SET DEFAULT 400;

  ALTER TABLE personas ALTER COLUMN image_db_enabled SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN image_sourcing_enabled SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN image_generation_enabled SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN image_mode SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN image_profile SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN layer6_timeout_seconds SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN layer6_budget_usd SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN exa_max_attempts SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN generation_max_attempts SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN quota_postgres_image_daily SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN quota_sourced_image_daily SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN quota_generated_image_daily SET NOT NULL;
  ALTER TABLE personas ALTER COLUMN quota_text_only_daily SET NOT NULL;
END $$;

DO $$
BEGIN
  IF to_regclass('public.personas') IS NULL THEN
    RETURN;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'personas_image_mode_chk'
  ) THEN
    ALTER TABLE personas
      ADD CONSTRAINT personas_image_mode_chk
      CHECK (image_mode IN ('manual', 'auto')) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'personas_image_profile_chk'
  ) THEN
    ALTER TABLE personas
      ADD CONSTRAINT personas_image_profile_chk
      CHECK (image_profile IN ('professional', 'creative', 'cheap')) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'personas_layer6_bounds_chk'
  ) THEN
    ALTER TABLE personas
      ADD CONSTRAINT personas_layer6_bounds_chk
      CHECK (
        quota_postgres_image_daily >= 0
        AND quota_sourced_image_daily >= 0
        AND quota_generated_image_daily >= 0
        AND quota_text_only_daily >= 0
        AND layer6_timeout_seconds BETWEEN 15 AND 600
        AND layer6_budget_usd BETWEEN 0 AND 50
        AND exa_max_attempts BETWEEN 1 AND 20
        AND generation_max_attempts BETWEEN 1 AND 20
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM personas
    WHERE image_mode NOT IN ('manual', 'auto')
       OR image_profile NOT IN ('professional', 'creative', 'cheap')
       OR quota_postgres_image_daily < 0
       OR quota_sourced_image_daily < 0
       OR quota_generated_image_daily < 0
       OR quota_text_only_daily < 0
       OR layer6_timeout_seconds < 15
       OR layer6_timeout_seconds > 600
       OR layer6_budget_usd < 0
       OR layer6_budget_usd > 50
       OR exa_max_attempts < 1
       OR exa_max_attempts > 20
       OR generation_max_attempts < 1
       OR generation_max_attempts > 20
  ) THEN
    ALTER TABLE personas VALIDATE CONSTRAINT personas_image_mode_chk;
    ALTER TABLE personas VALIDATE CONSTRAINT personas_image_profile_chk;
    ALTER TABLE personas VALIDATE CONSTRAINT personas_layer6_bounds_chk;
  END IF;
END $$;
