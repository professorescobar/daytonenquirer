DO $$
BEGIN
  IF to_regclass('public.personas') IS NULL THEN
    RAISE NOTICE 'Skipping migration: personas table does not exist';
    RETURN;
  END IF;
END $$;

-- Adopt conservative defaults for new personas.
ALTER TABLE personas ALTER COLUMN quota_postgres_image_daily SET DEFAULT 2;
ALTER TABLE personas ALTER COLUMN quota_sourced_image_daily SET DEFAULT 2;
ALTER TABLE personas ALTER COLUMN quota_generated_image_daily SET DEFAULT 2;
ALTER TABLE personas ALTER COLUMN quota_text_only_daily SET DEFAULT 3;

-- Safety remap: only migrate exact legacy defaults to conservative values.
-- Custom persona limits remain untouched.
UPDATE personas
SET quota_postgres_image_daily = 2
WHERE quota_postgres_image_daily = 200;

UPDATE personas
SET quota_sourced_image_daily = 2
WHERE quota_sourced_image_daily = 120;

UPDATE personas
SET quota_generated_image_daily = 2
WHERE quota_generated_image_daily = 30;

UPDATE personas
SET quota_text_only_daily = 3
WHERE quota_text_only_daily = 400;
