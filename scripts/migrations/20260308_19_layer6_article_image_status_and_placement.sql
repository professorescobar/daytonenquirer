-- Phase 6: article image status + placement contract
-- Safe to run multiple times. Non-destructive backfill.

DO $$
BEGIN
  IF to_regclass('public.articles') IS NULL THEN
    RETURN;
  END IF;

  ALTER TABLE articles ADD COLUMN IF NOT EXISTS image_status TEXT;
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS image_status_changed_at TIMESTAMP;
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS render_class TEXT;
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS placement_eligible JSONB;
END $$;

DO $$
BEGIN
  IF to_regclass('public.articles') IS NULL THEN
    RETURN;
  END IF;

  UPDATE articles
  SET
    image_status = CASE
      WHEN trim(COALESCE(image, '')) <> '' THEN 'with_image'
      ELSE 'text_only'
    END
  WHERE image_status IS NULL
     OR image_status NOT IN ('with_image', 'text_only');

  UPDATE articles
  SET image_status_changed_at = COALESCE(pub_date, updated_at, created_at, NOW())
  WHERE image_status_changed_at IS NULL;

  UPDATE articles
  SET
    render_class = CASE
      WHEN image_status = 'with_image' THEN 'with_image'
      ELSE 'text_only'
    END
  WHERE render_class IS NULL
     OR render_class NOT IN ('with_image', 'text_only');

  UPDATE articles
  SET placement_eligible = CASE
    WHEN image_status = 'with_image'
      THEN '["main","top","carousel","grid","sidebar","extra_headlines"]'::jsonb
    ELSE '["sidebar","extra_headlines"]'::jsonb
  END
  WHERE placement_eligible IS NULL
     OR jsonb_typeof(placement_eligible) <> 'array';

  ALTER TABLE articles ALTER COLUMN image_status SET DEFAULT 'text_only';
  ALTER TABLE articles ALTER COLUMN render_class SET DEFAULT 'text_only';
  ALTER TABLE articles ALTER COLUMN placement_eligible SET DEFAULT '["sidebar","extra_headlines"]'::jsonb;

  IF NOT EXISTS (
    SELECT 1
    FROM articles
    WHERE image_status IS NULL
       OR render_class IS NULL
       OR placement_eligible IS NULL
  ) THEN
    ALTER TABLE articles ALTER COLUMN image_status SET NOT NULL;
    ALTER TABLE articles ALTER COLUMN render_class SET NOT NULL;
    ALTER TABLE articles ALTER COLUMN placement_eligible SET NOT NULL;
  END IF;
END $$;

DO $$
BEGIN
  IF to_regclass('public.articles') IS NULL THEN
    RETURN;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'articles_image_status_chk'
  ) THEN
    ALTER TABLE articles
      ADD CONSTRAINT articles_image_status_chk
      CHECK (image_status IN ('with_image', 'text_only')) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'articles_render_class_chk'
  ) THEN
    ALTER TABLE articles
      ADD CONSTRAINT articles_render_class_chk
      CHECK (render_class IN ('with_image', 'text_only')) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'articles_placement_eligible_array_chk'
  ) THEN
    ALTER TABLE articles
      ADD CONSTRAINT articles_placement_eligible_array_chk
      CHECK (jsonb_typeof(placement_eligible) = 'array') NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'articles_text_only_placement_chk'
  ) THEN
    ALTER TABLE articles
      ADD CONSTRAINT articles_text_only_placement_chk
      CHECK (
        image_status <> 'text_only'
        OR (
          placement_eligible <@ '["sidebar","extra_headlines"]'::jsonb
          AND placement_eligible @> '["sidebar"]'::jsonb
        )
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'articles_text_only_image_empty_chk'
  ) THEN
    ALTER TABLE articles
      ADD CONSTRAINT articles_text_only_image_empty_chk
      CHECK (
        image_status <> 'text_only'
        OR length(trim(COALESCE(image, ''))) = 0
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'articles_with_image_has_image_chk'
  ) THEN
    ALTER TABLE articles
      ADD CONSTRAINT articles_with_image_has_image_chk
      CHECK (
        image_status <> 'with_image'
        OR length(trim(COALESCE(image, ''))) > 0
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM articles
    WHERE image_status NOT IN ('with_image', 'text_only')
       OR render_class NOT IN ('with_image', 'text_only')
       OR jsonb_typeof(placement_eligible) <> 'array'
       OR (
         image_status = 'text_only'
         AND NOT (
           placement_eligible <@ '["sidebar","extra_headlines"]'::jsonb
           AND placement_eligible @> '["sidebar"]'::jsonb
         )
       )
       OR (
         image_status = 'text_only'
         AND length(trim(COALESCE(image, ''))) > 0
       )
       OR (
         image_status = 'with_image'
         AND length(trim(COALESCE(image, ''))) = 0
       )
  ) THEN
    ALTER TABLE articles VALIDATE CONSTRAINT articles_image_status_chk;
    ALTER TABLE articles VALIDATE CONSTRAINT articles_render_class_chk;
    ALTER TABLE articles VALIDATE CONSTRAINT articles_placement_eligible_array_chk;
    ALTER TABLE articles VALIDATE CONSTRAINT articles_text_only_placement_chk;
    ALTER TABLE articles VALIDATE CONSTRAINT articles_text_only_image_empty_chk;
    ALTER TABLE articles VALIDATE CONSTRAINT articles_with_image_has_image_chk;
  END IF;
END $$;
