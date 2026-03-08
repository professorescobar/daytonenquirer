-- Phase 5: Draft Writing DB contract
-- Safe to run multiple times.
-- Non-destructive: this migration does not delete legacy rows.

-- 0) Duplicate-safe canonicalization for signal-linked draft packages.
-- Keep newest canonical row per signal_id as artifact_type='draft_package'.
-- Re-label older duplicates to legacy artifact_type so unique index creation is deterministic.
WITH ranked AS (
  SELECT
    id,
    signal_id,
    ROW_NUMBER() OVER (
      PARTITION BY signal_id
      ORDER BY created_at DESC, id DESC
    ) AS rn
  FROM research_artifacts
  WHERE stage = 'draft_writing'
    AND artifact_type = 'draft_package'
    AND signal_id IS NOT NULL
)
UPDATE research_artifacts ra
SET
  artifact_type = 'draft_package_legacy',
  metadata = jsonb_set(
    COALESCE(
      CASE WHEN jsonb_typeof(ra.metadata) = 'object' THEN ra.metadata ELSE '{}'::jsonb END,
      '{}'::jsonb
    ),
    '{migration_20260308_16}',
    jsonb_build_object(
      'reason', 'duplicate_signal_draft_package',
      'demoted_from', 'draft_package',
      'updated_at', NOW()::text
    ),
    true
  )
FROM ranked r
WHERE ra.id = r.id
  AND r.rn > 1;

-- 1) Ensure one draft-package artifact per signal for canonical signal-linked rows.
CREATE UNIQUE INDEX IF NOT EXISTS uq_research_artifacts_draft_package_signal
  ON research_artifacts (signal_id)
  WHERE stage = 'draft_writing'
    AND artifact_type = 'draft_package'
    AND signal_id IS NOT NULL;

-- 2) Fast retrieval path for latest draft package by signal.
CREATE INDEX IF NOT EXISTS idx_research_artifacts_draft_package_signal_created
  ON research_artifacts (signal_id, created_at DESC)
  WHERE stage = 'draft_writing'
    AND artifact_type = 'draft_package'
    AND signal_id IS NOT NULL;

-- 3) Backfill shape for signal-linked rows where safe.
UPDATE research_artifacts
SET source_url = format('signal://%s/draft', signal_id)
WHERE stage = 'draft_writing'
  AND artifact_type = 'draft_package'
  AND signal_id IS NOT NULL
  AND (
    source_url IS NULL
    OR trim(source_url) = ''
    OR source_url NOT LIKE 'signal://%/draft'
  );

UPDATE research_artifacts
SET metadata = '{}'::jsonb
WHERE stage = 'draft_writing'
  AND artifact_type = 'draft_package'
  AND signal_id IS NOT NULL
  AND (metadata IS NULL OR jsonb_typeof(metadata) <> 'object');

UPDATE research_artifacts
SET metadata = jsonb_set(
  metadata,
  '{draft}',
  CASE
    WHEN jsonb_typeof(metadata->'draft') = 'object' THEN metadata->'draft'
    ELSE '{}'::jsonb
  END,
  true
)
WHERE stage = 'draft_writing'
  AND artifact_type = 'draft_package'
  AND signal_id IS NOT NULL;

UPDATE research_artifacts
SET content = COALESCE(
  NULLIF(trim(content), ''),
  NULLIF(trim(COALESCE(metadata->'draft'->>'body', '')), ''),
  'Draft content unavailable.'
)
WHERE stage = 'draft_writing'
  AND artifact_type = 'draft_package'
  AND signal_id IS NOT NULL
  AND (content IS NULL OR trim(content) = '');

UPDATE research_artifacts
SET metadata = jsonb_set(
  metadata,
  '{draft,sourceUrls}',
  CASE
    WHEN jsonb_typeof(metadata->'draft'->'sourceUrls') = 'array' THEN metadata->'draft'->'sourceUrls'
    ELSE '[]'::jsonb
  END,
  true
)
WHERE stage = 'draft_writing'
  AND artifact_type = 'draft_package'
  AND signal_id IS NOT NULL;

UPDATE research_artifacts
SET metadata = jsonb_set(
  metadata,
  '{draft,uncertaintyNotes}',
  CASE
    WHEN jsonb_typeof(metadata->'draft'->'uncertaintyNotes') = 'array' THEN metadata->'draft'->'uncertaintyNotes'
    ELSE '[]'::jsonb
  END,
  true
)
WHERE stage = 'draft_writing'
  AND artifact_type = 'draft_package'
  AND signal_id IS NOT NULL;

UPDATE research_artifacts
SET metadata = jsonb_set(
  metadata,
  '{draft,coverageGaps}',
  CASE
    WHEN jsonb_typeof(metadata->'draft'->'coverageGaps') = 'array' THEN metadata->'draft'->'coverageGaps'
    ELSE '[]'::jsonb
  END,
  true
)
WHERE stage = 'draft_writing'
  AND artifact_type = 'draft_package'
  AND signal_id IS NOT NULL;

-- 4) Add contract constraint as NOT VALID. Validate only when all existing canonical rows conform.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'research_artifacts_draft_package_contract_chk'
  ) THEN
    ALTER TABLE research_artifacts
      ADD CONSTRAINT research_artifacts_draft_package_contract_chk
      CHECK (
        stage <> 'draft_writing'
        OR artifact_type <> 'draft_package'
        OR (
          signal_id IS NOT NULL
          AND source_url IS NOT NULL
          AND source_url LIKE 'signal://%/draft'
          AND length(trim(COALESCE(content, ''))) > 0
          AND jsonb_typeof(metadata) = 'object'
          AND jsonb_typeof(metadata->'draft') = 'object'
          AND jsonb_typeof(metadata->'draft'->'sourceUrls') = 'array'
          AND jsonb_typeof(metadata->'draft'->'uncertaintyNotes') = 'array'
          AND jsonb_typeof(metadata->'draft'->'coverageGaps') = 'array'
        )
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM research_artifacts
    WHERE stage = 'draft_writing'
      AND artifact_type = 'draft_package'
      AND NOT (
        signal_id IS NOT NULL
        AND source_url IS NOT NULL
        AND source_url LIKE 'signal://%/draft'
        AND length(trim(COALESCE(content, ''))) > 0
        AND jsonb_typeof(metadata) = 'object'
        AND jsonb_typeof(metadata->'draft') = 'object'
        AND jsonb_typeof(metadata->'draft'->'sourceUrls') = 'array'
        AND jsonb_typeof(metadata->'draft'->'uncertaintyNotes') = 'array'
        AND jsonb_typeof(metadata->'draft'->'coverageGaps') = 'array'
      )
  ) THEN
    ALTER TABLE research_artifacts
      VALIDATE CONSTRAINT research_artifacts_draft_package_contract_chk;
  END IF;
END $$;
