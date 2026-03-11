-- Phase A / Chunk 4: snapshot foundation + published read boundary
-- Safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS dictionary.dictionary_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  version BIGINT NOT NULL,
  status dictionary.snapshot_status NOT NULL DEFAULT 'building',
  substrate_run_id UUID NOT NULL,
  entity_count BIGINT NOT NULL DEFAULT 0,
  assertion_count BIGINT NOT NULL DEFAULT 0,
  alias_count BIGINT NOT NULL DEFAULT 0,
  change_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ,

  CONSTRAINT dictionary_snapshots_version_positive_chk
    CHECK (version >= 1),
  CONSTRAINT dictionary_snapshots_entity_count_nonnegative_chk
    CHECK (entity_count >= 0),
  CONSTRAINT dictionary_snapshots_assertion_count_nonnegative_chk
    CHECK (assertion_count >= 0),
  CONSTRAINT dictionary_snapshots_alias_count_nonnegative_chk
    CHECK (alias_count >= 0),
  CONSTRAINT dictionary_snapshots_change_summary_object_chk
    CHECK (jsonb_typeof(change_summary) = 'object'),
  CONSTRAINT dictionary_snapshots_published_at_status_chk
    CHECK (
      (status = 'building' AND published_at IS NULL)
      OR (status IN ('published', 'superseded', 'rolled_back') AND published_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_snapshots_version
  ON dictionary.dictionary_snapshots (version);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshots_status_published
  ON dictionary.dictionary_snapshots (status, published_at DESC NULLS LAST, created_at DESC);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_snapshot_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_id UUID NOT NULL REFERENCES dictionary.dictionary_snapshots(id),
  record_type TEXT NOT NULL,
  record_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_snapshot_records_record_type_chk
    CHECK (record_type IN ('entity', 'alias', 'role', 'assertion', 'jurisdiction')),
  CONSTRAINT dictionary_snapshot_records_unique_membership_chk
    UNIQUE (snapshot_id, record_type, record_id)
);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_records_lookup
  ON dictionary.dictionary_snapshot_records (snapshot_id, record_type, record_id);

CREATE INDEX IF NOT EXISTS idx_dictionary_snapshot_records_record
  ON dictionary.dictionary_snapshot_records (record_type, record_id, snapshot_id);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_active_snapshot (
  slot TEXT PRIMARY KEY,
  snapshot_id UUID NOT NULL UNIQUE REFERENCES dictionary.dictionary_snapshots(id),
  activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_active_snapshot_slot_chk
    CHECK (slot = 'newsroom')
);

CREATE OR REPLACE FUNCTION dictionary.enforce_published_active_snapshot()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  snapshot_status dictionary.snapshot_status;
BEGIN
  SELECT status
  INTO snapshot_status
  FROM dictionary.dictionary_snapshots
  WHERE id = NEW.snapshot_id;

  IF snapshot_status IS DISTINCT FROM 'published' THEN
    RAISE EXCEPTION 'active newsroom snapshot must reference a published snapshot';
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.guard_immutable_snapshot_rows()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.status <> 'building' THEN
      RAISE EXCEPTION 'published snapshot rows are immutable';
    END IF;
    RETURN OLD;
  END IF;

  IF OLD.status = 'building' THEN
    RETURN NEW;
  END IF;

  IF NEW.id <> OLD.id
     OR NEW.version <> OLD.version
     OR NEW.substrate_run_id <> OLD.substrate_run_id
     OR NEW.entity_count <> OLD.entity_count
     OR NEW.assertion_count <> OLD.assertion_count
     OR NEW.alias_count <> OLD.alias_count
     OR NEW.change_summary IS DISTINCT FROM OLD.change_summary
     OR NEW.created_at <> OLD.created_at
  THEN
    RAISE EXCEPTION 'published snapshot payload fields are immutable';
  END IF;

  IF NEW.published_at IS DISTINCT FROM OLD.published_at THEN
    IF OLD.published_at IS NOT NULL THEN
      RAISE EXCEPTION 'published_at is immutable once set';
    END IF;

    IF NEW.status NOT IN ('published', 'superseded', 'rolled_back') THEN
      RAISE EXCEPTION 'published_at may only be set for published snapshot lifecycle states';
    END IF;
  END IF;

  IF OLD.status = 'published' AND NEW.status NOT IN ('published', 'superseded', 'rolled_back') THEN
    RAISE EXCEPTION 'published snapshots may only transition to published, superseded, or rolled_back';
  END IF;

  IF OLD.status IN ('superseded', 'rolled_back') AND NEW.status <> OLD.status THEN
    RAISE EXCEPTION 'terminal snapshot lifecycle states are immutable';
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION dictionary.guard_snapshot_record_membership()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  target_snapshot_id UUID;
  snapshot_status dictionary.snapshot_status;
BEGIN
  target_snapshot_id := COALESCE(NEW.snapshot_id, OLD.snapshot_id);

  SELECT status
  INTO snapshot_status
  FROM dictionary.dictionary_snapshots
  WHERE id = target_snapshot_id;

  IF snapshot_status IS DISTINCT FROM 'building' THEN
    RAISE EXCEPTION 'snapshot membership is mutable only while the snapshot is building';
  END IF;

  RETURN COALESCE(NEW, OLD);
END;
$$;

DROP TRIGGER IF EXISTS trg_dictionary_active_snapshot_published_only
  ON dictionary.dictionary_active_snapshot;

CREATE TRIGGER trg_dictionary_active_snapshot_published_only
  BEFORE INSERT OR UPDATE ON dictionary.dictionary_active_snapshot
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.enforce_published_active_snapshot();

DROP TRIGGER IF EXISTS trg_dictionary_snapshots_immutable
  ON dictionary.dictionary_snapshots;

CREATE TRIGGER trg_dictionary_snapshots_immutable
  BEFORE UPDATE OR DELETE ON dictionary.dictionary_snapshots
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.guard_immutable_snapshot_rows();

DROP TRIGGER IF EXISTS trg_dictionary_snapshot_records_guard
  ON dictionary.dictionary_snapshot_records;

CREATE TRIGGER trg_dictionary_snapshot_records_guard
  BEFORE INSERT OR UPDATE OR DELETE ON dictionary.dictionary_snapshot_records
  FOR EACH ROW
  EXECUTE FUNCTION dictionary.guard_snapshot_record_membership();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dictionary_assertions_snapshot_id_fkey'
      AND conrelid = 'dictionary.dictionary_assertions'::regclass
  ) THEN
    ALTER TABLE dictionary.dictionary_assertions
      ADD CONSTRAINT dictionary_assertions_snapshot_id_fkey
      FOREIGN KEY (snapshot_id)
      REFERENCES dictionary.dictionary_snapshots(id);
  END IF;
END $$;

CREATE OR REPLACE VIEW dictionary.active_snapshot_metadata AS
SELECT
  s.id,
  s.version,
  s.status,
  s.substrate_run_id,
  s.entity_count,
  s.assertion_count,
  s.alias_count,
  s.change_summary,
  s.created_at,
  s.published_at,
  a.activated_at
FROM dictionary.dictionary_active_snapshot a
JOIN dictionary.dictionary_snapshots s
  ON s.id = a.snapshot_id
WHERE a.slot = 'newsroom'
  AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_entities AS
SELECT e.*
FROM dictionary.dictionary_entities e
JOIN dictionary.dictionary_snapshot_records sr
  ON sr.record_type = 'entity'
 AND sr.record_id = e.id
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sr.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sr.snapshot_id
 AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_aliases AS
SELECT da.*
FROM dictionary.dictionary_aliases da
JOIN dictionary.dictionary_snapshot_records sr
  ON sr.record_type = 'alias'
 AND sr.record_id = da.id
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sr.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sr.snapshot_id
 AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_roles AS
SELECT r.*
FROM dictionary.dictionary_roles r
JOIN dictionary.dictionary_snapshot_records sr
  ON sr.record_type = 'role'
 AND sr.record_id = r.id
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sr.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sr.snapshot_id
 AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_jurisdictions AS
SELECT j.*
FROM dictionary.dictionary_jurisdictions j
JOIN dictionary.dictionary_snapshot_records sr
  ON sr.record_type = 'jurisdiction'
 AND sr.record_id = j.id
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sr.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sr.snapshot_id
 AND s.status = 'published';

CREATE OR REPLACE VIEW dictionary.published_assertions AS
SELECT da.*
FROM dictionary.dictionary_assertions da
JOIN dictionary.dictionary_snapshot_records sr
  ON sr.record_type = 'assertion'
 AND sr.record_id = da.id
JOIN dictionary.dictionary_active_snapshot a
  ON a.slot = 'newsroom'
 AND a.snapshot_id = sr.snapshot_id
JOIN dictionary.dictionary_snapshots s
  ON s.id = sr.snapshot_id
 AND s.status = 'published'
WHERE da.snapshot_id = sr.snapshot_id;

COMMENT ON TABLE dictionary.dictionary_snapshots IS
  'Immutable published snapshot metadata. Newsroom-visible dictionary state is released through snapshots, not canonical head tables.';

COMMENT ON TABLE dictionary.dictionary_snapshot_records IS
  'Snapshot membership ledger for canonical records. Presence here is the publication boundary for newsroom-readable state.';

COMMENT ON TABLE dictionary.dictionary_active_snapshot IS
  'Single active published-snapshot pointer for newsroom reads. Rollback changes this pointer instead of rewriting historical records.';

COMMENT ON VIEW dictionary.active_snapshot_metadata IS
  'Read-safe metadata for the active newsroom snapshot.';

COMMENT ON VIEW dictionary.published_entities IS
  'Newsroom-readable entities scoped to the active published snapshot only.';

COMMENT ON VIEW dictionary.published_aliases IS
  'Newsroom-readable aliases scoped to the active published snapshot only.';

COMMENT ON VIEW dictionary.published_roles IS
  'Newsroom-readable roles scoped to the active published snapshot only.';

COMMENT ON VIEW dictionary.published_jurisdictions IS
  'Newsroom-readable jurisdictions scoped to the active published snapshot only.';

COMMENT ON VIEW dictionary.published_assertions IS
  'Newsroom-readable assertions scoped to the active published snapshot only.';
