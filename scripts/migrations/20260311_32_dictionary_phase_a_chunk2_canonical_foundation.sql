-- Phase A / Chunk 2: canonical foundation tables
-- Safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS dictionary.dictionary_jurisdictions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  jurisdiction_type TEXT NOT NULL,
  parent_jurisdiction_id UUID REFERENCES dictionary.dictionary_jurisdictions(id),
  centroid_lat DOUBLE PRECISION,
  centroid_lng DOUBLE PRECISION,
  bbox JSONB,
  geojson JSONB,
  status dictionary.canonical_record_status NOT NULL DEFAULT 'active',
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_jurisdictions_name_nonempty_chk
    CHECK (length(trim(name)) > 0),
  CONSTRAINT dictionary_jurisdictions_type_nonempty_chk
    CHECK (length(trim(jurisdiction_type)) > 0),
  CONSTRAINT dictionary_jurisdictions_centroid_lat_chk
    CHECK (centroid_lat IS NULL OR (centroid_lat >= -90 AND centroid_lat <= 90)),
  CONSTRAINT dictionary_jurisdictions_centroid_lng_chk
    CHECK (centroid_lng IS NULL OR (centroid_lng >= -180 AND centroid_lng <= 180)),
  CONSTRAINT dictionary_jurisdictions_centroid_pair_chk
    CHECK (
      (centroid_lat IS NULL AND centroid_lng IS NULL)
      OR (centroid_lat IS NOT NULL AND centroid_lng IS NOT NULL)
    ),
  CONSTRAINT dictionary_jurisdictions_bbox_type_chk
    CHECK (bbox IS NULL OR jsonb_typeof(bbox) = 'object'),
  CONSTRAINT dictionary_jurisdictions_geojson_type_chk
    CHECK (geojson IS NULL OR jsonb_typeof(geojson) = 'object'),
  CONSTRAINT dictionary_jurisdictions_parent_not_self_chk
    CHECK (parent_jurisdiction_id IS NULL OR parent_jurisdiction_id <> id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_jurisdictions_name_type_parent
  ON dictionary.dictionary_jurisdictions (
    lower(name),
    lower(jurisdiction_type),
    COALESCE(parent_jurisdiction_id, '00000000-0000-0000-0000-000000000000'::uuid)
  );

CREATE INDEX IF NOT EXISTS idx_dictionary_jurisdictions_parent_status
  ON dictionary.dictionary_jurisdictions (parent_jurisdiction_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_entities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  canonical_name TEXT NOT NULL,
  slug TEXT NOT NULL,
  primary_jurisdiction_id UUID REFERENCES dictionary.dictionary_jurisdictions(id),
  normalized_address TEXT,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  spatial_confidence DOUBLE PRECISION,
  status dictionary.canonical_record_status NOT NULL DEFAULT 'active',
  description TEXT,
  notes TEXT,
  attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_entities_type_nonempty_chk
    CHECK (length(trim(entity_type)) > 0),
  CONSTRAINT dictionary_entities_canonical_name_nonempty_chk
    CHECK (length(trim(canonical_name)) > 0),
  CONSTRAINT dictionary_entities_slug_nonempty_chk
    CHECK (length(trim(slug)) > 0),
  CONSTRAINT dictionary_entities_normalized_address_nonempty_chk
    CHECK (normalized_address IS NULL OR length(trim(normalized_address)) > 0),
  CONSTRAINT dictionary_entities_lat_chk
    CHECK (lat IS NULL OR (lat >= -90 AND lat <= 90)),
  CONSTRAINT dictionary_entities_lng_chk
    CHECK (lng IS NULL OR (lng >= -180 AND lng <= 180)),
  CONSTRAINT dictionary_entities_lat_lng_pair_chk
    CHECK (
      (lat IS NULL AND lng IS NULL)
      OR (lat IS NOT NULL AND lng IS NOT NULL)
    ),
  CONSTRAINT dictionary_entities_spatial_confidence_chk
    CHECK (spatial_confidence IS NULL OR (spatial_confidence >= 0 AND spatial_confidence <= 1)),
  CONSTRAINT dictionary_entities_spatial_requires_jurisdiction_chk
    CHECK (
      (
        lat IS NULL
        AND lng IS NULL
        AND spatial_confidence IS NULL
      )
      OR primary_jurisdiction_id IS NOT NULL
    ),
  CONSTRAINT dictionary_entities_attributes_object_chk
    CHECK (jsonb_typeof(attributes) = 'object')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_entities_slug
  ON dictionary.dictionary_entities (lower(slug));

CREATE INDEX IF NOT EXISTS idx_dictionary_entities_jurisdiction_status
  ON dictionary.dictionary_entities (primary_jurisdiction_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_entities_status_verified
  ON dictionary.dictionary_entities (status, last_verified_at DESC NULLS LAST, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_entities_type_name
  ON dictionary.dictionary_entities (lower(entity_type), lower(canonical_name));

CREATE TABLE IF NOT EXISTS dictionary.dictionary_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL REFERENCES dictionary.dictionary_entities(id),
  alias TEXT NOT NULL,
  alias_type TEXT NOT NULL,
  status dictionary.canonical_record_status NOT NULL DEFAULT 'active',
  effective_start_at TIMESTAMPTZ,
  effective_end_at TIMESTAMPTZ,
  source_count INTEGER NOT NULL DEFAULT 0,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_aliases_alias_nonempty_chk
    CHECK (length(trim(alias)) > 0),
  CONSTRAINT dictionary_aliases_type_nonempty_chk
    CHECK (length(trim(alias_type)) > 0),
  CONSTRAINT dictionary_aliases_source_count_nonnegative_chk
    CHECK (source_count >= 0),
  CONSTRAINT dictionary_aliases_effective_window_chk
    CHECK (
      effective_end_at IS NULL
      OR effective_start_at IS NULL
      OR effective_end_at >= effective_start_at
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_aliases_entity_alias_type_window
  ON dictionary.dictionary_aliases (
    entity_id,
    lower(alias),
    lower(alias_type),
    COALESCE(effective_start_at, '-infinity'::timestamptz),
    COALESCE(effective_end_at, 'infinity'::timestamptz)
  );

CREATE INDEX IF NOT EXISTS idx_dictionary_aliases_entity_status
  ON dictionary.dictionary_aliases (entity_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dictionary_aliases_alias_lookup
  ON dictionary.dictionary_aliases (lower(alias), status, last_verified_at DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS dictionary.dictionary_roles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  role_name TEXT NOT NULL,
  role_type TEXT NOT NULL,
  jurisdiction_id UUID REFERENCES dictionary.dictionary_jurisdictions(id),
  status dictionary.canonical_record_status NOT NULL DEFAULT 'active',
  notes TEXT,
  term_pattern JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT dictionary_roles_name_nonempty_chk
    CHECK (length(trim(role_name)) > 0),
  CONSTRAINT dictionary_roles_type_nonempty_chk
    CHECK (length(trim(role_type)) > 0),
  CONSTRAINT dictionary_roles_term_pattern_object_chk
    CHECK (jsonb_typeof(term_pattern) = 'object')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dictionary_roles_name_type_jurisdiction
  ON dictionary.dictionary_roles (
    lower(role_name),
    lower(role_type),
    COALESCE(jurisdiction_id, '00000000-0000-0000-0000-000000000000'::uuid)
  );

CREATE INDEX IF NOT EXISTS idx_dictionary_roles_jurisdiction_status
  ON dictionary.dictionary_roles (jurisdiction_id, status, updated_at DESC);

COMMENT ON TABLE dictionary.dictionary_jurisdictions IS
  'Canonical coverage objects for cities, counties, neighborhoods, districts, and other structured geography used for deterministic locality checks.';

COMMENT ON TABLE dictionary.dictionary_entities IS
  'Canonical durable entities. This is substrate build-state only and is not newsroom-readable until incorporated into a published snapshot.';

COMMENT ON TABLE dictionary.dictionary_aliases IS
  'Canonical alias memory for entities, including shorthand, abbreviations, prior names, and spelling variants.';

COMMENT ON TABLE dictionary.dictionary_roles IS
  'Canonical role and office definitions, separated from time-bound officeholding assertions.';
