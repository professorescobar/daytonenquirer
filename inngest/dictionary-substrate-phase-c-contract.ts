import { createHash } from "node:crypto";

export const PHASE_C_EXTRACTION_VERSION = "phase_c_contract_v1";

export const ENTITY_KINDS = [
  "person",
  "organization",
  "government_body",
  "institution",
  "place",
  "venue"
] as const;

export const ALIAS_KINDS = [
  "short_name",
  "alternate_name",
  "abbreviation",
  "historical_name"
] as const;

export const ROLE_KINDS = [
  "elected_office",
  "appointed_office",
  "board_role",
  "institutional_role"
] as const;

export const JURISDICTION_TYPES = [
  "city",
  "county",
  "township",
  "village",
  "school_district",
  "neighborhood",
  "district",
  "state",
  "country"
] as const;

export const DIAGNOSTIC_TYPES = [
  "generic_reference_rejected",
  "schema_invalid",
  "unsupported_candidate",
  "empty_extraction",
  "normalization_adjustment"
] as const;

export const DIAGNOSTIC_SEVERITIES = ["low", "medium", "high"] as const;

export const REJECTION_REASONS = [
  "generic_reference",
  "missing_required_field",
  "invalid_normalized_value",
  "missing_evidence",
  "unsupported_candidate_type",
  "non_authoritative_jurisdiction_hint"
] as const;

type EntityKind = (typeof ENTITY_KINDS)[number];
type AliasKind = (typeof ALIAS_KINDS)[number];
type RoleKind = (typeof ROLE_KINDS)[number];
type JurisdictionType = (typeof JURISDICTION_TYPES)[number];
type DiagnosticType = (typeof DIAGNOSTIC_TYPES)[number];
type DiagnosticSeverity = (typeof DIAGNOSTIC_SEVERITIES)[number];

export type CandidateType =
  | "entity"
  | "alias"
  | "role"
  | "assertion"
  | "jurisdiction"
  | "diagnostic";

export type JurisdictionHint = {
  label: string;
  scopeType?: string | null;
  stateCode?: string | null;
  countryCode?: string | null;
  isAuthoritative: false;
};

export type EntityCandidatePayload = {
  canonical_name: string;
  entity_kind: EntityKind;
  jurisdiction_hint: JurisdictionHint | null;
  description: string | null;
  source_mentions: string[];
};

export type AliasCandidatePayload = {
  alias_name: string;
  target_canonical_name: string;
  alias_kind: AliasKind;
  jurisdiction_hint: JurisdictionHint | null;
  source_mentions: string[];
};

export type RoleCandidatePayload = {
  role_name: string;
  role_kind: RoleKind;
  governing_body_name: string | null;
  jurisdiction_hint: JurisdictionHint | null;
  is_time_bounded: boolean;
};

export type AssertionCandidatePayload = {
  assertion_type: string;
  subject_canonical_name: string;
  object_canonical_name: string | null;
  role_name: string | null;
  effective_start_at: string | null;
  effective_end_at: string | null;
  term_end_at: string | null;
  observed_at: string | null;
  assertion_confidence: number | null;
  is_time_sensitive: boolean;
};

export type JurisdictionCandidatePayload = {
  canonical_name: string;
  jurisdiction_type: JurisdictionType;
  parent_jurisdiction_name: string | null;
  state_code: string | null;
  country_code: string | null;
  source_mentions: string[];
};

export type DiagnosticCandidatePayload = {
  diagnostic_type: DiagnosticType;
  severity: DiagnosticSeverity;
  message: string;
  related_candidate_key: string | null;
  details: Record<string, unknown>;
};

export type ExtractionCandidatePayloadByType = {
  entity: EntityCandidatePayload;
  alias: AliasCandidatePayload;
  role: RoleCandidatePayload;
  assertion: AssertionCandidatePayload;
  jurisdiction: JurisdictionCandidatePayload;
  diagnostic: DiagnosticCandidatePayload;
};

function cleanText(value: unknown, max = 500): string {
  return String(value || "").trim().slice(0, max);
}

function cleanOptionalText(value: unknown, max = 500): string | null {
  const cleaned = cleanText(value, max);
  return cleaned || null;
}

function requireText(value: unknown, fieldName: string, max = 500): string {
  const cleaned = cleanText(value, max);
  if (!cleaned) {
    throw new Error(`${fieldName} is required`);
  }
  return cleaned;
}

function normalizeToken(value: unknown): string {
  return cleanText(value, 200)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function requireEnumValue<T extends readonly string[]>(
  value: unknown,
  allowed: T,
  fieldName: string
): T[number] {
  const normalized = normalizeToken(value);
  if ((allowed as readonly string[]).includes(normalized)) {
    return normalized as T[number];
  }
  throw new Error(`Invalid ${fieldName}: ${String(value || "").slice(0, 120)}`);
}

function normalizeStringArray(value: unknown, fieldName: string): string[] {
  if (!Array.isArray(value)) {
    throw new Error(`${fieldName} must be an array`);
  }
  const normalized = value.map((item) => cleanText(item, 240)).filter(Boolean);
  if (!normalized.length) {
    throw new Error(`${fieldName} must contain at least one value`);
  }
  return normalized;
}

function normalizeIsoOrNull(value: unknown): string | null {
  const cleaned = cleanOptionalText(value, 80);
  if (!cleaned) return null;
  const parsed = Date.parse(cleaned);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid ISO datetime: ${cleaned}`);
  }
  return new Date(parsed).toISOString();
}

export function normalizeJurisdictionHint(value: unknown): JurisdictionHint | null {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "string") {
    const label = cleanText(value, 240);
    if (!label) return null;
    return {
      label,
      scopeType: null,
      stateCode: null,
      countryCode: null,
      isAuthoritative: false
    };
  }

  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("jurisdiction_hint must be null, string, or object");
  }

  const raw = value as Record<string, unknown>;
  const label = cleanText(raw.label, 240);
  if (!label) {
    throw new Error("jurisdiction_hint.label is required when jurisdiction_hint is an object");
  }
  const rawAuthoritative = raw.isAuthoritative ?? raw.is_authoritative;
  if (rawAuthoritative === true || String(rawAuthoritative || "").trim().toLowerCase() === "true") {
    throw new Error("jurisdiction_hint must not claim authoritative status");
  }

  return {
    label,
    scopeType: cleanOptionalText(raw.scopeType ?? raw.scope_type, 120),
    stateCode: cleanOptionalText(raw.stateCode ?? raw.state_code, 12),
    countryCode: cleanOptionalText(raw.countryCode ?? raw.country_code, 12),
    isAuthoritative: false
  };
}

export function normalizeEntityCandidatePayload(value: unknown): EntityCandidatePayload {
  const raw = value as Record<string, unknown>;
  return {
    canonical_name: requireText(raw.canonical_name, "entity.canonical_name", 240),
    entity_kind: requireEnumValue(raw.entity_kind, ENTITY_KINDS, "entity_kind"),
    jurisdiction_hint: normalizeJurisdictionHint(raw.jurisdiction_hint),
    description: cleanOptionalText(raw.description, 600),
    source_mentions: normalizeStringArray(raw.source_mentions, "entity.source_mentions")
  };
}

export function normalizeAliasCandidatePayload(value: unknown): AliasCandidatePayload {
  const raw = value as Record<string, unknown>;
  return {
    alias_name: requireText(raw.alias_name, "alias.alias_name", 240),
    target_canonical_name: requireText(
      raw.target_canonical_name,
      "alias.target_canonical_name",
      240
    ),
    alias_kind: requireEnumValue(raw.alias_kind, ALIAS_KINDS, "alias_kind"),
    jurisdiction_hint: normalizeJurisdictionHint(raw.jurisdiction_hint),
    source_mentions: normalizeStringArray(raw.source_mentions, "alias.source_mentions")
  };
}

export function normalizeRoleCandidatePayload(value: unknown): RoleCandidatePayload {
  const raw = value as Record<string, unknown>;
  return {
    role_name: requireText(raw.role_name, "role.role_name", 240),
    role_kind: requireEnumValue(raw.role_kind, ROLE_KINDS, "role_kind"),
    governing_body_name: cleanOptionalText(raw.governing_body_name, 240),
    jurisdiction_hint: normalizeJurisdictionHint(raw.jurisdiction_hint),
    is_time_bounded: Boolean(raw.is_time_bounded)
  };
}

export function normalizeAssertionCandidatePayload(value: unknown): AssertionCandidatePayload {
  const raw = value as Record<string, unknown>;
  const subjectCanonicalName = requireText(
    raw.subject_canonical_name,
    "assertion.subject_canonical_name",
    240
  );
  const roleName = cleanOptionalText(raw.role_name, 240);
  const objectCanonicalName = cleanOptionalText(raw.object_canonical_name, 240);

  if (!roleName && !objectCanonicalName) {
    throw new Error("assertion candidate must include role_name or object_canonical_name");
  }

  return {
    assertion_type: requireText(raw.assertion_type, "assertion.assertion_type", 160),
    subject_canonical_name: subjectCanonicalName,
    object_canonical_name: objectCanonicalName,
    role_name: roleName,
    effective_start_at: normalizeIsoOrNull(raw.effective_start_at),
    effective_end_at: normalizeIsoOrNull(raw.effective_end_at),
    term_end_at: normalizeIsoOrNull(raw.term_end_at),
    observed_at: normalizeIsoOrNull(raw.observed_at),
    assertion_confidence: (() => {
      if (raw.assertion_confidence === null || raw.assertion_confidence === undefined) {
        return null;
      }
      const parsed = Number(raw.assertion_confidence);
      if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
        throw new Error("assertion.assertion_confidence must be between 0 and 1");
      }
      return parsed;
    })(),
    is_time_sensitive: Boolean(raw.is_time_sensitive)
  };
}

export function normalizeJurisdictionCandidatePayload(value: unknown): JurisdictionCandidatePayload {
  const raw = value as Record<string, unknown>;
  return {
    canonical_name: requireText(raw.canonical_name, "jurisdiction.canonical_name", 240),
    jurisdiction_type: requireEnumValue(
      raw.jurisdiction_type,
      JURISDICTION_TYPES,
      "jurisdiction_type"
    ),
    parent_jurisdiction_name: cleanOptionalText(raw.parent_jurisdiction_name, 240),
    state_code: cleanOptionalText(raw.state_code, 12),
    country_code: cleanOptionalText(raw.country_code, 12),
    source_mentions: normalizeStringArray(raw.source_mentions, "jurisdiction.source_mentions")
  };
}

export function normalizeDiagnosticCandidatePayload(value: unknown): DiagnosticCandidatePayload {
  const raw = value as Record<string, unknown>;
  const details = raw.details;
  if (!details || typeof details !== "object" || Array.isArray(details)) {
    throw new Error("diagnostic.details must be an object");
  }

  return {
    diagnostic_type: requireEnumValue(raw.diagnostic_type, DIAGNOSTIC_TYPES, "diagnostic_type"),
    severity: requireEnumValue(raw.severity, DIAGNOSTIC_SEVERITIES, "diagnostic.severity"),
    message: requireText(raw.message, "diagnostic.message", 1000),
    related_candidate_key: cleanOptionalText(raw.related_candidate_key, 160),
    details: details as Record<string, unknown>
  };
}

export function normalizeCandidatePayload<T extends CandidateType>(
  candidateType: T,
  value: unknown
): ExtractionCandidatePayloadByType[T] {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${candidateType} candidate payload must be an object`);
  }

  if (candidateType === "entity") {
    return normalizeEntityCandidatePayload(value) as ExtractionCandidatePayloadByType[T];
  }
  if (candidateType === "alias") {
    return normalizeAliasCandidatePayload(value) as ExtractionCandidatePayloadByType[T];
  }
  if (candidateType === "role") {
    return normalizeRoleCandidatePayload(value) as ExtractionCandidatePayloadByType[T];
  }
  if (candidateType === "assertion") {
    return normalizeAssertionCandidatePayload(value) as ExtractionCandidatePayloadByType[T];
  }
  if (candidateType === "jurisdiction") {
    return normalizeJurisdictionCandidatePayload(value) as ExtractionCandidatePayloadByType[T];
  }
  return normalizeDiagnosticCandidatePayload(value) as ExtractionCandidatePayloadByType[T];
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableJson(item)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
      a.localeCompare(b)
    );
    return `{${entries
      .map(([key, entryValue]) => `${JSON.stringify(key)}:${stableJson(entryValue)}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function buildHash(parts: unknown[]): string {
  return createHash("sha256").update(stableJson(parts)).digest("hex").slice(0, 24);
}

export function deriveCandidateKey<T extends CandidateType>(
  candidateType: T,
  payload: ExtractionCandidatePayloadByType[T]
): string {
  let seed: unknown[];

  if (candidateType === "entity") {
    const entityPayload = payload as EntityCandidatePayload;
    seed = [
      entityPayload.canonical_name.toLowerCase(),
      entityPayload.entity_kind,
      entityPayload.jurisdiction_hint
    ];
  } else if (candidateType === "alias") {
    const aliasPayload = payload as AliasCandidatePayload;
    seed = [
      aliasPayload.alias_name.toLowerCase(),
      aliasPayload.target_canonical_name.toLowerCase(),
      aliasPayload.alias_kind,
      aliasPayload.jurisdiction_hint
    ];
  } else if (candidateType === "role") {
    const rolePayload = payload as RoleCandidatePayload;
    seed = [
      rolePayload.role_name.toLowerCase(),
      rolePayload.role_kind,
      rolePayload.governing_body_name?.toLowerCase() || null,
      rolePayload.jurisdiction_hint,
      rolePayload.is_time_bounded
    ];
  } else if (candidateType === "assertion") {
    const assertionPayload = payload as AssertionCandidatePayload;
    seed = [
      assertionPayload.assertion_type.toLowerCase(),
      assertionPayload.subject_canonical_name.toLowerCase(),
      assertionPayload.object_canonical_name?.toLowerCase() || null,
      assertionPayload.role_name?.toLowerCase() || null,
      assertionPayload.effective_start_at,
      assertionPayload.effective_end_at,
      assertionPayload.term_end_at
    ];
  } else if (candidateType === "jurisdiction") {
    const jurisdictionPayload = payload as JurisdictionCandidatePayload;
    seed = [
      jurisdictionPayload.canonical_name.toLowerCase(),
      jurisdictionPayload.jurisdiction_type,
      jurisdictionPayload.parent_jurisdiction_name?.toLowerCase() || null,
      jurisdictionPayload.state_code?.toLowerCase() || null,
      jurisdictionPayload.country_code?.toLowerCase() || null
    ];
  } else {
    const diagnosticPayload = payload as DiagnosticCandidatePayload;
    seed = [
      diagnosticPayload.diagnostic_type,
      diagnosticPayload.severity,
      diagnosticPayload.related_candidate_key,
      diagnosticPayload.message
    ];
  }

  return `${candidateType}:${buildHash(seed)}`;
}
