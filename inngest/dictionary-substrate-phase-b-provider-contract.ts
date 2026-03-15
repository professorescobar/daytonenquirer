export const PHASE_B_PROVIDER_CONTRACT_VERSION = "phase_b_provider_contract_v1";
export const PHASE_B_PROVIDER_SCOPE = "single_document";

export const PHASE_B_PROVIDER_ERROR_CODES = [
  "provider_auth_failed",
  "provider_rate_limited",
  "provider_timeout",
  "provider_transport_failed",
  "provider_unavailable",
  "provider_invalid_response",
  "provider_capture_empty"
] as const;

export type PhaseBProviderErrorCode = (typeof PHASE_B_PROVIDER_ERROR_CODES)[number];

export type PhaseBProviderCaptureMaterials = {
  rawHtml: string | null;
  extractedText: string | null;
  binaryPayload: null;
};

export type PhaseBProviderCaptureMetadata = {
  providerName: string;
  providerVersion: string | null;
  requestedUrl: string;
  finalUrl: string | null;
  fetchedAt: string;
  httpStatus: number | null;
  contentType: string | null;
  durationMs: number | null;
  attemptCount: number;
  providerDetails: Record<string, unknown>;
};

export type PhaseBProviderCaptureSuccess = {
  ok: true;
  scope: typeof PHASE_B_PROVIDER_SCOPE;
  materials: PhaseBProviderCaptureMaterials;
  metadata: PhaseBProviderCaptureMetadata;
};

export type PhaseBProviderCaptureFailure = {
  ok: false;
  scope: typeof PHASE_B_PROVIDER_SCOPE;
  errorCode: PhaseBProviderErrorCode;
  retryable: boolean;
  metadata: PhaseBProviderCaptureMetadata;
  message: string;
  details: Record<string, unknown>;
};

export type PhaseBProviderCaptureResult =
  | PhaseBProviderCaptureSuccess
  | PhaseBProviderCaptureFailure;

export type PhaseBAssemblerNormalizedArtifact = {
  normalizedComparisonText: string;
  contentHash: string;
  hashAlgorithm: string;
  rawHtml: string | null;
  extractedText: string | null;
};

/**
 * Phase B provider boundary rules:
 * - The provider adapter returns raw capture materials plus provider metadata only.
 * - The provider adapter does not decide handoff readiness, change state, or content hashing.
 * - The assembler derives one deterministic normalized comparison string from provider materials.
 * - dictionary.dictionary_crawl_artifacts.content_hash is always computed from that assembler-owned string.
 * - Provider-native hashes, job ids, crawl ids, and statuses may be copied into namespaced metadata,
 *   but they must never become substrate pipeline semantics.
 *
 * First-pass capture scope is intentionally narrow:
 * - one approved root source run
 * - one provider capture attempt
 * - one persisted crawl artifact row
 * - no sitemap expansion
 * - no multi-page fanout
 * - no provider-driven discovery beyond the approved root URL
 */
