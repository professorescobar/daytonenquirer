import { createHash } from "node:crypto";
import {
  PHASE_B_PROVIDER_CONTRACT_VERSION,
  PHASE_B_PROVIDER_SCOPE,
  type PhaseBAssemblerNormalizedArtifact,
  type PhaseBProviderCaptureFailure,
  type PhaseBProviderCaptureMetadata,
  type PhaseBProviderCaptureResult
} from "./dictionary-substrate-phase-b-provider-contract";

export type PhaseBProviderName = "webcrawlerapi" | "direct_http";
export type PhaseBFailureItemType = "fetch_failure";

export type PhaseBRootSourceContext = {
  rootUrl: string;
  sourceDomain: string;
  trustTier: "authoritative" | "corroborative" | "contextual";
  supportedEntityClasses: string[];
  urlMetadata?: Record<string, unknown>;
  crawlCadenceDays: number | null;
  freshnessSlaDays: number | null;
  failureThreshold: number;
  latestArtifactId: string | null;
  latestArtifactContentHash: string | null;
};

export type PhaseBFetchArtifactResult = {
  sourceUrl: string;
  sourceDomain: string;
  fetchedAt: string;
  httpStatus: number;
  contentType: string | null;
  rawHtml: string | null;
  extractedText: string | null;
  contentHash: string;
  changeState: "initial" | "unchanged" | "changed";
  priorArtifactId: string | null;
  metadata: Record<string, unknown>;
};

export type PhaseBAssembledCaptureResult =
  | {
      ok: true;
      providerName: PhaseBProviderName;
      artifact: PhaseBFetchArtifactResult;
    }
  | {
      ok: false;
      providerName: PhaseBProviderName;
      itemType: PhaseBFailureItemType;
      message: string;
      details: Record<string, unknown>;
    };

type PhaseBProviderSelection = {
  providerName: PhaseBProviderName;
  overrideMetadata: Record<string, unknown> | null;
};

const PHASE_B_PROVIDER_OVERRIDE_RUNTIME_MARKER = "phase_b_provider_override_v1";

function cleanText(value: unknown, max = 1000): string {
  return String(value || "").trim().slice(0, max);
}

function cleanOptionalText(value: unknown, max = 1000): string | null {
  const cleaned = cleanText(value, max);
  return cleaned || null;
}

function normalizeWhitespace(value: string): string {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function stripHtmlToText(html: string): string {
  return normalizeWhitespace(
    html
      .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
      .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
      .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/gi, " ")
      .replace(/&amp;/gi, "&")
      .replace(/&lt;/gi, "<")
      .replace(/&gt;/gi, ">")
      .replace(/&#39;/gi, "'")
      .replace(/&quot;/gi, "\"")
  );
}

function safeJsonParse(text: string): any {
  try {
    return JSON.parse(text);
  } catch (_) {
    return null;
  }
}

function getConfiguredPhaseBProvider(): PhaseBProviderName {
  const configured = cleanText(process.env.DICTIONARY_SUBSTRATE_PHASE_B_PROVIDER || "", 40).toLowerCase();
  if (configured === "direct_http") return "direct_http";
  if (configured === "webcrawlerapi") return "webcrawlerapi";
  return cleanText(process.env.WEBCRAWLERAPI_API_KEY || "", 400) ? "webcrawlerapi" : "direct_http";
}

function resolvePhaseBProvider(rootSource: PhaseBRootSourceContext): PhaseBProviderSelection {
  const requestedOverride = cleanText(rootSource?.urlMetadata?.provider_override, 40).toLowerCase();
  if (requestedOverride === "direct_http" || requestedOverride === "webcrawlerapi") {
    return {
      providerName: requestedOverride,
      overrideMetadata: {
        requested_provider: requestedOverride,
        applied_provider: requestedOverride,
        source: "root_url_metadata"
      }
    };
  }

  const providerName = getConfiguredPhaseBProvider();
  if (requestedOverride) {
    return {
      providerName,
      overrideMetadata: {
        requested_provider: requestedOverride,
        applied_provider: providerName,
        source: "root_url_metadata",
        ignored_reason: "invalid_provider_override"
      }
    };
  }

  return {
    providerName,
    overrideMetadata: null
  };
}

function createProviderMetadata(params: {
  providerName: PhaseBProviderName;
  providerVersion?: string | null;
  requestedUrl: string;
  finalUrl?: string | null;
  fetchedAt: string;
  httpStatus?: number | null;
  contentType?: string | null;
  durationMs?: number | null;
  attemptCount?: number;
  providerDetails?: Record<string, unknown>;
}): PhaseBProviderCaptureMetadata {
  return {
    providerName: params.providerName,
    providerVersion: params.providerVersion || null,
    requestedUrl: params.requestedUrl,
    finalUrl: params.finalUrl || null,
    fetchedAt: params.fetchedAt,
    httpStatus: params.httpStatus ?? null,
    contentType: params.contentType || null,
    durationMs: params.durationMs ?? null,
    attemptCount: params.attemptCount ?? 1,
    providerDetails: params.providerDetails || {}
  };
}

function buildOverrideRuntimeMarker(
  providerSelection: PhaseBProviderSelection
): Record<string, unknown> | null {
  if (!providerSelection.overrideMetadata) return null;
  return {
    marker: PHASE_B_PROVIDER_OVERRIDE_RUNTIME_MARKER,
    branch: "provider_override_resolution",
    requested_provider: cleanOptionalText(providerSelection.overrideMetadata.requested_provider, 40),
    applied_provider: providerSelection.providerName,
    source: cleanOptionalText(providerSelection.overrideMetadata.source, 80),
    ignored_reason: cleanOptionalText(providerSelection.overrideMetadata.ignored_reason, 120)
  };
}

function mapWebCrawlerApiHttpError(status: number): {
  errorCode: PhaseBProviderCaptureFailure["errorCode"];
  retryable: boolean;
} {
  if (status === 401) return { errorCode: "provider_auth_failed", retryable: false };
  if (status === 402 || status === 429) return { errorCode: "provider_rate_limited", retryable: true };
  if (status >= 500) return { errorCode: "provider_unavailable", retryable: true };
  return { errorCode: "provider_invalid_response", retryable: false };
}

function mapWebCrawlerApiSoftError(errorCode: string): {
  errorCode: PhaseBProviderCaptureFailure["errorCode"];
  retryable: boolean;
} {
  if (errorCode === "unauthorized") return { errorCode: "provider_auth_failed", retryable: false };
  if (errorCode.includes("timeout")) return { errorCode: "provider_timeout", retryable: true };
  if (errorCode.includes("rate")) return { errorCode: "provider_rate_limited", retryable: true };
  if (errorCode.includes("name_not_resolved")) return { errorCode: "provider_transport_failed", retryable: true };
  return { errorCode: "provider_invalid_response", retryable: true };
}

async function captureWithDirectHttpProvider(
  rootSource: PhaseBRootSourceContext,
  providerSelection: PhaseBProviderSelection
): Promise<PhaseBProviderCaptureResult> {
  const startedAt = Date.now();
  const overrideRuntimeMarker = buildOverrideRuntimeMarker(providerSelection);
  const response = await fetch(rootSource.rootUrl, {
    method: "GET",
    headers: {
      "User-Agent": "DaytonEnquirerDictionarySubstrateBot/1.0 (+https://thedaytonenquirer.com)",
      Accept: "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8"
    }
  });

  const body = await response.text();
  const fetchedAt = new Date().toISOString();
  const contentType = cleanOptionalText(response.headers.get("content-type"), 255);
  const metadata = createProviderMetadata({
    providerName: "direct_http",
    providerVersion: "legacy_v1",
    requestedUrl: rootSource.rootUrl,
    finalUrl: cleanOptionalText(response.url || rootSource.rootUrl, 2000),
    fetchedAt,
    httpStatus: response.status,
    contentType,
    durationMs: Date.now() - startedAt,
    providerDetails: {
      transport: "native_fetch",
      ...(providerSelection.overrideMetadata ? { override: providerSelection.overrideMetadata } : {}),
      ...(overrideRuntimeMarker ? { override_runtime: overrideRuntimeMarker } : {})
    }
  });

  if (!response.ok) {
    return {
      ok: false,
      scope: PHASE_B_PROVIDER_SCOPE,
      errorCode: "provider_transport_failed",
      retryable: response.status >= 500,
      metadata,
      message: `Fetch failed for ${rootSource.rootUrl} with status ${response.status}`,
      details: {
        rootUrl: rootSource.rootUrl,
        httpStatus: response.status
      }
    };
  }

  const normalizedBody = normalizeWhitespace(body);
  const isHtml = cleanText(contentType || "", 200).toLowerCase().includes("html");
  const extractedText = normalizedBody ? (isHtml ? stripHtmlToText(body) : normalizedBody) : null;

  return {
    ok: true,
    scope: PHASE_B_PROVIDER_SCOPE,
    materials: {
      rawHtml: isHtml ? body : null,
      extractedText,
      binaryPayload: null
    },
    metadata
  };
}

async function captureWithWebCrawlerApiProvider(
  rootSource: PhaseBRootSourceContext,
  providerSelection: PhaseBProviderSelection
): Promise<PhaseBProviderCaptureResult> {
  const overrideRuntimeMarker = buildOverrideRuntimeMarker(providerSelection);
  const apiKey = cleanText(process.env.WEBCRAWLERAPI_API_KEY || "", 400);
  if (!apiKey) {
    const fetchedAt = new Date().toISOString();
    return {
      ok: false,
      scope: PHASE_B_PROVIDER_SCOPE,
      errorCode: "provider_auth_failed",
      retryable: false,
      metadata: createProviderMetadata({
        providerName: "webcrawlerapi",
        requestedUrl: rootSource.rootUrl,
        fetchedAt,
        providerDetails: {
          configError: "missing_api_key",
          ...(providerSelection.overrideMetadata ? { override: providerSelection.overrideMetadata } : {}),
          ...(overrideRuntimeMarker ? { override_runtime: overrideRuntimeMarker } : {})
        }
      }),
      message: "Missing WEBCRAWLERAPI_API_KEY",
      details: {
        rootUrl: rootSource.rootUrl
      }
    };
  }

  const startedAt = Date.now();
  const response = await fetch("https://api.webcrawlerapi.com/v2/scrape", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      url: rootSource.rootUrl,
      output_format: "markdown"
    })
  });

  const raw = await response.text();
  const parsed = safeJsonParse(raw) || {};
  const fetchedAt = new Date().toISOString();
  const pageStatusCode = Number(parsed?.page_status_code);
  const pageHttpStatus = Number.isFinite(pageStatusCode) ? pageStatusCode : null;
  const metadata = createProviderMetadata({
    providerName: "webcrawlerapi",
    providerVersion: "v2_scrape",
    requestedUrl: rootSource.rootUrl,
    finalUrl: cleanOptionalText(parsed?.url || rootSource.rootUrl, 2000),
    fetchedAt,
    httpStatus: pageHttpStatus ?? response.status,
    contentType: "text/markdown",
    durationMs: Date.now() - startedAt,
    providerDetails: {
      status: cleanOptionalText(parsed?.status, 80),
      success: parsed?.success === false ? false : true,
      error_code: cleanOptionalText(parsed?.error_code, 120),
      error_message: cleanOptionalText(parsed?.error_message, 1000),
      page_title: cleanOptionalText(parsed?.page_title, 500),
      output_format: "markdown",
      ...(providerSelection.overrideMetadata ? { override: providerSelection.overrideMetadata } : {}),
      ...(overrideRuntimeMarker ? { override_runtime: overrideRuntimeMarker } : {})
    }
  });

  if (!response.ok) {
    const mapped = mapWebCrawlerApiHttpError(response.status);
    return {
      ok: false,
      scope: PHASE_B_PROVIDER_SCOPE,
      errorCode: mapped.errorCode,
      retryable: mapped.retryable,
      metadata,
      message: `WebCrawlerAPI scrape request failed with status ${response.status}`,
      details: {
        rootUrl: rootSource.rootUrl,
        providerHttpStatus: response.status,
        responseBody: raw.slice(0, 2000)
      }
    };
  }

  if (parsed?.success === false) {
    const providerErrorCode = cleanText(parsed?.error_code, 120).toLowerCase();
    const mapped = mapWebCrawlerApiSoftError(providerErrorCode);
    return {
      ok: false,
      scope: PHASE_B_PROVIDER_SCOPE,
      errorCode: mapped.errorCode,
      retryable: mapped.retryable,
      metadata,
      message: cleanText(parsed?.error_message || "WebCrawlerAPI scrape failed", 1000),
      details: {
        rootUrl: rootSource.rootUrl,
        providerErrorCode,
        providerErrorMessage: cleanOptionalText(parsed?.error_message, 1000),
        pageHttpStatus
      }
    };
  }

  if (pageHttpStatus !== null && (pageHttpStatus < 200 || pageHttpStatus >= 300)) {
    return {
      ok: false,
      scope: PHASE_B_PROVIDER_SCOPE,
      errorCode: "provider_transport_failed",
      retryable: pageHttpStatus >= 500,
      metadata,
      message: `Target page fetch failed for ${rootSource.rootUrl} with status ${pageHttpStatus}`,
      details: {
        rootUrl: rootSource.rootUrl,
        httpStatus: pageHttpStatus
      }
    };
  }

  const markdown = cleanOptionalText(parsed?.markdown, 2_000_000);
  const cleaned = cleanOptionalText(parsed?.cleaned, 2_000_000);
  const html = cleanOptionalText(parsed?.html, 2_000_000);
  const extractedText = markdown || cleaned || null;

  return {
    ok: true,
    scope: PHASE_B_PROVIDER_SCOPE,
    materials: {
      rawHtml: html,
      extractedText,
      binaryPayload: null
    },
    metadata
  };
}

async function captureWithProvider(
  rootSource: PhaseBRootSourceContext
): Promise<{ providerName: PhaseBProviderName; capture: PhaseBProviderCaptureResult }> {
  const providerSelection = resolvePhaseBProvider(rootSource);
  const providerName = providerSelection.providerName;
  const capture =
    providerName === "webcrawlerapi"
      ? await captureWithWebCrawlerApiProvider(rootSource, providerSelection)
      : await captureWithDirectHttpProvider(rootSource, providerSelection);
  return { providerName, capture };
}

function assemblePhaseBNormalizedArtifact(capture: PhaseBProviderCaptureResult): PhaseBAssemblerNormalizedArtifact {
  if (!capture.ok) {
    throw new Error("Cannot assemble a normalized artifact from a failed provider capture");
  }

  const normalizedExtractedText = normalizeWhitespace(capture.materials.extractedText || "");
  const normalizedRawHtml = cleanOptionalText(capture.materials.rawHtml, 2_000_000);
  const htmlDerivedText = normalizedRawHtml ? stripHtmlToText(normalizedRawHtml) : "";
  const normalizedComparisonText = normalizedExtractedText || htmlDerivedText;
  const contentHash = createHash("sha256").update(normalizedComparisonText).digest("hex");

  return {
    normalizedComparisonText,
    contentHash,
    hashAlgorithm: "sha256",
    rawHtml: normalizedRawHtml,
    extractedText: normalizedExtractedText || htmlDerivedText || null
  };
}

function buildProviderMetadataNamespace(params: {
  providerName: PhaseBProviderName;
  metadata: PhaseBProviderCaptureMetadata;
}) {
  return {
    name: params.providerName,
    contract_version: PHASE_B_PROVIDER_CONTRACT_VERSION,
    scope: PHASE_B_PROVIDER_SCOPE,
    version: params.metadata.providerVersion,
    [params.providerName]: {
      ...params.metadata.providerDetails
    }
  };
}

export async function buildPhaseBFetchArtifact(params: {
  rootSource: PhaseBRootSourceContext;
  trigger: string;
  parentRunId: string | null;
}): Promise<PhaseBAssembledCaptureResult> {
  const { providerName, capture } = await captureWithProvider(params.rootSource);

  if (capture.ok === false) {
    const failedCapture: PhaseBProviderCaptureFailure = capture;
    return {
      ok: false,
      providerName,
      itemType: "fetch_failure",
      message: failedCapture.message,
      details: {
        ...failedCapture.details,
        providerName,
        providerErrorCode: failedCapture.errorCode,
        retryable: failedCapture.retryable,
        providerMetadata: {
          contractVersion: PHASE_B_PROVIDER_CONTRACT_VERSION,
          scope: PHASE_B_PROVIDER_SCOPE,
          name: providerName,
          version: failedCapture.metadata.providerVersion,
          details: failedCapture.metadata.providerDetails
        }
      }
    };
  }

  const assembled = assemblePhaseBNormalizedArtifact(capture);
  const priorArtifactId = params.rootSource.latestArtifactId || null;
  const previousHash = cleanText(params.rootSource.latestArtifactContentHash || "", 128) || null;
  const changeState: "initial" | "unchanged" | "changed" =
    !priorArtifactId ? "initial" : previousHash === assembled.contentHash ? "unchanged" : "changed";
  const contentLengthBytes = Buffer.byteLength(
    assembled.rawHtml || assembled.extractedText || "",
    "utf8"
  );

  return {
    ok: true,
    providerName,
    artifact: {
      sourceUrl: params.rootSource.rootUrl,
      sourceDomain: params.rootSource.sourceDomain,
      fetchedAt: capture.metadata.fetchedAt,
      httpStatus: Number(capture.metadata.httpStatus || 200),
      contentType: capture.metadata.contentType,
      rawHtml: assembled.rawHtml,
      extractedText: assembled.extractedText,
      contentHash: assembled.contentHash,
      changeState,
      priorArtifactId,
      metadata: {
        change_state: changeState,
        hash_algorithm: assembled.hashAlgorithm,
        provider: buildProviderMetadataNamespace({
          providerName,
          metadata: capture.metadata
        }),
        fetch: {
          requested_url: capture.metadata.requestedUrl,
          final_url: capture.metadata.finalUrl || params.rootSource.rootUrl,
          http_status: capture.metadata.httpStatus,
          ok:
            capture.metadata.httpStatus !== null
              ? capture.metadata.httpStatus >= 200 && capture.metadata.httpStatus < 300
              : true,
          duration_ms: capture.metadata.durationMs,
          content_length_bytes: contentLengthBytes
        },
        parser: {
          content_type: capture.metadata.contentType,
          extracted_text_length: assembled.extractedText ? assembled.extractedText.length : 0,
          raw_html_length: assembled.rawHtml ? assembled.rawHtml.length : 0,
          normalized_comparison_text_length: assembled.normalizedComparisonText.length
        },
        source_policy: {
          trust_tier: params.rootSource.trustTier,
          crawl_cadence_days: params.rootSource.crawlCadenceDays,
          freshness_sla_days: params.rootSource.freshnessSlaDays,
          failure_threshold: params.rootSource.failureThreshold,
          supported_entity_classes: params.rootSource.supportedEntityClasses
        },
        handoff: {
          extraction_ready: Boolean(assembled.extractedText),
          trigger: params.trigger,
          parent_run_id: params.parentRunId
        }
      }
    }
  };
}
