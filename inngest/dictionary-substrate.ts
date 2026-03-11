import { createHash } from "node:crypto";
import { neon } from "@neondatabase/serverless";
import type { Inngest } from "inngest";

type DispatchTrigger = "scheduled" | "manual";
type PipelineRunStatus = "running" | "succeeded" | "failed";
type ReviewQueueItemType = "fetch_failure" | "artifact_parse_failure";
type ReviewQueueSeverity = "low" | "medium" | "high" | "critical";
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

type RootSourceRow = {
  id: string;
  sourceName: string;
  sourceType: string;
  sourceDomain: string;
  rootUrl: string;
  trustTier: "authoritative" | "corroborative" | "contextual";
  supportedEntityClasses: string[];
  crawlCadenceDays: number | null;
  freshnessSlaDays: number | null;
  failureThreshold: number;
  enabled: boolean;
  lastCrawledAt: string | null;
  latestArtifactId: string | null;
  latestArtifactContentHash: string | null;
};

type PipelineRunRow = {
  id: string;
  rootSourceId: string | null;
  parentRunId: string | null;
};

type FetchArtifactResult = {
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

type IngestionFailure = Error & {
  itemType: ReviewQueueItemType;
  crawlArtifactId?: string | null;
  details?: Record<string, unknown>;
};

function isArtifactHandoffReady(artifact: FetchArtifactResult): boolean {
  const handoff = artifact.metadata.handoff;
  if (!handoff || typeof handoff !== "object" || Array.isArray(handoff)) return false;
  return (handoff as { extraction_ready?: unknown }).extraction_ready === true;
}

function createIngestionFailure(
  itemType: ReviewQueueItemType,
  message: string,
  details?: Record<string, unknown>
): IngestionFailure {
  const error = new Error(message) as IngestionFailure;
  error.itemType = itemType;
  error.details = details || {};
  return error;
}

function buildHandoffOutputPayload(params: {
  rootSourceId: string;
  artifactId: string | null;
  fetchedAt: string | null;
  changeState: FetchArtifactResult["changeState"] | null;
  priorArtifactId: string | null;
  handoffReady: boolean;
  reviewQueueId?: string;
  reviewSeverity?: ReviewQueueSeverity;
  failureCount?: number;
}) {
  return {
    artifactId: params.artifactId,
    rootSourceId: params.rootSourceId,
    fetchedAt: params.fetchedAt,
    changeState: params.changeState,
    handoffReady: params.handoffReady,
    extractionReady: params.handoffReady,
    priorArtifactId: params.priorArtifactId,
    nextStage: params.handoffReady ? "phase_c_extraction_candidates" : null,
    phaseBoundary: "phase_b_ingestion_complete",
    phaseCEventEmitted: false,
    reviewQueueId: params.reviewQueueId || null,
    reviewSeverity: params.reviewSeverity || null,
    failureCount: params.failureCount ?? null
  };
}

function cleanText(value: unknown, max = 1000): string {
  return String(value || "").trim().slice(0, max);
}

function parsePositiveInt(value: unknown, fallback: number, min: number, max: number): number {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function getSql() {
  const databaseUrl = cleanText(process.env.DATABASE_URL || "", 4000);
  if (!databaseUrl) throw new Error("Missing DATABASE_URL");
  return neon(databaseUrl);
}

function normalizeWhitespace(value: string): string {
  return value
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

function buildNormalizedArtifact(body: string, contentType: string | null) {
  const normalizedBody = normalizeWhitespace(body);
  const isHtml = cleanText(contentType || "", 200).toLowerCase().includes("html");
  const extractedText = normalizedBody ? (isHtml ? stripHtmlToText(body) : normalizedBody) : null;
  const comparisonBasis = extractedText || normalizedBody;
  const contentHash = createHash("sha256").update(comparisonBasis).digest("hex");

  return {
    rawHtml: isHtml ? body : null,
    extractedText,
    comparisonBasis,
    contentHash,
    hashAlgorithm: "sha256"
  };
}

async function loadDueRootSources(limit: number): Promise<RootSourceRow[]> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      rs.id,
      rs.source_name as "sourceName",
      rs.source_type as "sourceType",
      rs.source_domain as "sourceDomain",
      rs.root_url as "rootUrl",
      rs.trust_tier as "trustTier",
      rs.supported_entity_classes as "supportedEntityClasses",
      rs.crawl_cadence_days as "crawlCadenceDays",
      rs.freshness_sla_days as "freshnessSlaDays",
      rs.failure_threshold as "failureThreshold",
      rs.enabled,
      rs.last_crawled_at as "lastCrawledAt",
      latest.id as "latestArtifactId",
      latest.content_hash as "latestArtifactContentHash"
    FROM dictionary.root_sources_due_for_ingestion() due
    JOIN dictionary.dictionary_root_sources rs
      ON rs.id = due.root_source_id
    LEFT JOIN LATERAL dictionary.latest_root_source_artifact(rs.id, rs.root_url) latest
      ON true
    ORDER BY
      CASE rs.trust_tier
        WHEN 'authoritative' THEN 0
        WHEN 'corroborative' THEN 1
        ELSE 2
      END,
      due.last_successful_crawl_at NULLS FIRST,
      rs.source_name ASC
    LIMIT ${limit}
  `;
  return rows as RootSourceRow[];
}

async function loadRootSourceById(rootSourceId: string): Promise<RootSourceRow | null> {
  const sql = getSql();
  const rows = await sql`
    SELECT
      rs.id,
      rs.source_name as "sourceName",
      rs.source_type as "sourceType",
      rs.source_domain as "sourceDomain",
      rs.root_url as "rootUrl",
      rs.trust_tier as "trustTier",
      rs.supported_entity_classes as "supportedEntityClasses",
      rs.crawl_cadence_days as "crawlCadenceDays",
      rs.freshness_sla_days as "freshnessSlaDays",
      rs.failure_threshold as "failureThreshold",
      rs.enabled,
      rs.last_crawled_at as "lastCrawledAt",
      latest.id as "latestArtifactId",
      latest.content_hash as "latestArtifactContentHash"
    FROM dictionary.dictionary_root_sources rs
    LEFT JOIN LATERAL dictionary.latest_root_source_artifact(rs.id, rs.root_url) latest
      ON true
    WHERE rs.id = ${rootSourceId}::uuid
    LIMIT 1
  `;
  return (rows[0] as RootSourceRow) || null;
}

async function createPipelineRun(params: {
  stageName: string;
  triggerType: string;
  parentRunId?: string | null;
  rootSourceId?: string | null;
  inputPayload?: Record<string, unknown>;
}): Promise<PipelineRunRow> {
  const sql = getSql();
  const rows = await sql`
    INSERT INTO dictionary.dictionary_pipeline_runs (
      parent_run_id,
      stage_name,
      trigger_type,
      status,
      root_source_id,
      input_payload,
      started_at,
      created_at,
      updated_at
    )
    VALUES (
      ${params.parentRunId || null}::uuid,
      ${params.stageName},
      ${params.triggerType},
      'running',
      ${params.rootSourceId || null}::uuid,
      ${JSON.stringify(params.inputPayload || {})}::jsonb,
      NOW(),
      NOW(),
      NOW()
    )
    RETURNING
      id,
      root_source_id as "rootSourceId",
      parent_run_id as "parentRunId"
  `;
  return rows[0] as PipelineRunRow;
}

async function finalizePipelineRun(params: {
  runId: string;
  status: PipelineRunStatus;
  crawlArtifactId?: string | null;
  outputPayload?: Record<string, unknown>;
  errorPayload?: Record<string, unknown>;
  metrics?: Record<string, unknown>;
}) {
  const sql = getSql();
  await sql`
    UPDATE dictionary.dictionary_pipeline_runs
    SET
      status = ${params.status},
      crawl_artifact_id = COALESCE(${params.crawlArtifactId || null}::uuid, crawl_artifact_id),
      output_payload = ${JSON.stringify(params.outputPayload || {})}::jsonb,
      error_payload = ${JSON.stringify(params.errorPayload || {})}::jsonb,
      metrics = ${JSON.stringify(params.metrics || {})}::jsonb,
      ended_at = NOW(),
      updated_at = NOW()
    WHERE id = ${params.runId}::uuid
  `;
}

function deriveFailureSeverity(
  failureCount: number,
  failureThreshold: number,
  trustTier: RootSourceRow["trustTier"]
): ReviewQueueSeverity {
  if (failureCount >= failureThreshold) {
    return trustTier === "authoritative" ? "critical" : "high";
  }
  if (failureCount >= Math.max(2, failureThreshold - 1)) return "medium";
  return "low";
}

function buildSuggestedAction(
  itemType: ReviewQueueItemType,
  severity: ReviewQueueSeverity,
  rootSource: RootSourceRow
): string {
  if (itemType === "artifact_parse_failure") {
    return severity === "critical"
      ? `Inspect parser behavior for ${rootSource.rootUrl} and update capture handling before the next crawl.`
      : `Inspect parse output for ${rootSource.rootUrl} and verify the page still yields usable substrate text.`;
  }

  return severity === "critical"
    ? `Inspect access or availability for ${rootSource.rootUrl}; repeated failures exceeded threshold for this root source.`
    : `Retry crawl for ${rootSource.rootUrl} and verify remote availability or access controls.`;
}

async function recordReviewQueueFailure(params: {
  rootSource: RootSourceRow;
  pipelineRunId: string;
  itemType: ReviewQueueItemType;
  lastError: string;
  crawlArtifactId?: string | null;
}) {
  const sql = getSql();
  const nowIso = new Date().toISOString();
  const existingRows = await sql`
    SELECT id, retry_count as "retryCount"
    FROM dictionary.dictionary_review_queue
    WHERE item_type = ${params.itemType}
      AND root_source_id = ${params.rootSource.id}::uuid
      AND affected_record_type = 'root_source'
      AND affected_record_id = ${params.rootSource.id}::uuid
      AND resolved_at IS NULL
    ORDER BY last_failed_at DESC, created_at DESC
    LIMIT 1
  `;

  const existing = existingRows[0] as { id: string; retryCount: number } | undefined;
  const failureCount = existing ? Number(existing.retryCount || 0) + 1 : 1;
  const severity = deriveFailureSeverity(
    failureCount,
    params.rootSource.failureThreshold,
    params.rootSource.trustTier
  );
  const suggestedAction = buildSuggestedAction(params.itemType, severity, params.rootSource);

  if (existing?.id) {
    await sql`
      UPDATE dictionary.dictionary_review_queue
      SET
        severity = ${severity},
        crawl_artifact_id = COALESCE(${params.crawlArtifactId || null}::uuid, crawl_artifact_id),
        pipeline_run_id = ${params.pipelineRunId}::uuid,
        retry_count = ${failureCount},
        last_error = ${params.lastError},
        suggested_action = ${suggestedAction},
        last_failed_at = ${nowIso}::timestamptz,
        updated_at = NOW()
      WHERE id = ${existing.id}::uuid
    `;
    return { reviewQueueId: existing.id, severity, failureCount };
  }

  const inserted = await sql`
    INSERT INTO dictionary.dictionary_review_queue (
      item_type,
      severity,
      root_source_id,
      crawl_artifact_id,
      pipeline_run_id,
      affected_record_type,
      affected_record_id,
      retry_count,
      last_error,
      suggested_action,
      first_failed_at,
      last_failed_at,
      created_at,
      updated_at
    )
    VALUES (
      ${params.itemType},
      ${severity},
      ${params.rootSource.id}::uuid,
      ${params.crawlArtifactId || null}::uuid,
      ${params.pipelineRunId}::uuid,
      'root_source',
      ${params.rootSource.id}::uuid,
      ${failureCount},
      ${params.lastError},
      ${suggestedAction},
      ${nowIso}::timestamptz,
      ${nowIso}::timestamptz,
      NOW(),
      NOW()
    )
    RETURNING id
  `;

  return {
    reviewQueueId: cleanText(inserted[0]?.id || "", 80),
    severity,
    failureCount
  };
}

async function resolveOpenReviewQueueFailures(rootSourceId: string, pipelineRunId: string) {
  const sql = getSql();
  await sql`
    UPDATE dictionary.dictionary_review_queue
    SET
      resolved_at = NOW(),
      pipeline_run_id = ${pipelineRunId}::uuid,
      updated_at = NOW()
    WHERE root_source_id = ${rootSourceId}::uuid
      AND item_type IN ('fetch_failure', 'artifact_parse_failure')
      AND resolved_at IS NULL
  `;
}

async function persistCrawlArtifact(rootSource: RootSourceRow, runId: string, artifact: FetchArtifactResult) {
  const sql = getSql();
  const rows = await sql`
    INSERT INTO dictionary.dictionary_crawl_artifacts (
      root_source_id,
      substrate_run_id,
      prior_artifact_id,
      source_url,
      source_domain,
      content_hash,
      fetched_at,
      http_status,
      content_type,
      raw_html,
      extracted_text,
      metadata,
      created_at
    )
    VALUES (
      ${rootSource.id}::uuid,
      ${runId}::uuid,
      ${artifact.priorArtifactId || null}::uuid,
      ${artifact.sourceUrl},
      ${artifact.sourceDomain},
      ${artifact.contentHash},
      ${artifact.fetchedAt}::timestamptz,
      ${artifact.httpStatus},
      ${artifact.contentType},
      ${artifact.rawHtml},
      ${artifact.extractedText},
      ${JSON.stringify(artifact.metadata)}::jsonb,
      NOW()
    )
    RETURNING id
  `;

  await sql`
    UPDATE dictionary.dictionary_root_sources
    SET
      last_crawled_at = ${artifact.fetchedAt}::timestamptz,
      updated_at = NOW()
    WHERE id = ${rootSource.id}::uuid
  `;

  return cleanText(rows[0]?.id || "", 80);
}

async function fetchAndBuildArtifact(rootSource: RootSourceRow, trigger: string, parentRunId: string | null): Promise<FetchArtifactResult> {
  const startedAt = Date.now();
  const response = await fetch(rootSource.rootUrl, {
    method: "GET",
    headers: {
      "User-Agent": "DaytonEnquirerDictionarySubstrateBot/1.0 (+https://thedaytonenquirer.com)",
      Accept: "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8"
    }
  });

  const body = await response.text();
  if (!response.ok) {
    throw createIngestionFailure(
      "fetch_failure",
      `Fetch failed for ${rootSource.rootUrl} with status ${response.status}`,
      {
        rootUrl: rootSource.rootUrl,
        httpStatus: response.status
      }
    );
  }
  const contentType = cleanText(response.headers.get("content-type") || "", 255) || null;
  const normalized = buildNormalizedArtifact(body, contentType);
  const priorArtifactId = rootSource.latestArtifactId || null;
  const previousHash = cleanText(rootSource.latestArtifactContentHash || "", 128) || null;
  const changeState: "initial" | "unchanged" | "changed" =
    !priorArtifactId ? "initial" : previousHash === normalized.contentHash ? "unchanged" : "changed";
  const fetchedAt = new Date().toISOString();

  return {
    sourceUrl: rootSource.rootUrl,
    sourceDomain: rootSource.sourceDomain,
    fetchedAt,
    httpStatus: response.status,
    contentType,
    rawHtml: normalized.rawHtml,
    extractedText: normalized.extractedText,
    contentHash: normalized.contentHash,
    changeState,
    priorArtifactId,
    metadata: {
      change_state: changeState,
      hash_algorithm: normalized.hashAlgorithm,
      fetch: {
        requested_url: rootSource.rootUrl,
        final_url: cleanText(response.url || rootSource.rootUrl, 2000),
        http_status: response.status,
        ok: response.ok,
        duration_ms: Date.now() - startedAt,
        content_length_bytes: Buffer.byteLength(body, "utf8")
      },
      parser: {
        content_type: contentType,
        extracted_text_length: normalized.extractedText ? normalized.extractedText.length : 0,
        raw_html_length: normalized.rawHtml ? normalized.rawHtml.length : 0
      },
      source_policy: {
        trust_tier: rootSource.trustTier,
        crawl_cadence_days: rootSource.crawlCadenceDays,
        freshness_sla_days: rootSource.freshnessSlaDays,
        failure_threshold: rootSource.failureThreshold,
        supported_entity_classes: rootSource.supportedEntityClasses
      },
      handoff: {
        extraction_ready: response.ok && Boolean(normalized.extractedText),
        trigger,
        parent_run_id: parentRunId
      }
    }
  };
}

async function runRootSourceIngestion(input: {
  rootSourceId: string;
  trigger: DispatchTrigger;
  parentRunId?: string | null;
}) {
  const rootSource = await loadRootSourceById(input.rootSourceId);
  if (!rootSource) {
    throw new Error(`Unknown root source ${input.rootSourceId}`);
  }
  if (!rootSource.enabled) {
    throw new Error(`Root source ${input.rootSourceId} is disabled`);
  }

  const run = await createPipelineRun({
    parentRunId: input.parentRunId || null,
    rootSourceId: rootSource.id,
    stageName: "phase_b_root_ingestion",
    triggerType: input.trigger,
    inputPayload: {
      rootSourceId: rootSource.id,
      rootUrl: rootSource.rootUrl,
      trigger: input.trigger
    }
  });

  try {
    const artifact = await fetchAndBuildArtifact(rootSource, input.trigger, input.parentRunId || null);
    const artifactId = await persistCrawlArtifact(rootSource, run.id, artifact);
    if (!isArtifactHandoffReady(artifact)) {
      const parseFailure = createIngestionFailure(
        "artifact_parse_failure",
        `Artifact capture for ${rootSource.rootUrl} did not yield handoff-ready text`,
        {
          rootUrl: rootSource.rootUrl,
          fetchedAt: artifact.fetchedAt,
          contentType: artifact.contentType,
          changeState: artifact.changeState,
          priorArtifactId: artifact.priorArtifactId
        }
      );
      parseFailure.crawlArtifactId = artifactId;
      throw parseFailure;
    }

    await resolveOpenReviewQueueFailures(rootSource.id, run.id);
    await finalizePipelineRun({
      runId: run.id,
      status: "succeeded",
      crawlArtifactId: artifactId,
      outputPayload: buildHandoffOutputPayload({
        artifactId,
        rootSourceId: rootSource.id,
        fetchedAt: artifact.fetchedAt,
        changeState: artifact.changeState,
        handoffReady: isArtifactHandoffReady(artifact),
        priorArtifactId: artifact.priorArtifactId
      }),
      metrics: {
        httpStatus: artifact.httpStatus,
        contentHash: artifact.contentHash
      }
    });
    return {
      ok: true,
      runId: run.id,
      rootSourceId: rootSource.id,
      artifactId,
      changeState: artifact.changeState,
      handoffReady: true,
      nextStage: "phase_c_extraction_candidates",
      phaseCEventEmitted: false
    };
  } catch (error: any) {
    const itemType: ReviewQueueItemType =
      error?.itemType === "artifact_parse_failure" ? "artifact_parse_failure" : "fetch_failure";
    const reviewState = await recordReviewQueueFailure({
      rootSource,
      pipelineRunId: run.id,
      itemType,
      lastError: cleanText(error?.message || "root_source_ingestion_failed", 2000),
      crawlArtifactId: cleanText(error?.crawlArtifactId || "", 80) || null
    });
    const isParseFailure = itemType === "artifact_parse_failure";

    await finalizePipelineRun({
      runId: run.id,
      status: isParseFailure ? "succeeded" : "failed",
      crawlArtifactId: cleanText(error?.crawlArtifactId || "", 80) || null,
      outputPayload: isParseFailure
        ? buildHandoffOutputPayload({
            artifactId: cleanText(error?.crawlArtifactId || "", 80) || null,
            rootSourceId: rootSource.id,
            fetchedAt: cleanText(error?.details?.fetchedAt || "", 80) || null,
            changeState: (error?.details?.changeState as FetchArtifactResult["changeState"] | null) || null,
            handoffReady: false,
            priorArtifactId: cleanText(error?.details?.priorArtifactId || "", 80) || null,
            reviewQueueId: reviewState.reviewQueueId,
            reviewSeverity: reviewState.severity,
            failureCount: reviewState.failureCount
          })
        : {},
      errorPayload: {
        message: cleanText(error?.message || "root_source_ingestion_failed", 2000),
        rootSourceId: rootSource.id,
        itemType,
        reviewQueueId: reviewState.reviewQueueId,
        reviewSeverity: reviewState.severity,
        failureCount: reviewState.failureCount
      }
    });
    if (isParseFailure) {
      return {
        ok: true,
        runId: run.id,
        rootSourceId: rootSource.id,
        artifactId: cleanText(error?.crawlArtifactId || "", 80) || null,
        fetchedAt: cleanText(error?.details?.fetchedAt || "", 80) || null,
        changeState: (error?.details?.changeState as FetchArtifactResult["changeState"] | null) || null,
        handoffReady: false,
        nextStage: null,
        phaseCEventEmitted: false,
        reviewQueueId: reviewState.reviewQueueId,
        reviewSeverity: reviewState.severity,
        failureCount: reviewState.failureCount
      };
    }
    throw error;
  }
}

export function createDictionarySubstrateDispatchSchedulerFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-dispatch-scheduler", concurrency: { limit: 1 } },
    { cron: "0 14 * * 1" },
    async ({ step }: any) => {
      await step.sendEvent("emit-dictionary-substrate-scheduled-dispatch", {
        name: "dictionary.substrate.ingestion.dispatch",
        data: {
          trigger: "scheduled",
          limit: 25
        }
      });
      return {
        ok: true,
        trigger: "scheduled",
        scheduledAt: new Date().toISOString()
      };
    }
  );
}

export function createDictionarySubstrateDispatchFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-dispatch" },
    { event: "dictionary.substrate.ingestion.dispatch" },
    async ({ event, step }: any) => {
      const trigger = cleanText(event?.data?.trigger || "manual", 20) === "scheduled" ? "scheduled" : "manual";
      const limit = parsePositiveInt(event?.data?.limit, 25, 1, 200);
      const rootSourceId = cleanText(event?.data?.rootSourceId || "", 80) || null;
      if (rootSourceId && !UUID_RE.test(rootSourceId)) {
        throw new Error("Invalid rootSourceId");
      }

      const dispatchRun = await step.run("create-dispatch-run", async () =>
        createPipelineRun({
          stageName: "phase_b_ingestion_dispatch",
          triggerType: trigger,
          inputPayload: {
            rootSourceId,
            limit,
            trigger
          }
        })
      );

      try {
        const rootSources = await step.run("select-root-sources", async () => {
          if (rootSourceId) {
            const row = await loadRootSourceById(rootSourceId);
            if (!row) {
              throw new Error(`Unknown root source ${rootSourceId}`);
            }
            return [row];
          }
          return loadDueRootSources(limit);
        });

        for (const rootSource of rootSources) {
          await step.sendEvent(`emit-root-ingestion-${rootSource.id}`, {
            name: "dictionary.substrate.ingestion.root",
            data: {
              rootSourceId: rootSource.id,
              trigger,
              parentRunId: dispatchRun.id
            }
          });
        }

        await step.run("finalize-dispatch-run", async () =>
          finalizePipelineRun({
            runId: dispatchRun.id,
            status: "succeeded",
            outputPayload: {
              trigger,
              rootSourceId,
              selectedRootSourceCount: rootSources.length,
              selectedRootSourceIds: rootSources.map((row) => row.id)
            }
          })
        );

        return {
          ok: true,
          trigger,
          dispatchRunId: dispatchRun.id,
          selectedRootSourceCount: rootSources.length,
          selectedRootSourceIds: rootSources.map((row) => row.id)
        };
      } catch (error: any) {
        await step.run("finalize-dispatch-run-failed", async () =>
          finalizePipelineRun({
            runId: dispatchRun.id,
            status: "failed",
            errorPayload: {
              message: cleanText(error?.message || "dictionary_dispatch_failed", 2000),
              trigger,
              rootSourceId
            }
          })
        );
        throw error;
      }
    }
  );
}

export function createDictionarySubstrateRootIngestionFunction(inngest: Inngest) {
  return inngest.createFunction(
    { id: "dictionary-substrate-root-ingestion" },
    { event: "dictionary.substrate.ingestion.root" },
    async ({ event, step }: any) => {
      const rootSourceId = cleanText(event?.data?.rootSourceId || "", 80);
      if (!rootSourceId) throw new Error("Missing rootSourceId");
      if (!UUID_RE.test(rootSourceId)) throw new Error("Invalid rootSourceId");

      const trigger = cleanText(event?.data?.trigger || "manual", 20) === "scheduled" ? "scheduled" : "manual";
      const parentRunId = cleanText(event?.data?.parentRunId || "", 80) || null;

      const result = await step.run("root-source-ingestion", async () =>
        runRootSourceIngestion({
          rootSourceId,
          trigger,
          parentRunId
        })
      );

      return {
        ok: true,
        ...result
      };
    }
  );
}
