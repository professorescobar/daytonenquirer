const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('./_admin-auth');

const TRUST_TIERS = new Set(['authoritative', 'corroborative', 'contextual']);
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function cleanText(value, max = 255) {
  return String(value || '').trim().slice(0, max);
}

function parseBool(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function parsePositiveIntOrNull(value, fieldName) {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${fieldName} must be a positive integer`);
  }
  return parsed;
}

function parseJsonArray(value, fieldName) {
  if (value === undefined || value === null || value === '') return [];
  const parsed = typeof value === 'string' ? JSON.parse(value) : value;
  if (!Array.isArray(parsed)) {
    throw new Error(`${fieldName} must be an array`);
  }
  return parsed
    .map(item => cleanText(item, 120))
    .filter(Boolean);
}

function parseJsonObject(value, fieldName) {
  if (value === undefined || value === null || value === '') return {};
  const parsed = typeof value === 'string' ? JSON.parse(value) : value;
  if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
    throw new Error(`${fieldName} must be an object`);
  }
  return parsed;
}

function normalizeTrustTier(value) {
  const normalized = cleanText(value, 40).toLowerCase();
  if (!TRUST_TIERS.has(normalized)) {
    throw new Error('trustTier must be authoritative, corroborative, or contextual');
  }
  return normalized;
}

function normalizeRootSourcePayload(body = {}) {
  const id = cleanText(body.id, 64) || null;
  const sourceName = cleanText(body.sourceName || body.source_name, 255);
  const sourceType = cleanText(body.sourceType || body.source_type, 120);
  const sourceDomain = cleanText(body.sourceDomain || body.source_domain, 255).toLowerCase();
  const rootUrl = cleanText(body.rootUrl || body.root_url, 2000);
  const trustTier = normalizeTrustTier(body.trustTier || body.trust_tier);
  const supportedEntityClasses = parseJsonArray(
    body.supportedEntityClasses ?? body.supported_entity_classes,
    'supportedEntityClasses'
  );
  const domainMetadata = parseJsonObject(body.domainMetadata ?? body.domain_metadata, 'domainMetadata');
  const urlMetadata = parseJsonObject(body.urlMetadata ?? body.url_metadata, 'urlMetadata');
  const crawlCadenceDays = parsePositiveIntOrNull(
    body.crawlCadenceDays ?? body.crawl_cadence_days,
    'crawlCadenceDays'
  );
  const freshnessSlaDays = parsePositiveIntOrNull(
    body.freshnessSlaDays ?? body.freshness_sla_days,
    'freshnessSlaDays'
  );
  const failureThreshold = parsePositiveIntOrNull(
    body.failureThreshold ?? body.failure_threshold,
    'failureThreshold'
  ) ?? 3;
  const enabled = parseBool(body.enabled, true);
  const notes = cleanText(body.notes, 2000) || null;

  if (!sourceName) throw new Error('sourceName is required');
  if (!sourceType) throw new Error('sourceType is required');
  if (!sourceDomain) throw new Error('sourceDomain is required');
  if (!rootUrl) throw new Error('rootUrl is required');
  if (id && !UUID_RE.test(id)) throw new Error('id must be a valid UUID');

  return {
    id,
    sourceName,
    sourceType,
    sourceDomain,
    rootUrl,
    trustTier,
    supportedEntityClasses,
    domainMetadata,
    urlMetadata,
    crawlCadenceDays,
    freshnessSlaDays,
    failureThreshold,
    enabled,
    notes
  };
}

async function dictionarySchemaReady(sql) {
  const rows = await sql`SELECT to_regclass('dictionary.dictionary_root_sources') AS reg`;
  return Boolean(rows?.[0]?.reg);
}

async function listRootSources(sql, { enabledOnly, dueOnly }) {
  if (dueOnly) {
    return sql`
      SELECT
        rs.id,
        rs.source_name as "sourceName",
        rs.source_type as "sourceType",
        rs.source_domain as "sourceDomain",
        rs.root_url as "rootUrl",
        rs.trust_tier as "trustTier",
        rs.supported_entity_classes as "supportedEntityClasses",
        rs.domain_metadata as "domainMetadata",
        rs.url_metadata as "urlMetadata",
        rs.crawl_cadence_days as "crawlCadenceDays",
        rs.freshness_sla_days as "freshnessSlaDays",
        rs.failure_threshold as "failureThreshold",
        rs.enabled,
        rs.notes,
        rs.last_crawled_at as "lastCrawledAt",
        rs.created_at as "createdAt",
        rs.updated_at as "updatedAt",
        due.last_successful_crawl_at as "lastSuccessfulCrawlAt",
        due.days_since_last_success as "daysSinceLastSuccess",
        due.due_by_cadence as "dueByCadence",
        due.overdue_by_freshness_sla as "overdueByFreshnessSla",
        due.selection_reason as "selectionReason",
        latest.id as "latestArtifactId",
        latest.fetched_at as "latestArtifactFetchedAt",
        latest.content_hash as "latestArtifactContentHash",
        latest.prior_artifact_id as "latestArtifactPriorArtifactId"
      FROM dictionary.root_sources_due_for_ingestion() due
      JOIN dictionary.dictionary_root_sources rs
        ON rs.id = due.root_source_id
      LEFT JOIN LATERAL dictionary.latest_root_source_artifact(rs.id, rs.root_url) latest
        ON true
      ORDER BY
        CASE due.trust_tier
          WHEN 'authoritative' THEN 0
          WHEN 'corroborative' THEN 1
          ELSE 2
        END,
        due.last_successful_crawl_at NULLS FIRST,
        rs.source_name ASC
    `;
  }

  return sql`
    SELECT
      rs.id,
      rs.source_name as "sourceName",
      rs.source_type as "sourceType",
      rs.source_domain as "sourceDomain",
      rs.root_url as "rootUrl",
      rs.trust_tier as "trustTier",
      rs.supported_entity_classes as "supportedEntityClasses",
      rs.domain_metadata as "domainMetadata",
      rs.url_metadata as "urlMetadata",
      rs.crawl_cadence_days as "crawlCadenceDays",
      rs.freshness_sla_days as "freshnessSlaDays",
      rs.failure_threshold as "failureThreshold",
      rs.enabled,
      rs.notes,
      rs.last_crawled_at as "lastCrawledAt",
      rs.created_at as "createdAt",
      rs.updated_at as "updatedAt",
      due.last_successful_crawl_at as "lastSuccessfulCrawlAt",
      due.days_since_last_success as "daysSinceLastSuccess",
      due.due_by_cadence as "dueByCadence",
      due.overdue_by_freshness_sla as "overdueByFreshnessSla",
      due.selection_reason as "selectionReason",
      latest.id as "latestArtifactId",
      latest.fetched_at as "latestArtifactFetchedAt",
      latest.content_hash as "latestArtifactContentHash",
      latest.prior_artifact_id as "latestArtifactPriorArtifactId"
    FROM dictionary.dictionary_root_sources rs
    LEFT JOIN dictionary.root_sources_due_for_ingestion() due
      ON due.root_source_id = rs.id
    LEFT JOIN LATERAL dictionary.latest_root_source_artifact(rs.id, rs.root_url) latest
      ON true
    WHERE (${enabledOnly}::boolean = false OR rs.enabled = true)
    ORDER BY
      rs.enabled DESC,
      CASE rs.trust_tier
        WHEN 'authoritative' THEN 0
        WHEN 'corroborative' THEN 1
        ELSE 2
      END,
      rs.source_name ASC,
      rs.created_at ASC
  `;
}

async function upsertRootSource(sql, payload) {
  return sql`
    WITH updated AS (
      UPDATE dictionary.dictionary_root_sources
      SET
        source_name = ${payload.sourceName},
        source_type = ${payload.sourceType},
        source_domain = ${payload.sourceDomain},
        root_url = ${payload.rootUrl},
        trust_tier = ${payload.trustTier},
        supported_entity_classes = ${JSON.stringify(payload.supportedEntityClasses)}::jsonb,
        domain_metadata = ${JSON.stringify(payload.domainMetadata)}::jsonb,
        url_metadata = ${JSON.stringify(payload.urlMetadata)}::jsonb,
        crawl_cadence_days = ${payload.crawlCadenceDays},
        freshness_sla_days = ${payload.freshnessSlaDays},
        failure_threshold = ${payload.failureThreshold},
        enabled = ${payload.enabled},
        notes = ${payload.notes},
        updated_at = NOW()
      WHERE (
        ${payload.id}::uuid IS NOT NULL
        AND id = ${payload.id}::uuid
      )
      OR lower(root_url) = lower(${payload.rootUrl})
      RETURNING
        id,
        source_name as "sourceName",
        source_type as "sourceType",
        source_domain as "sourceDomain",
        root_url as "rootUrl",
        trust_tier as "trustTier",
        supported_entity_classes as "supportedEntityClasses",
        domain_metadata as "domainMetadata",
        url_metadata as "urlMetadata",
        crawl_cadence_days as "crawlCadenceDays",
        freshness_sla_days as "freshnessSlaDays",
        failure_threshold as "failureThreshold",
        enabled,
        notes,
        last_crawled_at as "lastCrawledAt",
        created_at as "createdAt",
        updated_at as "updatedAt"
    ),
    inserted AS (
      INSERT INTO dictionary.dictionary_root_sources (
        id,
        source_name,
        source_type,
        source_domain,
        root_url,
        trust_tier,
        supported_entity_classes,
        domain_metadata,
        url_metadata,
        crawl_cadence_days,
        freshness_sla_days,
        failure_threshold,
        enabled,
        notes,
        updated_at
      )
      SELECT
        COALESCE(${payload.id}::uuid, gen_random_uuid()),
        ${payload.sourceName},
        ${payload.sourceType},
        ${payload.sourceDomain},
        ${payload.rootUrl},
        ${payload.trustTier},
        ${JSON.stringify(payload.supportedEntityClasses)}::jsonb,
        ${JSON.stringify(payload.domainMetadata)}::jsonb,
        ${JSON.stringify(payload.urlMetadata)}::jsonb,
        ${payload.crawlCadenceDays},
        ${payload.freshnessSlaDays},
        ${payload.failureThreshold},
        ${payload.enabled},
        ${payload.notes},
        NOW()
      WHERE NOT EXISTS (SELECT 1 FROM updated)
      RETURNING
        id,
        source_name as "sourceName",
        source_type as "sourceType",
        source_domain as "sourceDomain",
        root_url as "rootUrl",
        trust_tier as "trustTier",
        supported_entity_classes as "supportedEntityClasses",
        domain_metadata as "domainMetadata",
        url_metadata as "urlMetadata",
        crawl_cadence_days as "crawlCadenceDays",
        freshness_sla_days as "freshnessSlaDays",
        failure_threshold as "failureThreshold",
        enabled,
        notes,
        last_crawled_at as "lastCrawledAt",
        created_at as "createdAt",
        updated_at as "updatedAt"
    )
    SELECT *
    FROM updated
    UNION ALL
    SELECT *
    FROM inserted
    LIMIT 1
  `;
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;

  if (!['GET', 'PUT'].includes(req.method)) {
    res.setHeader('Allow', ['GET', 'PUT']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  try {
    const sql = neon(process.env.DATABASE_URL);
    const schemaReady = await dictionarySchemaReady(sql);
    if (!schemaReady) {
      return res.status(503).json({
        error: 'Dictionary substrate schema missing',
        details: 'Run the dictionary Phase A migrations first.'
      });
    }

    if (req.method === 'GET') {
      const enabledOnly = parseBool(req.query?.enabledOnly ?? req.query?.enabled_only, false);
      const dueOnly = parseBool(req.query?.dueOnly ?? req.query?.due_only, false);
      const rootSources = await listRootSources(sql, { enabledOnly, dueOnly });
      return res.status(200).json({
        filters: { enabledOnly, dueOnly },
        rootSources
      });
    }

    const payload = normalizeRootSourcePayload(req.body || {});
    const rows = await upsertRootSource(sql, payload);
    return res.status(200).json({ rootSource: rows[0] || null });
  } catch (error) {
    console.error('Admin dictionary root sources error:', error);
    const message = String(error?.message || '');
    if (
      message.includes('supportedEntityClasses')
      || message.includes('domainMetadata')
      || message.includes('urlMetadata')
      || message.includes('trustTier')
      || message.includes('sourceName')
      || message.includes('sourceType')
      || message.includes('sourceDomain')
      || message.includes('rootUrl')
      || message.includes('valid UUID')
      || message.includes('positive integer')
    ) {
      return res.status(400).json({ error: message });
    }
    if (
      message.includes('dictionary.root_sources_due_for_ingestion')
      || message.includes('dictionary.latest_root_source_artifact')
    ) {
      return res.status(503).json({
        error: 'Dictionary Phase B helpers missing',
        details: 'Run migration 20260311_37_dictionary_phase_b_chunk1_ingestion_helpers.sql first.'
      });
    }
    return res.status(500).json({ error: 'Failed to manage dictionary root sources' });
  }
};
