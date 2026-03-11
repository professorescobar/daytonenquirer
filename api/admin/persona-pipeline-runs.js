const { neon } = require('@neondatabase/serverless');
const { requireAdmin } = require('../_admin-auth');

const STAGE_ORDER = [
  'topic_qualification',
  'quota_pacing',
  'research_discovery',
  'evidence_extraction',
  'story_planning',
  'draft_writing',
  'image_sourcing',
  'final_review'
];

function cleanText(value, max = 255) {
  return String(value || '').trim().slice(0, max);
}

function parsePositiveInt(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value || ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function tableNameToLiteral(name) {
  return cleanText(name, 80).replace(/[^a-z0-9_]/gi, '');
}

async function tableExists(sql, tableName) {
  const safe = tableNameToLiteral(tableName);
  if (!safe) return false;
  const rows = await sql`SELECT to_regclass(${`public.${safe}`}) as name`;
  return Boolean(rows[0]?.name);
}

function toMetadataObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value;
}

function mapLayer6StageStatus(layer6Run) {
  const status = cleanText(layer6Run?.status || '', 40).toLowerCase();
  if (status === 'completed') return 'completed';
  if (status === 'timed_out') return 'timed_out';
  if (status === 'failed') return 'failed';
  if (status === 'running') return 'in_progress';
  return 'pending';
}

function summarizeResearchDiscovery(details, signal, queue, stageCounts) {
  const researchCount = Number(stageCounts?.research_discovery || 0);
  const queueStatus = cleanText(queue?.status || '', 40).toLowerCase();
  const promoted = cleanText(signal?.action || '', 40).toLowerCase() === 'promote';
  const status = cleanText(details?.status || '', 40).toLowerCase();
  if (status === 'completed' || status === 'degraded' || status === 'failed') return status;
  if (researchCount > 0) return 'completed';
  if (promoted && queueStatus !== 'queued' && queueStatus !== 'deferred' && queueStatus !== 'rejected') return 'in_progress';
  return 'pending';
}

function getStoryPlanningSummary(artifacts) {
  const items = Array.isArray(artifacts) ? artifacts : [];
  const latest = items[0] || null;
  if (!latest) return null;
  const metadata = toMetadataObject(latest.metadata);
  return {
    planningStatus: cleanText(metadata.planningStatus || '', 40).toUpperCase(),
    executionOutcome: cleanText(metadata.executionOutcome || '', 40).toLowerCase(),
    isCanonical: metadata.isCanonical === true
  };
}

function getEvidenceExtractionSummary(artifacts) {
  const items = Array.isArray(artifacts) ? artifacts : [];
  const latestBundle = items.find((item) => cleanText(item.artifactType, 120).toLowerCase() === 'evidence_bundle');
  if (!latestBundle) return null;
  const metadata = toMetadataObject(latestBundle.metadata);
  return {
    extractionStatus: cleanText(metadata.extractionStatus || '', 40).toUpperCase(),
    executionOutcome: cleanText(metadata.executionOutcome || '', 40).toLowerCase(),
    isCanonical: metadata.isCanonical === true,
    evidenceCount: Number.isFinite(Number(metadata.evidenceCount)) ? Number(metadata.evidenceCount) : null,
    failureReasons: Array.isArray(metadata.failureReasons) ? metadata.failureReasons.map((item) => cleanText(item, 240)).filter(Boolean) : [],
    editorialRiskCount: Number.isFinite(Number(metadata.editorialRiskCount)) ? Number(metadata.editorialRiskCount) : null,
    missingEvidenceCount: Number.isFinite(Number(metadata.missingEvidenceCount)) ? Number(metadata.missingEvidenceCount) : null
  };
}

function summarizeStageStatus({ signal, queue, stageCounts, layer6Run, researchDiscovery, evidenceExtraction, storyPlanning }) {
  const researchCount = Number(stageCounts?.research_discovery || 0);
  const evidenceCount = Number(stageCounts?.evidence_extraction || 0);
  const storyPlanningCount = Number(stageCounts?.story_planning || 0);
  const draftWritingCount = Number(stageCounts?.draft_writing || 0);
  const hasDownstreamProgress =
    researchCount > 0 ||
    evidenceCount > 0 ||
    storyPlanningCount > 0 ||
    draftWritingCount > 0 ||
    Boolean(layer6Run);
  const queueStatus = cleanText(queue?.status || '', 40).toLowerCase();
  const promoted =
    cleanText(signal?.action || '', 40).toLowerCase() === 'promote' ||
    hasDownstreamProgress;

  const stages = {
    topic_qualification: promoted ? 'completed' : 'pending',
    quota_pacing: 'pending',
    research_discovery: 'pending',
    evidence_extraction: 'pending',
    story_planning: 'pending',
    draft_writing: 'pending',
    image_sourcing: 'pending',
    final_review: 'pending'
  };

  if (queueStatus === 'released') {
    stages.quota_pacing = 'completed';
  } else if (queueStatus === 'queued' || queueStatus === 'deferred') {
    stages.quota_pacing = 'in_progress';
  } else if (queueStatus === 'rejected') {
    stages.quota_pacing = 'failed';
  } else if (promoted || hasDownstreamProgress) {
    stages.quota_pacing = 'completed';
  }

  stages.research_discovery = summarizeResearchDiscovery(researchDiscovery, signal, queue, stageCounts);

  if (
    evidenceExtraction?.executionOutcome === 'validated' &&
    evidenceExtraction?.extractionStatus === 'READY' &&
    evidenceExtraction?.isCanonical === true
  ) {
    stages.evidence_extraction = 'completed';
  } else if (evidenceExtraction?.extractionStatus === 'NEEDS_REPORTING') {
    stages.evidence_extraction = 'needs_reporting';
  } else if (
    evidenceExtraction?.executionOutcome &&
    evidenceExtraction?.executionOutcome !== 'validated'
  ) {
    stages.evidence_extraction = 'failed';
  } else if (stages.research_discovery === 'completed' || stages.research_discovery === 'degraded') {
    stages.evidence_extraction = 'in_progress';
  } else if (stages.research_discovery === 'failed') {
    stages.evidence_extraction = 'failed';
  }

  const phase3ReadyForPlanning = stages.evidence_extraction === 'completed';

  if (phase3ReadyForPlanning && storyPlanning?.executionOutcome === 'validated' && storyPlanning?.planningStatus === 'READY') {
    stages.story_planning = 'completed';
  } else if (phase3ReadyForPlanning && storyPlanning?.executionOutcome === 'validated' && storyPlanning?.planningStatus === 'NEEDS_REPORTING') {
    stages.story_planning = 'needs_reporting';
  } else if (phase3ReadyForPlanning && storyPlanning?.executionOutcome === 'validated' && storyPlanning?.planningStatus === 'REJECTED') {
    stages.story_planning = 'rejected';
  } else if (
    phase3ReadyForPlanning &&
    storyPlanning?.executionOutcome &&
    storyPlanning?.executionOutcome !== 'validated'
  ) {
    stages.story_planning = 'failed';
  } else if (stages.evidence_extraction === 'needs_reporting') {
    stages.story_planning = 'pending';
  } else if (stages.evidence_extraction === 'failed') {
    stages.story_planning = 'pending';
  } else if (stages.evidence_extraction === 'completed') {
    stages.story_planning = 'in_progress';
  }

  if (stages.story_planning !== 'completed') {
    stages.draft_writing = 'pending';
  } else if (draftWritingCount > 0) {
    stages.draft_writing = 'completed';
  } else if (stages.story_planning === 'completed') {
    stages.draft_writing = 'in_progress';
  }

  if (stages.draft_writing === 'completed') {
    stages.image_sourcing = mapLayer6StageStatus(layer6Run);
    if (stages.image_sourcing === 'pending') stages.image_sourcing = 'in_progress';
  }

  if (stages.image_sourcing === 'completed') {
    // Final review is not auto-derived from image sourcing completion.
    // Keep it pending until an explicit final-review implementation/artifact exists.
    stages.final_review = 'pending';
  } else if (stages.image_sourcing === 'timed_out') {
    stages.final_review = 'failed';
  } else if (stages.image_sourcing === 'failed') {
    stages.final_review = 'failed';
  } else if (stages.image_sourcing === 'in_progress') {
    stages.final_review = 'in_progress';
  }

  let currentStage = 'topic_qualification';
  if (stages.image_sourcing === 'in_progress') currentStage = 'image_sourcing';
  else if (stages.image_sourcing === 'failed') currentStage = 'image_sourcing';
  else if (stages.image_sourcing === 'completed' && stages.final_review !== 'completed') currentStage = 'final_review';
  else if (stages.draft_writing === 'in_progress') currentStage = 'draft_writing';
  else if (stages.draft_writing === 'failed') currentStage = 'draft_writing';
  else if (stages.story_planning === 'in_progress') currentStage = 'story_planning';
  else if (stages.story_planning === 'needs_reporting') currentStage = 'story_planning';
  else if (stages.story_planning === 'rejected') currentStage = 'story_planning';
  else if (stages.story_planning === 'failed') currentStage = 'story_planning';
  else if (stages.evidence_extraction === 'needs_reporting') currentStage = 'evidence_extraction';
  else if (stages.evidence_extraction === 'in_progress') currentStage = 'evidence_extraction';
  else if (stages.evidence_extraction === 'failed') currentStage = 'evidence_extraction';
  else if (stages.research_discovery === 'in_progress') currentStage = 'research_discovery';
  else if (stages.research_discovery === 'degraded') currentStage = 'research_discovery';
  else if (stages.research_discovery === 'failed') currentStage = 'research_discovery';
  else if (stages.quota_pacing === 'in_progress') currentStage = 'quota_pacing';
  else if (stages.quota_pacing === 'failed') currentStage = 'quota_pacing';
  else if (stages.draft_writing === 'completed') currentStage = 'image_sourcing';
  else if (stages.story_planning === 'completed') currentStage = 'draft_writing';
  else if (stages.evidence_extraction === 'completed') currentStage = 'story_planning';
  else if (stages.research_discovery === 'completed') currentStage = 'evidence_extraction';

  return { stages, currentStage };
}

function summarizeRunStatus({ queue, stageStatuses, storyPlanning }) {
  const queueStatus = cleanText(queue?.status || '', 40).toLowerCase();
  if (queueStatus === 'queued' || queueStatus === 'deferred') return 'queued';
  if (queueStatus === 'rejected' || stageStatuses.quota_pacing === 'failed') return 'blocked';
  if (stageStatuses.image_sourcing === 'in_progress') return 'in_progress';
  if (stageStatuses.draft_writing === 'in_progress') return 'in_progress';
  if (stageStatuses.story_planning === 'in_progress') return 'in_progress';
  if (stageStatuses.evidence_extraction === 'in_progress') return 'in_progress';
  if (stageStatuses.research_discovery === 'in_progress') return 'in_progress';
  if (stageStatuses.image_sourcing === 'completed') return 'phase_6_complete';
  if (stageStatuses.image_sourcing === 'timed_out') return 'phase_6_timed_out';
  if (stageStatuses.image_sourcing === 'failed') return 'phase_6_failed';
  if (stageStatuses.draft_writing === 'completed') return 'phase_5_complete';
  if (storyPlanning?.executionOutcome === 'validated' && storyPlanning?.planningStatus === 'NEEDS_REPORTING') return 'phase_4_needs_reporting';
  if (storyPlanning?.executionOutcome === 'validated' && storyPlanning?.planningStatus === 'REJECTED') return 'phase_4_rejected';
  if (stageStatuses.story_planning === 'failed') return 'phase_4_failed';
  if (stageStatuses.story_planning === 'completed') return 'phase_4_complete';
  if (stageStatuses.evidence_extraction === 'needs_reporting') return 'phase_3_needs_reporting';
  if (stageStatuses.evidence_extraction === 'failed') return 'phase_3_failed';
  if (stageStatuses.evidence_extraction === 'completed') return 'phase_3_complete';
  if (stageStatuses.research_discovery === 'failed') return 'phase_2_failed';
  if (stageStatuses.research_discovery === 'degraded') return 'phase_2_degraded';
  if (stageStatuses.research_discovery === 'completed') return 'phase_2_complete';
  return 'promoted';
}

function getResearchDiscoverySummary(artifacts) {
  const items = Array.isArray(artifacts) ? artifacts : [];
  const searchPlanArtifact = items.find((item) => cleanText(item.artifactType, 120).toLowerCase() === 'search_plan');
  if (!searchPlanArtifact) return null;
  const metadata = toMetadataObject(searchPlanArtifact.metadata);
  const passSummaries = Array.isArray(metadata.passSummaries) ? metadata.passSummaries : [];
  return {
    status: cleanText(metadata.status || '', 40).toLowerCase() || 'pending',
    provider: cleanText(metadata.provider || '', 80),
    model: cleanText(metadata.model || '', 120),
    successfulPassType: cleanText(metadata.successfulPassType || '', 80),
    degradationReason: cleanText(metadata.degradationReason || '', 240),
    failureReason: cleanText(metadata.failureReason || '', 240),
    passSummaries: passSummaries.slice(0, 3).map((pass) => ({
      passType: cleanText(pass.passType || '', 80),
      passIndex: Number.isFinite(Number(pass.passIndex)) ? Number(pass.passIndex) : null,
      intent: cleanText(pass.intent || '', 280),
      fetchedResultCount: Number(pass.fetchedResultCount || 0),
      usableResultCount: Number(pass.usableResultCount || 0),
      distinctDomainCount: Number(pass.distinctDomainCount || 0),
      appliedDomains: Array.isArray(pass.appliedDomains) ? pass.appliedDomains.map((v) => cleanText(v, 255)).filter(Boolean) : [],
      appliedMaxAgeDays: Number.isFinite(Number(pass.appliedMaxAgeDays)) ? Number(pass.appliedMaxAgeDays) : null,
      sufficiencyMet: pass.sufficiencyMet === true
    }))
  };
}

module.exports = async (req, res) => {
  if (!requireAdmin(req, res)) return;
  if (req.method !== 'GET') {
    res.setHeader('Allow', ['GET']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const personaId = cleanText(req.query?.persona_id || req.query?.personaId || '', 255);
  const limit = parsePositiveInt(req.query?.limit, 30, 1, 100);

  try {
    const sql = neon(process.env.DATABASE_URL);
    const hasReleaseQueue = await tableExists(sql, 'topic_engine_release_queue');
    const hasResearchArtifacts = await tableExists(sql, 'research_artifacts');
    const hasImagePipelineRuns = await tableExists(sql, 'image_pipeline_runs');

    const promotedSignals = await sql`
      SELECT
        s.id,
        s.persona_id as "personaId",
        s.title,
        s.snippet,
        s.source_type as "sourceType",
        s.source_name as "sourceName",
        s.action,
        s.next_step as "nextStep",
        s.review_decision as "reviewDecision",
        s.review_notes as "reviewNotes",
        s.reasoning,
        s.policy_flags as "policyFlags",
        s.event_key as "eventKey",
        s.dedupe_key as "dedupeKey",
        s.relation_to_archive as "relationToArchive",
        s.is_newsworthy as "isNewsworthy",
        s.confidence,
        s.processed_at as "processedAt",
        s.created_at as "createdAt",
        s.updated_at as "updatedAt"
      FROM topic_signals s
      WHERE (
          s.action = 'promote'
          OR EXISTS (
            SELECT 1
            FROM image_pipeline_runs ipr
            WHERE ipr.signal_id = s.id
          )
          OR EXISTS (
            SELECT 1
            FROM research_artifacts ra
            WHERE ra.signal_id = s.id
              AND ra.stage IN ('research_discovery', 'evidence_extraction', 'story_planning', 'draft_writing')
          )
        )
        AND (${personaId || null}::text IS NULL OR s.persona_id = ${personaId || null})
      ORDER BY COALESCE(s.processed_at, s.updated_at, s.created_at) DESC, s.id DESC
      LIMIT ${limit}
    `;

    const signalIds = promotedSignals.map((row) => Number(row.id)).filter((id) => Number.isFinite(id) && id > 0);

    let queueRows = [];
    if (hasReleaseQueue && signalIds.length) {
      queueRows = await sql`
        SELECT
          signal_id as "signalId",
          persona_id as "personaId",
          status,
          reason_code as "reasonCode",
          scheduled_for_utc as "scheduledForUtc",
          released_at as "releasedAt",
          released_day_local as "releasedDayLocal",
          attempt_count as "attemptCount",
          last_error as "lastError",
          created_at as "createdAt",
          updated_at as "updatedAt"
        FROM topic_engine_release_queue
        WHERE signal_id = ANY(${signalIds}::bigint[])
      `;
    }
    const queueBySignal = new Map(queueRows.map((row) => [Number(row.signalId), row]));

    let imageRunRows = [];
    if (hasImagePipelineRuns && signalIds.length) {
      imageRunRows = await sql`
        SELECT DISTINCT ON (signal_id)
          signal_id as "signalId",
          id,
          status,
          final_outcome as "finalOutcome",
          selected_tier as "selectedTier",
          (
            SELECT COUNT(*)::int
            FROM image_candidates c
            WHERE c.run_id = image_pipeline_runs.id
          ) as "candidateCount",
          started_at as "startedAt",
          completed_at as "completedAt",
          updated_at as "updatedAt"
        FROM image_pipeline_runs
        WHERE signal_id = ANY(${signalIds}::bigint[])
        ORDER BY
          signal_id,
          CASE WHEN COALESCE(diagnostics->>'idempotencyCanonical', 'false') = 'true' THEN 0 ELSE 1 END,
          COALESCE(updated_at, created_at, started_at) DESC,
          id DESC
      `;
    }
    const imageRunBySignal = new Map(imageRunRows.map((row) => [Number(row.signalId), row]));

    let artifactStatsRows = [];
    let artifactDetailRows = [];
    if (hasResearchArtifacts && signalIds.length) {
      artifactStatsRows = await sql`
        SELECT
          signal_id as "signalId",
          stage,
          COUNT(*)::int as count,
          MAX(created_at) as "latestCreatedAt"
        FROM research_artifacts
        WHERE signal_id = ANY(${signalIds}::bigint[])
        GROUP BY signal_id, stage
      `;

      artifactDetailRows = await sql`
        WITH ranked AS (
          SELECT
            signal_id as "signalId",
            stage,
            artifact_type as "artifactType",
            source_url as "sourceUrl",
            title,
            content,
            metadata,
            created_at as "createdAt",
            ROW_NUMBER() OVER (
              PARTITION BY signal_id, stage
              ORDER BY created_at DESC
            ) as rank_idx
          FROM research_artifacts
          WHERE signal_id = ANY(${signalIds}::bigint[])
        )
        SELECT
          "signalId",
          stage,
          "artifactType",
          "sourceUrl",
          title,
          content,
          metadata,
          "createdAt"
        FROM ranked
        WHERE rank_idx <= 4
        ORDER BY "signalId" DESC, stage ASC, "createdAt" DESC
      `;
    }

    const stageCountBySignal = new Map();
    for (const row of artifactStatsRows) {
      const signalIdNum = Number(row.signalId);
      const stage = cleanText(row.stage, 80).toLowerCase();
      if (!stageCountBySignal.has(signalIdNum)) stageCountBySignal.set(signalIdNum, {});
      const bucket = stageCountBySignal.get(signalIdNum);
      bucket[stage] = Number(row.count || 0);
      const latest = row.latestCreatedAt ? new Date(row.latestCreatedAt).toISOString() : null;
      if (latest) bucket[`${stage}Latest`] = latest;
    }

    const stageArtifactsBySignal = new Map();
    for (const row of artifactDetailRows) {
      const signalIdNum = Number(row.signalId);
      const stage = cleanText(row.stage, 80).toLowerCase();
      if (!stageArtifactsBySignal.has(signalIdNum)) stageArtifactsBySignal.set(signalIdNum, {});
      const byStage = stageArtifactsBySignal.get(signalIdNum);
      if (!byStage[stage]) byStage[stage] = [];

      const metadata = toMetadataObject(row.metadata);
      byStage[stage].push({
        artifactType: cleanText(row.artifactType, 120),
        sourceUrl: cleanText(row.sourceUrl, 2000),
        title: cleanText(row.title, 300),
        content: cleanText(row.content, 900),
        metadata,
        createdAt: row.createdAt ? new Date(row.createdAt).toISOString() : null,
        query: cleanText(metadata.query || '', 200),
        provider: cleanText(metadata.provider || '', 80),
        model: cleanText(metadata.model || '', 120),
        score: Number.isFinite(Number(metadata.score)) ? Number(metadata.score) : null,
        confidence: Number.isFinite(Number(metadata.confidence)) ? Number(metadata.confidence) : null,
        rank: Number.isFinite(Number(metadata.rank)) ? Number(metadata.rank) : null,
        sectionCount: Number.isFinite(Number(metadata.sectionCount)) ? Number(metadata.sectionCount) : null,
        evidenceCount: Number.isFinite(Number(metadata.evidenceCount)) ? Number(metadata.evidenceCount) : null,
        evidenceQuote: cleanText(metadata.evidenceQuote || '', 320),
        whyItMatters: cleanText(metadata.whyItMatters || '', 320)
      });
    }

    const runs = promotedSignals.map((signal) => {
      const signalIdNum = Number(signal.id);
      const queue = queueBySignal.get(signalIdNum) || null;
      const layer6Run = imageRunBySignal.get(signalIdNum) || null;
      const stageCounts = stageCountBySignal.get(signalIdNum) || {};
      const artifactsByStage = stageArtifactsBySignal.get(signalIdNum) || {};
      const researchDiscovery = getResearchDiscoverySummary(artifactsByStage.research_discovery || []);
      const evidenceExtraction = getEvidenceExtractionSummary(artifactsByStage.evidence_extraction || []);
      const storyPlanning = getStoryPlanningSummary(artifactsByStage.story_planning || []);
      const stageInfo = summarizeStageStatus({ signal, queue, stageCounts, layer6Run, researchDiscovery, evidenceExtraction, storyPlanning });
      const runStatus = summarizeRunStatus({ queue, stageStatuses: stageInfo.stages, storyPlanning });

      const timestamps = [
        signal.processedAt,
        signal.updatedAt,
        signal.createdAt,
        queue?.updatedAt,
        queue?.releasedAt,
        stageCounts.research_discoveryLatest,
        stageCounts.evidence_extractionLatest,
        stageCounts.story_planningLatest,
        stageCounts.draft_writingLatest,
        layer6Run?.startedAt,
        layer6Run?.completedAt,
        layer6Run?.updatedAt
      ].filter(Boolean);
      const lastActivityAt = timestamps.length
        ? new Date(Math.max(...timestamps.map((t) => new Date(t).getTime()))).toISOString()
        : null;

      return {
        signalId: signalIdNum,
        personaId: cleanText(signal.personaId, 255),
        title: cleanText(signal.title, 400),
        snippet: cleanText(signal.snippet, 1200),
        sourceType: cleanText(signal.sourceType, 80),
        sourceName: cleanText(signal.sourceName, 255),
        action: cleanText(signal.action, 40),
        nextStep: cleanText(signal.nextStep, 80),
        reviewDecision: cleanText(signal.reviewDecision, 80),
        relationToArchive: cleanText(signal.relationToArchive, 80),
        eventKey: cleanText(signal.eventKey || signal.dedupeKey, 300),
        isNewsworthy: Number.isFinite(Number(signal.isNewsworthy)) ? Number(signal.isNewsworthy) : null,
        confidence: Number.isFinite(Number(signal.confidence)) ? Number(signal.confidence) : null,
        processedAt: signal.processedAt ? new Date(signal.processedAt).toISOString() : null,
        createdAt: signal.createdAt ? new Date(signal.createdAt).toISOString() : null,
        updatedAt: signal.updatedAt ? new Date(signal.updatedAt).toISOString() : null,
        lastActivityAt,
        runStatus,
        currentStage: stageInfo.currentStage,
        queue: queue
          ? {
              status: cleanText(queue.status, 60),
              reasonCode: cleanText(queue.reasonCode, 120),
              scheduledForUtc: queue.scheduledForUtc ? new Date(queue.scheduledForUtc).toISOString() : null,
              releasedAt: queue.releasedAt ? new Date(queue.releasedAt).toISOString() : null,
              releasedDayLocal: queue.releasedDayLocal ? String(queue.releasedDayLocal) : null,
              attemptCount: Number(queue.attemptCount || 0),
              lastError: cleanText(queue.lastError, 500)
            }
          : null,
        stageProgress: STAGE_ORDER.map((stageName) => ({
          stage: stageName,
          status: stageInfo.stages[stageName] || 'pending',
          artifactCount: stageName === 'image_sourcing'
            ? Number(layer6Run?.candidateCount || 0)
            : stageName === 'evidence_extraction' && evidenceExtraction
              ? Number(evidenceExtraction.evidenceCount || 0)
            : stageName === 'research_discovery' && researchDiscovery
              ? Number(stageCounts[stageName] || 0)
              : Number(stageCounts[stageName] || 0),
          latestAt: stageName === 'image_sourcing'
            ? layer6Run?.updatedAt || layer6Run?.completedAt || layer6Run?.startedAt || null
            : stageCounts[`${stageName}Latest`] || null,
          details: stageName === 'image_sourcing'
            ? (layer6Run
              ? [{
                  runId: cleanText(layer6Run.id, 120),
                  status: cleanText(layer6Run.status, 60),
                  finalOutcome: cleanText(layer6Run.finalOutcome, 80),
                  selectedTier: cleanText(layer6Run.selectedTier, 80),
                  candidateCount: Number(layer6Run.candidateCount || 0),
                  updatedAt: layer6Run.updatedAt ? new Date(layer6Run.updatedAt).toISOString() : null
                }]
              : [])
            : stageName === 'research_discovery' && researchDiscovery
              ? [{
                  artifactType: 'search_plan_summary',
                  title: 'Research Discovery Summary',
                  sourceUrl: '',
                  content: '',
                  createdAt: stageCounts[`${stageName}Latest`] || null,
                  query: '',
                  provider: researchDiscovery.provider,
                  model: researchDiscovery.model,
                  score: null,
                  confidence: null,
                  rank: null,
                  sectionCount: null,
                  evidenceCount: null,
                  evidenceQuote: '',
                  whyItMatters: '',
                  metadata: {
                    status: researchDiscovery.status,
                    successfulPassType: researchDiscovery.successfulPassType,
                    degradationReason: researchDiscovery.degradationReason,
                    failureReason: researchDiscovery.failureReason,
                    passSummaries: researchDiscovery.passSummaries
                  }
                }].concat((artifactsByStage[stageName] || []).filter((item) => cleanText(item.artifactType, 120).toLowerCase() !== 'search_plan'))
            : stageName === 'evidence_extraction' && evidenceExtraction
              ? [{
                  artifactType: 'evidence_bundle_summary',
                  title: 'Evidence Extraction Summary',
                  sourceUrl: '',
                  content: '',
                  createdAt: stageCounts[`${stageName}Latest`] || null,
                  query: '',
                  provider: '',
                  model: '',
                  score: null,
                  confidence: null,
                  rank: null,
                  sectionCount: null,
                  evidenceCount: evidenceExtraction.evidenceCount,
                  evidenceQuote: '',
                  whyItMatters: '',
                  metadata: {
                    extractionStatus: evidenceExtraction.extractionStatus,
                    executionOutcome: evidenceExtraction.executionOutcome,
                    isCanonical: evidenceExtraction.isCanonical,
                    failureReasons: evidenceExtraction.failureReasons,
                    editorialRiskCount: evidenceExtraction.editorialRiskCount,
                    missingEvidenceCount: evidenceExtraction.missingEvidenceCount
                  }
                }].concat((artifactsByStage[stageName] || []).filter((item) => cleanText(item.artifactType, 120).toLowerCase() !== 'evidence_bundle'))
            : (artifactsByStage[stageName] || [])
        })),
        researchDiscovery,
        evidenceExtraction,
        imageSourcing: layer6Run
          ? {
              runId: cleanText(layer6Run.id, 120),
              status: cleanText(layer6Run.status, 60),
              finalOutcome: cleanText(layer6Run.finalOutcome, 80),
              selectedTier: cleanText(layer6Run.selectedTier, 80),
              candidateCount: Number(layer6Run.candidateCount || 0),
              startedAt: layer6Run.startedAt ? new Date(layer6Run.startedAt).toISOString() : null,
              completedAt: layer6Run.completedAt ? new Date(layer6Run.completedAt).toISOString() : null,
              updatedAt: layer6Run.updatedAt ? new Date(layer6Run.updatedAt).toISOString() : null
            }
          : null,
        decisionDetails: {
          reasoning: cleanText(signal.reasoning, 4000),
          reviewNotes: cleanText(signal.reviewNotes, 4000),
          policyFlags: Array.isArray(signal.policyFlags) ? signal.policyFlags.map((f) => cleanText(f, 120)).filter(Boolean) : []
        }
      };
    });

    const summary = {
      total: runs.length,
      byStatus: runs.reduce((acc, run) => {
        const key = cleanText(run.runStatus, 80) || 'unknown';
        acc[key] = Number(acc[key] || 0) + 1;
        return acc;
      }, {}),
      byCurrentStage: runs.reduce((acc, run) => {
        const key = cleanText(run.currentStage, 80) || 'unknown';
        acc[key] = Number(acc[key] || 0) + 1;
        return acc;
      }, {})
    };

    return res.status(200).json({
      filters: { personaId: personaId || null, limit },
      summary,
      runs
    });
  } catch (error) {
    console.error('Admin persona pipeline runs error:', error);
    return res.status(500).json({
      error: 'Failed to load persona pipeline runs',
      details: cleanText(error?.message || '', 500)
    });
  }
};
