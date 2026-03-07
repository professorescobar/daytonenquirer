const crypto = require('crypto');

const TOPIC_ENGINE_STAGES = [
  'topic_qualification',
  'research_discovery',
  'evidence_extraction',
  'story_planning',
  'draft_writing',
  'final_review'
];

const SECTION_OPTIONS = [
  'local',
  'national',
  'world',
  'business',
  'sports',
  'health',
  'entertainment',
  'technology'
];

const PERSONA_TOKEN = '{{persona_prompt}}';
const PROMPT_SCHEMA_VERSION = '2026-03-07.v1';

const CONTRACT_PROMPTS = {
  topic_qualification: [
    'Return strict JSON only.',
    'Required keys: is_newsworthy, is_local, confidence, category, relation_to_archive, event_key, action, next_step, policy_flags, reasoning.',
    'Allowed relation_to_archive: none|duplicate|update|follow_up.',
    'Allowed action: reject|watch|promote.',
    'Allowed next_step: none|research_discovery|cluster_update|story_planning.'
  ].join('\n'),
  research_discovery: [
    'Return strict JSON only.',
    'Required shape: {"queries":["..."]}.',
    'Return 3 to 5 focused, verifiable reporting queries.',
    'No markdown and no extra keys.'
  ].join('\n'),
  evidence_extraction: [
    'Return strict JSON only.',
    'Required shape: {"claims":[{"claim":"...","sourceUrl":"...","evidenceQuote":"...","confidence":0.0,"whyItMatters":"..."}]}.',
    'Use only provided sources.',
    'sourceUrl must exactly match one provided URL.',
    'Return 2 to 5 claims max.'
  ].join('\n'),
  story_planning: [
    'Return strict JSON only.',
    'Required shape: {"angle":"...","narrativeStrategy":"...","sections":[{"heading":"...","summary":"...","evidenceSourceUrls":["..."]}],"uncertaintyNotes":["..."],"missingInformation":["..."]}.',
    'Use only provided evidence.',
    'Return 3 to 6 sections.',
    'Each section must include at least one exact evidenceSourceUrls match.'
  ].join('\n'),
  draft_writing: [
    'Return output in the exact JSON/schema contract required by this stage configuration.',
    'Do not fabricate facts, dates, names, or quotes.',
    'Only include claims supported by provided planning/evidence context.'
  ].join('\n'),
  final_review: [
    'Return output in the exact JSON/schema contract required by this stage configuration.',
    'Assess factual grounding, policy compliance, and editorial quality.',
    'Flag unsupported claims and policy issues deterministically.'
  ].join('\n')
};

function cleanText(value, max = 12000) {
  return String(value || '').trim().slice(0, max);
}

function normalizeStageName(value) {
  const stage = cleanText(value, 120).toLowerCase();
  return TOPIC_ENGINE_STAGES.includes(stage) ? stage : '';
}

function normalizeSection(value) {
  const section = cleanText(value, 120).toLowerCase();
  return SECTION_OPTIONS.includes(section) ? section : '';
}

function normalizeScopeType(value) {
  const scope = cleanText(value, 120).toLowerCase();
  if (scope === 'global' || scope === 'section' || scope === 'persona') return scope;
  return '';
}

function normalizeGuidance(value) {
  return cleanText(value, 50000);
}

function buildRuntimeContextBlock(runtimeContext) {
  if (!runtimeContext || typeof runtimeContext !== 'object' || Array.isArray(runtimeContext)) {
    return '';
  }
  const entries = Object.entries(runtimeContext)
    .map(([key, value]) => [cleanText(key, 80), cleanText(value, 3000)])
    .filter(([key, value]) => key && value);
  if (!entries.length) return '';
  return entries.map(([key, value]) => `${key}: ${value}`).join('\n');
}

function hashText(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex');
}

function buildPromptSourceVersion(parts) {
  return hashText(JSON.stringify(parts)).slice(0, 20);
}

function compileStagePrompt(options = {}) {
  const stageName = normalizeStageName(options.stageName);
  const section = normalizeSection(options.section);
  const globalPrompt = normalizeGuidance(options.globalPrompt);
  const sectionPrompt = normalizeGuidance(options.sectionPrompt);
  const personaPrompt = normalizeGuidance(options.personaPrompt);
  const runtimeContextBlock = buildRuntimeContextBlock(options.runtimeContext);
  const warnings = [];

  if (!stageName) {
    return {
      ok: false,
      stageName: '',
      section: '',
      compiledPrompt: '',
      layerBreakdown: [],
      warnings: ['invalid_stage_name'],
      promptHash: '',
      promptSourceVersion: ''
    };
  }

  const contractPrompt = CONTRACT_PROMPTS[stageName] || '';
  let resolvedGlobalPrompt = globalPrompt;
  let resolvedSectionPrompt = sectionPrompt;
  let usedInterweave = false;

  if (personaPrompt) {
    if (resolvedGlobalPrompt.includes(PERSONA_TOKEN)) {
      resolvedGlobalPrompt = resolvedGlobalPrompt.split(PERSONA_TOKEN).join(personaPrompt);
      usedInterweave = true;
    }
    if (resolvedSectionPrompt.includes(PERSONA_TOKEN)) {
      resolvedSectionPrompt = resolvedSectionPrompt.split(PERSONA_TOKEN).join(personaPrompt);
      usedInterweave = true;
    }
  } else if (resolvedGlobalPrompt.includes(PERSONA_TOKEN) || resolvedSectionPrompt.includes(PERSONA_TOKEN)) {
    warnings.push('persona_token_present_but_persona_prompt_empty');
    resolvedGlobalPrompt = resolvedGlobalPrompt.split(PERSONA_TOKEN).join('');
    resolvedSectionPrompt = resolvedSectionPrompt.split(PERSONA_TOKEN).join('');
  }

  const layerBreakdown = [];
  if (contractPrompt) layerBreakdown.push({ layer: 'contract', text: contractPrompt });
  if (resolvedGlobalPrompt) layerBreakdown.push({ layer: 'global', text: resolvedGlobalPrompt });
  if (resolvedSectionPrompt) layerBreakdown.push({ layer: 'section', text: resolvedSectionPrompt });
  if (personaPrompt && !usedInterweave) layerBreakdown.push({ layer: 'persona', text: personaPrompt });
  if (runtimeContextBlock) layerBreakdown.push({ layer: 'runtime', text: runtimeContextBlock });

  const compiledPrompt = layerBreakdown
    .map((item) => `[${item.layer.toUpperCase()}]\n${item.text}`)
    .join('\n\n');

  const promptHash = hashText(compiledPrompt);
  const promptSourceVersion = buildPromptSourceVersion({
    schema: PROMPT_SCHEMA_VERSION,
    stageName,
    section: section || null,
    versions: options.sourceVersions || {},
    hashes: {
      contract: hashText(contractPrompt),
      global: hashText(globalPrompt),
      section: hashText(sectionPrompt),
      persona: hashText(personaPrompt)
    }
  });

  return {
    ok: true,
    stageName,
    section: section || null,
    compiledPrompt,
    layerBreakdown,
    warnings,
    promptHash,
    promptSourceVersion,
    hasEditableGuidance: Boolean(globalPrompt || sectionPrompt || personaPrompt),
    usedInterweave
  };
}

function isPromptLayersEnabled() {
  return String(process.env.PROMPT_LAYERS_ENABLED || '')
    .trim()
    .toLowerCase() === 'true';
}

module.exports = {
  TOPIC_ENGINE_STAGES,
  SECTION_OPTIONS,
  CONTRACT_PROMPTS,
  PERSONA_TOKEN,
  PROMPT_SCHEMA_VERSION,
  normalizeStageName,
  normalizeSection,
  normalizeScopeType,
  normalizeGuidance,
  compileStagePrompt,
  isPromptLayersEnabled
};
