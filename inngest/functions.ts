import {
  createQuotaPacingIntakeFunction,
  createQuotaPacingReleaseSchedulerFunction,
  createClusterUpdateStartFunction,
  createEvidenceExtractionMockFunction,
  createEvidenceExtractionStartFunction,
  createGatekeeperPipeline,
  createManualGatekeeperRouteFunction,
  createResearchDiscoveryMockFunction,
  createResearchStartFunction,
  createStoryPlanningStartFunction,
  createDraftWritingStartFunction,
  createImageSourcingStartFunction
} from "./gatekeeper-pipeline";
import {
  createDictionarySubstrateDispatchFunction as createDictionarySubstrateDispatchFunctionFromModule,
  createDictionarySubstrateDispatchSchedulerFunction as createDictionarySubstrateDispatchSchedulerFunctionFromModule,
  createDictionarySubstrateExtractionArtifactFunction as createDictionarySubstrateExtractionArtifactFunctionFromModule,
  createDictionarySubstrateFreshnessSchedulerFunction as createDictionarySubstrateFreshnessSchedulerFunctionFromModule,
  createDictionarySubstrateFreshnessScanFunction as createDictionarySubstrateFreshnessScanFunctionFromModule,
  createDictionarySubstrateMergeArtifactFunction as createDictionarySubstrateMergeArtifactFunctionFromModule,
  createDictionarySubstratePromotionArtifactFunction as createDictionarySubstratePromotionArtifactFunctionFromModule,
  createDictionarySubstrateRootIngestionFunction as createDictionarySubstrateRootIngestionFunctionFromModule
} from "./dictionary-substrate";
import { inngest } from "./client";

export const functions = [
  createQuotaPacingIntakeFunction(inngest),
  createQuotaPacingReleaseSchedulerFunction(inngest),
  createGatekeeperPipeline(inngest),
  createManualGatekeeperRouteFunction(inngest),
  createResearchStartFunction(inngest),
  createClusterUpdateStartFunction(inngest),
  createEvidenceExtractionStartFunction(inngest),
  createStoryPlanningStartFunction(inngest),
  createDraftWritingStartFunction(inngest),
  createImageSourcingStartFunction(inngest),
  createEvidenceExtractionMockFunction(inngest),
  createResearchDiscoveryMockFunction(inngest),
  createDictionarySubstrateDispatchSchedulerFunctionFromModule(inngest),
  createDictionarySubstrateDispatchFunctionFromModule(inngest),
  createDictionarySubstrateFreshnessSchedulerFunctionFromModule(inngest),
  createDictionarySubstrateRootIngestionFunctionFromModule(inngest),
  createDictionarySubstrateExtractionArtifactFunctionFromModule(inngest),
  createDictionarySubstrateMergeArtifactFunctionFromModule(inngest),
  createDictionarySubstrateFreshnessScanFunctionFromModule(inngest),
  createDictionarySubstratePromotionArtifactFunctionFromModule(inngest)
];
