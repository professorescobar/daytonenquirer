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
  createDraftWritingStartFunction
} from "./gatekeeper-pipeline";
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
  createEvidenceExtractionMockFunction(inngest),
  createResearchDiscoveryMockFunction(inngest)
];
