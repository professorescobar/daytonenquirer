import {
  createEvidenceExtractionMockFunction,
  createEvidenceExtractionStartFunction,
  createGatekeeperPipeline,
  createManualGatekeeperRouteFunction,
  createResearchDiscoveryMockFunction,
  createResearchStartFunction
} from "./gatekeeper-pipeline";
import { inngest } from "./client";

export const functions = [
  createGatekeeperPipeline(inngest),
  createManualGatekeeperRouteFunction(inngest),
  createResearchStartFunction(inngest),
  createEvidenceExtractionStartFunction(inngest),
  createEvidenceExtractionMockFunction(inngest),
  createResearchDiscoveryMockFunction(inngest)
];
