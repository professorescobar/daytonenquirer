import { serve } from "inngest/node";
import { inngest } from "../inngest/client";
import { functions } from "../inngest/functions";

export default serve({
  client: inngest,
  functions
});
