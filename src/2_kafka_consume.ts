// This file is ran by the provider, I guess it is an overkilled setup, but if you have a lot of customer, it might makes sens to have this kind of setup ?

import { PubSub } from "./libs/kafka.ts";
import type { WebhookJobWithId } from "./libs/types.ts";
import { Logger } from "./libs/logger.ts";
import { calculateBackoff } from "./libs/retry.ts";
import { sendWebhook } from "./libs/http.ts";

const logger = new Logger("kafka-consumer");
const maxRetries = parseInt(Deno.env.get("MAX_RETRIES") ?? "5", 10);

const pubSub = new PubSub("webhook");
await pubSub.setupConsumer(["events"]);

async function shutdown() {
  logger.info("Shutting down gracefully");
  await pubSub.close();
  Deno.exit(0);
}

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);

await pubSub.consume(
  (message: string, { heartbeat }: { heartbeat: () => Promise<void> }) =>
    process(message, { heartbeat }),
);

async function process(
  message: string,
  { heartbeat }: { heartbeat: () => Promise<void> },
) {
  const job: WebhookJobWithId = JSON.parse(message);

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await sendWebhook(job.url, job.payload);

      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      logger.success("Webhook delivered", { id: job.id, target: job.target, attempt });
      break;
    } catch (err) {
      if (attempt < maxRetries) {
        await heartbeat();
        const backoff = calculateBackoff(attempt);
        logger.warn("Webhook delivery attempt failed", {
          id: job.id,
          target: job.target,
          attempt,
          backoff,
          error: (err as Error).message,
        });
        await delay(backoff);
      } else {
        logger.error("Max retries exhausted", undefined, { id: job.id, target: job.target });
      }
    }
  }
}

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
