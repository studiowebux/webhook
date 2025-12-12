// deno run -A 2_kafka_publisher.ts

import { PubSub } from "./libs/kafka.ts";
import type { WebhookJobWithId } from "./libs/types.ts";
import { CryptoService } from "./libs/crypto.ts";
import { Logger } from "./libs/logger.ts";
import { validatePublishRequest } from "./libs/validation.ts";
import { getServerConfig } from "./libs/http.ts";

const logger = new Logger("kafka-publisher");
const crypto = new CryptoService("public_key.pem");
const serverConfig = getServerConfig();

const pubSub = new PubSub("webhook");
await pubSub.setupProducer();

// Note: Kafka provides built-in backpressure via producer config
// maxInFlightRequests controls how many batches can be in flight
async function enqueue(job: WebhookJobWithId): Promise<void> {
  try {
    await pubSub.sendMessage("events", [
      { key: job.target, value: JSON.stringify(job) },
    ]);
  } catch (error) {
    logger.error("Failed to enqueue message to Kafka", error);
    throw error;
  }
}

let id = 0;

async function shutdown(): Promise<void> {
  logger.info("Shutting down gracefully");
  await pubSub.close();
  Deno.exit(0);
}

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);

Deno.serve(serverConfig, async (req) => {
  const { pathname } = new URL(req.url);

  if (req.method === "POST" && pathname === "/publish") {
    try {
      const body = await req.json();
      const validation = validatePublishRequest(body);

      if (!validation.valid) {
        return new Response(validation.error, { status: 400 });
      }

      const { url, payload, target } = validation.data!;

      if (!target || typeof target !== "string") {
        return new Response("Invalid: 'target' (customer id) is required", { status: 400 });
      }

      const encryptedPayload = await crypto.encryptPayload(payload);

      logger.info("Publishing webhook to Kafka", { url, target });

      try {
        await enqueue({
          url,
          payload: encryptedPayload,
          attempt: 0,
          target,
          id: ++id,
        });
      } catch (error) {
        logger.error("Kafka producer error", error);
        return new Response("Service unavailable: failed to publish to Kafka", { status: 503 });
      }

      return new Response("Accepted", { status: 202 });
    } catch (e) {
      logger.error("Request processing failed", e);
      const errorMessage = e instanceof Error ? e.message : "Invalid request";
      return new Response(errorMessage, { status: 400 });
    }
  }

  return new Response("Not Found", { status: 404 });
});
