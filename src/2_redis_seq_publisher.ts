// deno run -A 2_redis_seq_publisher.ts

import { createClient } from "redis";
import type { WebhookJob } from "./libs/types.ts";
import { CryptoService } from "./libs/crypto.ts";
import { Logger } from "./libs/logger.ts";
import { validatePublishRequest } from "./libs/validation.ts";
import { calculateBackoff } from "./libs/retry.ts";
import { sendWebhook, getServerConfig } from "./libs/http.ts";

const logger = new Logger("redis-seq-publisher");
const crypto = new CryptoService("public_key.pem");
const serverConfig = getServerConfig();
const maxRetries = parseInt(Deno.env.get("MAX_RETRIES") ?? "5", 10);

const redisUrl = Deno.env.get("REDIS_URL") ?? "redis://localhost:6379";
const redis = createClient({ url: redisUrl });

redis.on("error", (err) => logger.error("Redis client error", err));
await redis.connect();

let isShuttingDown = false;

// Enqueue a job
async function enqueue(job: WebhookJob) {
  await redis.rPush("webhookQueue", JSON.stringify(job));
}

// Background job processor with sequential retries
async function processQueue() {
  while (!isShuttingDown) {
    try {
      const result = await redis.blPop("webhookQueue", 5);

      if (!result) {
        continue;
      }

      const job: WebhookJob = JSON.parse(result.element);

      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
          const res = await sendWebhook(job.url, job.payload);

          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          logger.success("Webhook delivered", { url: job.url, attempt });
          break;
        } catch (err) {
          logger.warn("Webhook delivery attempt failed", { url: job.url, attempt, error: (err as Error).message });

          if (attempt < maxRetries) {
            const backoff = calculateBackoff(attempt);
            await delay(backoff);
          } else {
            logger.error("Max retries exhausted, moving to DLQ", undefined, { job });
            await redis.rPush("webhookDLQ", JSON.stringify(job));
          }
        }
      }
    } catch (err) {
      logger.error("Queue processing error", err);
      await delay(1000);
    }
  }
  logger.info("Queue processor stopped");
}

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Graceful shutdown handler
async function shutdown() {
  if (isShuttingDown) return;

  logger.info("Shutting down gracefully");
  isShuttingDown = true;

  await delay(100);

  const queueLength = await redis.lLen("webhookQueue");
  if (queueLength > 0) {
    logger.warn("Jobs remaining in queue", { queueLength });
  }

  await redis.quit();
  Deno.exit(0);
}

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);

processQueue();

Deno.serve(serverConfig, async (req) => {
  const { pathname } = new URL(req.url);

  if (req.method === "POST" && pathname === "/publish") {
    try {
      const body = await req.json();
      const validation = validatePublishRequest(body);

      if (!validation.valid) {
        return new Response(validation.error, { status: 400 });
      }

      const { url, payload } = validation.data!;
      const encryptedPayload = await crypto.encryptPayload(payload);

      logger.info("Publishing webhook", { url });
      await enqueue({
        url,
        payload: encryptedPayload,
        attempt: 0,
        target: "customer-1",
      });

      return new Response("Accepted", { status: 202 });
    } catch (e) {
      logger.error("Request processing failed", e);
      const errorMessage = e instanceof Error ? e.message : "Invalid request";
      return new Response(errorMessage, { status: 400 });
    }
  }

  return new Response("Not Found", { status: 404 });
});
