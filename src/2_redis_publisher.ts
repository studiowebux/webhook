// deno run -A 2_redis_publisher.ts

import { createClient } from "redis";
import type { WebhookJob } from "./libs/types.ts";
import { CryptoService } from "./libs/crypto.ts";
import { Logger } from "./libs/logger.ts";
import { validatePublishRequest } from "./libs/validation.ts";
import { getRetryConfig, delay } from "./libs/retry.ts";
import { sendWebhook, getServerConfig } from "./libs/http.ts";

const logger = new Logger("redis-publisher");
const crypto = new CryptoService("public_key.pem");
const retryConfig = getRetryConfig();
const serverConfig = getServerConfig();

const MAX_QUEUE_SIZE = parseInt(Deno.env.get("MAX_QUEUE_SIZE") ?? "10000", 10);
const redisUrl = Deno.env.get("REDIS_URL") ?? "redis://localhost:6379";
const redis = createClient({ url: redisUrl });

redis.on("error", (err) => logger.error("Redis client error", err));
await redis.connect();

let isShuttingDown = false;

// Enqueue a job with backpressure
async function enqueue(job: WebhookJob): Promise<boolean> {
  const queueLength = await redis.lLen("webhookQueue");
  if (queueLength >= MAX_QUEUE_SIZE) {
    logger.warn("Queue full, rejecting job", { queueSize: queueLength, maxSize: MAX_QUEUE_SIZE });
    return false;
  }
  await redis.rPush("webhookQueue", JSON.stringify(job));
  return true;
}

// Background job processor with blocking pop
async function processQueue(): Promise<void> {
  while (!isShuttingDown) {
    try {
      const result = await redis.blPop("webhookQueue", 5);

      if (!result) {
        continue;
      }

      const job: WebhookJob = JSON.parse(result.element);

      try {
        const response = await sendWebhook(job.url, job.payload);

        if (!response.ok) {
          throw new Error(`Webhook failed with ${response.status}`);
        }

        logger.success("Webhook delivered", { url: job.url, attempt: job.attempt });
      } catch (err) {
        logger.error("Webhook delivery failed", err, { url: job.url, attempt: job.attempt });

        job.attempt += 1;
        if (job.attempt < retryConfig.delays.length) {
          const delayMs = retryConfig.delays[job.attempt];
          logger.warn("Scheduling retry", { url: job.url, attempt: job.attempt, delayMs });

          const retryAt = Date.now() + delayMs;
          await redis.zAdd("webhookRetries", { score: retryAt, value: JSON.stringify(job) });
        } else {
          logger.error("Max retries exhausted, moving to DLQ", undefined, { job });
          await redis.rPush("webhookDLQ", JSON.stringify(job));
        }
      }
    } catch (err) {
      logger.error("Queue processing error", err);
      await delay(1000);
    }
  }
  logger.info("Queue processor stopped");
}

// Process delayed retries from sorted set
async function processRetries(): Promise<void> {
  while (!isShuttingDown) {
    try {
      const now = Date.now();
      const ready = await redis.zRangeByScore("webhookRetries", 0, now);

      for (const jobString of ready) {
        await redis.zRem("webhookRetries", jobString);
        await redis.rPush("webhookQueue", jobString);
      }

      if (ready.length === 0) {
        await delay(1000);
      }
    } catch (err) {
      logger.error("Retry processor error", err);
      await delay(1000);
    }
  }
  logger.info("Retry processor stopped");
}

// Graceful shutdown handler
async function shutdown(): Promise<void> {
  if (isShuttingDown) return;

  logger.info("Shutting down gracefully");
  isShuttingDown = true;

  await delay(100);

  const queueLength = await redis.lLen("webhookQueue");
  const retryCount = await redis.zCard("webhookRetries");

  if (queueLength > 0 || retryCount > 0) {
    logger.warn("Jobs remaining", { queueLength, retryCount });
  }

  await redis.quit();
  Deno.exit(0);
}

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);

processQueue();
processRetries();

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
      const enqueued = await enqueue({
        url,
        payload: encryptedPayload,
        attempt: 0,
        target: "customer-1",
      });

      if (!enqueued) {
        return new Response("Service unavailable: queue is full", { status: 503 });
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
