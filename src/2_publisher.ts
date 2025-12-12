// deno run -A 2_publisher.ts
// The retry approach is NOT Sequential. To get a fully sequential you need to handle the retry in-code, (instead of pushing in the queue),
// See redis seq example for details.

import type { WebhookJob } from "./libs/types.ts";
import { CryptoService } from "./libs/crypto.ts";
import { Logger } from "./libs/logger.ts";
import { validatePublishRequest } from "./libs/validation.ts";
import { getRetryConfig, delay } from "./libs/retry.ts";
import { sendWebhook, getServerConfig } from "./libs/http.ts";

const logger = new Logger("in-memory-publisher");
const crypto = new CryptoService("public_key.pem");
const retryConfig = getRetryConfig();
const serverConfig = getServerConfig();

const MAX_QUEUE_SIZE = parseInt(Deno.env.get("MAX_QUEUE_SIZE") ?? "10000", 10);
const queue: WebhookJob[] = [];
const retryTimers = new Set<number>();
let isShuttingDown = false;

// Enqueue a job with backpressure
function enqueue(url: string, payload: unknown): boolean {
  if (queue.length >= MAX_QUEUE_SIZE) {
    logger.warn("Queue full, rejecting job", { queueSize: queue.length, maxSize: MAX_QUEUE_SIZE });
    return false;
  }
  queue.push({ url, payload, attempt: 0, target: "customer-1" });
  return true;
}

// Background job processor
async function processQueue(): Promise<void> {
  while (!isShuttingDown) {
    const job = queue.shift();
    if (!job) {
      await delay(100);
      continue;
    }

    try {
      const res = await sendWebhook(job.url, job.payload);

      if (!res.ok) throw new Error(`Failed: ${res.status}`);
      logger.success("Webhook delivered", { url: job.url, attempt: job.attempt });
    } catch (err) {
      logger.error("Webhook delivery failed", err, { url: job.url, attempt: job.attempt });

      const nextAttempt = job.attempt + 1;
      if (nextAttempt < retryConfig.delays.length) {
        const delayMs = retryConfig.delays[job.attempt];
        logger.warn("Scheduling retry", { url: job.url, attempt: nextAttempt, delayMs });

        const timerId = setTimeout(() => {
          retryTimers.delete(timerId);
          queue.push({ ...job, attempt: nextAttempt });
        }, delayMs);
        retryTimers.add(timerId);
      } else {
        logger.error("Max retries exhausted, moving to DLQ", undefined, { job });
      }
    }
  }
  logger.info("Queue processor stopped");
}

// Graceful shutdown handler
async function shutdown(): Promise<void> {
  if (isShuttingDown) return;

  logger.info("Shutting down gracefully");
  isShuttingDown = true;

  for (const timerId of retryTimers) {
    clearTimeout(timerId);
  }
  retryTimers.clear();

  if (queue.length > 0) {
    logger.warn("Jobs remaining in queue", { count: queue.length });
    for (const job of queue) {
      logger.error("Dead letter on shutdown", undefined, { job });
    }
  }

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
      const enqueued = enqueue(url, encryptedPayload);

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
