// deno run -A 2_redis_publisher.ts

import { publicEncrypt } from "node:crypto";
import { Buffer } from "node:buffer";
import { createClient } from "redis";

type WebhookJob = {
  url: string;
  payload: unknown;
  attempt: number;
  target: string; // Customer id - can be an api key or any other secret shared with customer to identify which Public RSA Certificate to use
};

const redis = createClient({
  url: "redis://localhost:6379",
});
redis.on("error", (err) => console.error("Redis Client Error", err));
await redis.connect();

const retryDelays = [1000, 2000, 4000, 8000, 16000]; // ms
let isShuttingDown = false;

// Cache public key in memory
let publicKeyCache: string | null = null;
async function getPublicKey(): Promise<string> {
  if (!publicKeyCache) {
    const publicKeyBase64 = await Deno.readTextFile("public_key.pem");
    publicKeyCache = Buffer.from(publicKeyBase64, "base64").toString();
  }
  return publicKeyCache;
}

// Enqueue a job
async function enqueue(job: WebhookJob) {
  await redis.rPush("webhookQueue", JSON.stringify(job));
}

// Background job processor with blocking pop
async function processQueue() {
  while (!isShuttingDown) {
    try {
      // Use BLPOP for efficient blocking queue consumption
      const result = await redis.blPop("webhookQueue", 5);

      if (!result) {
        continue;
      }

      const job = JSON.parse(result.element);

      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 30000);

        const response = await fetch(job.url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(job.payload),
          signal: controller.signal,
        });

        clearTimeout(timeout);

        if (!response.ok) {
          throw new Error(`Webhook failed with ${response.status}`);
        }

        console.log(`SUCCESS: Webhook sent to ${job.url}`);
      } catch (err) {
        console.error(err);
        job.attempt += 1;
        if (job.attempt < retryDelays.length) {
          const delayMs = retryDelays[job.attempt];
          console.warn(`RETRY: Attempt ${job.attempt} in ${delayMs}ms: ${job.url}`);

          // Use Redis sorted set for delayed retries instead of setTimeout
          const retryAt = Date.now() + delayMs;
          await redis.zAdd("webhookRetries", { score: retryAt, value: JSON.stringify(job) });
        } else {
          console.error(`FAILED: Gave up after ${job.attempt} attempts: ${job.url}`);
          await redis.rPush("webhookDLQ", JSON.stringify(job));
        }
      }
    } catch (err) {
      console.error("Queue processing error:", err);
      await delay(1000);
    }
  }
  console.log("Queue processor stopped");
}

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Process delayed retries from sorted set
async function processRetries() {
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
      console.error("Retry processor error:", err);
      await delay(1000);
    }
  }
  console.log("Retry processor stopped");
}

// Graceful shutdown handler
async function shutdown() {
  if (isShuttingDown) return;

  console.log("\nShutting down gracefully...");
  isShuttingDown = true;

  await delay(100);

  const queueLength = await redis.lLen("webhookQueue");
  const retryCount = await redis.zCard("webhookRetries");

  if (queueLength > 0 || retryCount > 0) {
    console.log(`WARNING: ${queueLength} jobs in queue, ${retryCount} pending retries`);
  }

  await redis.quit();
  Deno.exit(0);
}

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);

// Start processors
processQueue();
processRetries();

// Validate webhook URL
function isValidUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

// Start HTTP server using official Deno.serve
Deno.serve({ port: 4242, hostname: "0.0.0.0" }, async (req) => {
  const { pathname } = new URL(req.url);

  if (req.method === "POST" && pathname === "/publish") {
    try {
      const body = await req.json();
      const { url, payload } = body;

      if (typeof url !== "string" || !isValidUrl(url)) {
        return new Response("Invalid: 'url' must be a valid HTTP(S) URL", { status: 400 });
      }

      if (!payload) {
        return new Response("Invalid: 'payload' is required", { status: 400 });
      }

      const publicKey = await getPublicKey();
      const encryptedMessage = publicEncrypt(
        publicKey,
        JSON.stringify({
          payload: payload,
          timestamp: new Date().getTime(),
        }),
      );
      const encryptedPayload = Buffer.from(encryptedMessage).toString("base64");

      console.log("Encrypted Message (base64):", encryptedPayload);

      await enqueue({
        url,
        payload: encryptedPayload,
        attempt: 0,
        target: "customer-1",
      });
      return new Response("Accepted", { status: 202 });
    } catch (e) {
      console.error(e);
      const errorMessage = e instanceof Error ? e.message : "Invalid request";
      return new Response(errorMessage, { status: 400 });
    }
  }

  return new Response("Not Found", { status: 404 });
});
