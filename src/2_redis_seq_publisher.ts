// deno run -A 2_redis_seq_publisher.ts

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

// Background job processor with sequential retries
async function processQueue() {
  while (!isShuttingDown) {
    try {
      const result = await redis.blPop("webhookQueue", 5);

      if (!result) {
        continue;
      }

      const job = JSON.parse(result.element);

      const maxRetries = 5;
      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 30000);

          const res = await fetch(job.url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(job.payload),
            signal: controller.signal,
          });

          clearTimeout(timeout);

          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          console.log(`SUCCESS: Webhook delivered on attempt ${attempt} for ${job.url}`);
          break;
        } catch (err) {
          console.warn(`WARNING: Attempt ${attempt} failed for ${job.url}:`, err.message);
          if (attempt < maxRetries) {
            const backoff = Math.pow(2, attempt) * 1000;
            await delay(backoff);
          } else {
            console.error(`FAILED: Max retries reached for ${job.url}`);
            await redis.rPush("webhookDLQ", JSON.stringify(job));
          }
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

// Graceful shutdown handler
async function shutdown() {
  if (isShuttingDown) return;

  console.log("\nShutting down gracefully...");
  isShuttingDown = true;

  await delay(100);

  const queueLength = await redis.lLen("webhookQueue");
  if (queueLength > 0) {
    console.log(`WARNING: ${queueLength} jobs remaining in queue`);
  }

  await redis.quit();
  Deno.exit(0);
}

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);

// Start the queue processor
processQueue();

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
