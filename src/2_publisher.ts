// deno run -A 2_publisher.ts
// The retry approach is NOT Sequential. To get a fully sequential you need to handle the retry in-code, (instead of pushing in the queue),
// See redis seq example for details.

import { publicEncrypt } from "node:crypto";
import { Buffer } from "node:buffer";

type WebhookJob = {
  url: string;
  payload: unknown;
  attempt: number;
  target: string; // Customer id - can be an api key or any other secret shared with customer to identify which Public RSA Certificate to use
};

const retryDelays = [1000, 2000, 4000, 8000, 16000]; // ms
const queue: WebhookJob[] = [];
const retryTimers = new Set<number>();
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
function enqueue(url: string, payload: unknown) {
  queue.push({ url, payload, attempt: 0, target: "customer-1" });
}

// Background job processor
async function processQueue() {
  while (!isShuttingDown) {
    const job = queue.shift();
    if (!job) {
      await delay(100);
      continue;
    }

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

      if (!res.ok) throw new Error(`Failed: ${res.status}`);
      console.log(`SUCCESS: Webhook sent to ${job.url}`);
    } catch (err) {
      console.error(err);
      const nextAttempt = job.attempt + 1;
      if (nextAttempt < retryDelays.length) {
        const delayMs = retryDelays[job.attempt];
        console.warn(`RETRY: Attempt ${nextAttempt} in ${delayMs}ms: ${job.url}`);
        const timerId = setTimeout(() => {
          retryTimers.delete(timerId);
          queue.push({ ...job, attempt: nextAttempt });
        }, delayMs);
        retryTimers.add(timerId);
      } else {
        console.error(`FAILED: Gave up after ${nextAttempt} attempts: ${job.url}`);
        console.error(`Dead letter: ${JSON.stringify(job)}`);
      }
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

  // Clear all pending retry timers
  for (const timerId of retryTimers) {
    clearTimeout(timerId);
  }
  retryTimers.clear();

  // Log remaining queue
  if (queue.length > 0) {
    console.log(`WARNING: ${queue.length} jobs remaining in queue`);
    for (const job of queue) {
      console.error(`Dead letter: ${JSON.stringify(job)}`);
    }
  }

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

      enqueue(url, encryptedPayload);
      return new Response("Accepted", { status: 202 });
    } catch (e) {
      console.error(e);
      const errorMessage = e instanceof Error ? e.message : "Invalid request";
      return new Response(errorMessage, { status: 400 });
    }
  }

  return new Response("Not Found", { status: 404 });
});
