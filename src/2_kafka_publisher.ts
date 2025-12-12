// deno run -A 2_kafka_publisher.ts

import { publicEncrypt } from "node:crypto";
import { Buffer } from "node:buffer";

import { PubSub } from "./libs/kafka.ts";

type WebhookJob = {
  url: string;
  payload: unknown;
  attempt: number;
  target: string; // Customer id - can be an api key or any other secret shared with customer to identify which Public RSA Certificate to use
  id: number;
};

const pubSub = new PubSub("webhook");
await pubSub.setupProducer();

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
  await pubSub.sendMessage("events", [
    { key: job.target, value: JSON.stringify(job) },
  ]);
}

// Validate webhook URL
function isValidUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

// For local debugging
let id = 0;

// Graceful shutdown
async function shutdown() {
  console.log("\nShutting down gracefully...");
  await pubSub.close();
  Deno.exit(0);
}

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);

// Start HTTP server using official Deno.serve
Deno.serve({ port: 4242, hostname: "0.0.0.0" }, async (req) => {
  const { pathname } = new URL(req.url);

  if (req.method === "POST" && pathname === "/publish") {
    try {
      const body = await req.json();
      const { url, payload, target } = body;

      if (typeof url !== "string" || !isValidUrl(url)) {
        return new Response("Invalid: 'url' must be a valid HTTP(S) URL", { status: 400 });
      }

      if (!payload) {
        return new Response("Invalid: 'payload' is required", { status: 400 });
      }

      if (!target || typeof target !== "string") {
        return new Response("Invalid: 'target' (customer id) is required", { status: 400 });
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
        target,
        id: ++id,
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
