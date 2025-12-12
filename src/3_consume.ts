// This file is run on the customer side
// The customer has to load its private key and share the public key with the server (the provider)

import { metrics, SpanStatusCode, trace } from "@opentelemetry/api";
import { ApiError } from "./libs/error.ts";
import { CryptoService } from "./libs/crypto.ts";
import { Logger } from "./libs/logger.ts";

const logger = new Logger("customer-webhook");
const crypto = new CryptoService("public_key.pem", "private_key.pem");

const tracer = trace.getTracer("webhook", "1.0.0");
const meter = metrics.getMeter("webhook", "1.0.0");
const counter = meter.createCounter("request_processed", {
  description: "A simple counter",
  unit: "1",
});

function processMessage(req: Request) {
  return tracer.startActiveSpan("processMessage", async (span) => {
    try {
      if (Math.random() < 0.2) {
        throw new ApiError(
          "MOCK: Simulated error on customer side to force retry",
          "MOCK_ERROR",
          400,
        );
      }

      const body = await req.json();
      const decryptedPayload = await crypto.decryptPayload(body);

      logger.info("Received webhook payload", { payload: decryptedPayload });

      span?.setStatus({
        code: SpanStatusCode.OK,
        message: "Webhook received",
      });
      counter.add(1);

      if (typeof decryptedPayload === "object" && decryptedPayload !== null) {
        const payload = decryptedPayload as Record<string, unknown>;
        if (payload.timestamp) {
          span?.setAttribute("webhook.timestamp", payload.timestamp as number);
        }
      }

      return new Response("Webhook received", { status: 200 });
    } catch (error) {
      const apiError = error instanceof ApiError
        ? error
        : new ApiError(
          (error as Error).message || "An unexpected issue has occurred",
          "UNPROCESSABLE",
          409,
        );

      span.recordException(apiError);
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: apiError.message,
      });

      throw apiError;
    } finally {
      span.end();
    }
  });
}

Deno.serve({ port: 9000, onError: onError }, async (req) => {
  const span = trace.getActiveSpan();

  const { method, url } = req;
  const { pathname } = new URL(url);

  if (method === "POST" && pathname === "/webhook") {
    span?.setAttribute("http.route", "/webhook");
    span?.updateName(`${req.method} /webhook`);
    return await processMessage(req);
  }

  throw new ApiError(
    "Not Found",
    "NOT_FOUND",
    404,
  );
});

function onError(err: ApiError) {
  return new Response((err as Error).message, { status: err.code || 500 });
}
