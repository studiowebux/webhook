import type { ServerConfig } from "./types.ts";

export function getServerConfig(): ServerConfig {
  return {
    port: parseInt(Deno.env.get("SERVER_PORT") ?? "4242", 10),
    hostname: Deno.env.get("SERVER_HOSTNAME") ?? "0.0.0.0",
  };
}

export async function sendWebhook(
  url: string,
  payload: unknown,
  timeoutMs = 30000,
): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    return response;
  } finally {
    clearTimeout(timeout);
  }
}
