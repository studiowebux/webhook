const MAX_PAYLOAD_SIZE = parseInt(Deno.env.get("MAX_PAYLOAD_SIZE") ?? "1048576", 10); // 1MB default

export function isValidUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

export function validatePublishRequest(body: unknown): {
  valid: boolean;
  error?: string;
  data?: { url: string; payload: unknown; target?: string };
} {
  if (!body || typeof body !== "object") {
    return { valid: false, error: "Request body must be a JSON object" };
  }

  const { url, payload, target } = body as Record<string, unknown>;

  if (typeof url !== "string" || !isValidUrl(url)) {
    return { valid: false, error: "'url' must be a valid HTTP(S) URL" };
  }

  if (!payload) {
    return { valid: false, error: "'payload' is required" };
  }

  const payloadSize = JSON.stringify(payload).length;
  if (payloadSize > MAX_PAYLOAD_SIZE) {
    return {
      valid: false,
      error: `Payload size ${payloadSize} bytes exceeds maximum ${MAX_PAYLOAD_SIZE} bytes`,
    };
  }

  return {
    valid: true,
    data: { url, payload, target: target as string | undefined },
  };
}
