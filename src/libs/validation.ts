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

  return {
    valid: true,
    data: { url, payload, target: target as string | undefined },
  };
}
