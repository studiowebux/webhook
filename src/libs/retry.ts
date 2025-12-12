import type { RetryConfig } from "./types.ts";

export function getRetryConfig(): RetryConfig {
  const delaysEnv = Deno.env.get("RETRY_DELAYS");
  const delays = delaysEnv
    ? delaysEnv.split(",").map((d) => parseInt(d.trim(), 10))
    : [1000, 2000, 4000, 8000, 16000];

  const maxRetries = parseInt(Deno.env.get("MAX_RETRIES") ?? "5", 10);

  return { delays, maxRetries };
}

export function calculateBackoff(attempt: number): number {
  return Math.pow(2, attempt) * 1000;
}

export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<T> {
  const controller = signal ? undefined : new AbortController();
  const timeoutSignal = signal ?? controller!.signal;

  const timeoutId = setTimeout(() => {
    if (controller) {
      controller.abort();
    }
  }, timeoutMs);

  try {
    return await promise;
  } finally {
    clearTimeout(timeoutId);
  }
}
