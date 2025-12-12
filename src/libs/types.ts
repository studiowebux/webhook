export interface WebhookJob {
  url: string;
  payload: unknown;
  attempt: number;
  target: string;
}

export interface WebhookJobWithId extends WebhookJob {
  id: number;
}

export interface RetryConfig {
  delays: number[];
  maxRetries: number;
}

export interface ServerConfig {
  port: number;
  hostname: string;
}
