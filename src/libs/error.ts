export class ApiError extends Error {
  public readonly code: number;
  public readonly extra: Record<string, unknown>;
  public readonly devMessage: string;

  constructor(
    message = "UNKNOWN_ERROR",
    name = "UNKNOWN_ERROR",
    code = 500,
    extra: Record<string, unknown> = {},
    devMessage = "",
  ) {
    super(message);

    this.name = name;
    this.cause = name;
    this.code = code;
    this.extra = extra;
    this.devMessage = devMessage;

    Object.setPrototypeOf(this, ApiError.prototype);
  }

  toJSON() {
    return {
      name: this.name,
      message: this.message,
      code: this.code,
      extra: this.extra,
      devMessage: this.devMessage,
    };
  }
}
