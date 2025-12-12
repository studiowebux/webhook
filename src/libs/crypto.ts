import { publicEncrypt, privateDecrypt } from "node:crypto";
import { Buffer } from "node:buffer";

export class CryptoService {
  private publicKeyCache: string | null = null;
  private privateKeyCache: string | null = null;

  constructor(
    private publicKeyPath: string,
    private privateKeyPath?: string,
  ) {}

  async getPublicKey(): Promise<string> {
    if (!this.publicKeyCache) {
      const publicKeyBase64 = await Deno.readTextFile(this.publicKeyPath);
      this.publicKeyCache = Buffer.from(publicKeyBase64, "base64").toString();
    }
    return this.publicKeyCache;
  }

  async getPrivateKey(): Promise<string> {
    if (!this.privateKeyPath) {
      throw new Error("Private key path not configured");
    }

    if (!this.privateKeyCache) {
      const privateKeyBase64 = await Deno.readTextFile(this.privateKeyPath);
      this.privateKeyCache = Buffer.from(privateKeyBase64, "base64").toString();
    }
    return this.privateKeyCache;
  }

  async encryptPayload(payload: unknown): Promise<string> {
    const publicKey = await this.getPublicKey();
    const encryptedMessage = publicEncrypt(
      publicKey,
      JSON.stringify({
        payload: payload,
        timestamp: Date.now(),
      }),
    );
    return Buffer.from(encryptedMessage).toString("base64");
  }

  async decryptPayload(encryptedPayload: string): Promise<unknown> {
    const privateKey = await this.getPrivateKey();
    const decryptedMessage = privateDecrypt(
      privateKey,
      Buffer.from(encryptedPayload, "base64"),
    );
    const plaintext = Buffer.from(decryptedMessage).toString();
    return JSON.parse(plaintext);
  }
}
