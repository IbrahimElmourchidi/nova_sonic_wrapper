// src/infrastructure/audio/GreetingAudioService.ts
import "reflect-metadata";
import { injectable, inject } from "tsyringe";
import { join } from "node:path";
import { existsSync } from "node:fs";
import { convertAudioFileToLpcm } from "../utils/AudioConverter";
import { TOKENS } from "../config/tokens";
import type { ILogger } from "../logging/ILogger";

@injectable()
export class GreetingAudioService {
  private cachedLpcmBuffer: Buffer | null = null;

  // greeting.mp3 lives at the project root (same dir as tsconfig.json).
  // process.cwd() always resolves to the project root when the process is
  // started from that directory (e.g. `node dist/main.js` or `ts-node src/main.ts`).
  private readonly assetPath = join(process.cwd(), "greeting.mp3");

  constructor(
    @inject(TOKENS.Logger)
    private readonly logger: ILogger
  ) {}

  /**
   * Call ONCE during app bootstrap before any session starts.
   * Converts greeting.mp3 → raw LPCM 16-bit 16 kHz mono and caches the result.
   * Throws immediately (crashing the process) if the file is missing.
   */
  async initialize(): Promise<void> {
    if (!existsSync(this.assetPath)) {
      throw new Error(
        `Greeting audio asset not found at: ${this.assetPath}\n` +
          `Place greeting.mp3 in the project root (same directory as tsconfig.json).`
      );
    }

    this.logger.info("Converting greeting.mp3 to LPCM…", {
      path: this.assetPath,
    });

    this.cachedLpcmBuffer = await convertAudioFileToLpcm(this.assetPath);

    this.logger.info("Greeting audio ready", {
      bytes: this.cachedLpcmBuffer.byteLength,
      durationMs: Math.round(
        (this.cachedLpcmBuffer.byteLength / 2 / 16_000) * 1000
      ),
    });
  }

  /**
   * Returns the cached LPCM buffer.
   * Throws if initialize() was never awaited at startup.
   */
  getLpcmBuffer(): Buffer {
    if (!this.cachedLpcmBuffer) {
      throw new Error(
        "GreetingAudioService.initialize() was never called. " +
          "Ensure it is awaited in bootstrap() before starting the server."
      );
    }
    return this.cachedLpcmBuffer;
  }
}