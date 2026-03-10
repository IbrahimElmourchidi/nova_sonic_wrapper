// src/infrastructure/audio/GreetingAudioService.ts
import "reflect-metadata";
import { injectable, inject } from "tsyringe";
import { join } from "node:path";
import { existsSync, statSync } from "node:fs";
import { convertAudioFileToLpcm } from "../utils/AudioConverter";
import { TOKENS } from "../config/tokens";
import type { ILogger } from "../logging/ILogger";

@injectable()
export class GreetingAudioService {
  private cachedLpcmBuffer: Buffer | null = null;

  // greeting.mp3 lives at the project root (same dir as tsconfig.json).
  // process.cwd() resolves to the project root when the process is started
  // from that directory (e.g. `node dist/main.js` or `ts-node src/main.ts`).
  private readonly assetPath = join(process.cwd(), "greeting.mp3");

  constructor(
    @inject(TOKENS.Logger)
    private readonly logger: ILogger
  ) {}

  /**
   * Call ONCE during app bootstrap before any session starts.
   * Converts greeting.mp3 → raw LPCM 16-bit 16 kHz mono and caches the result.
   * Throws immediately (crashing the process) if the file is missing or invalid.
   *
   * Logs full diagnostics so audio problems are caught before first connection:
   *   • MP3 file size
   *   • Converted LPCM buffer size and duration
   *   • First 32 bytes as hex (detects silence / all-zero conversion failures)
   *   • RMS energy (detects very quiet / inaudible audio)
   */
  async initialize(): Promise<void> {
    if (!existsSync(this.assetPath)) {
      throw new Error(
        `Greeting audio asset not found at: ${this.assetPath}\n` +
          `Place greeting.mp3 in the project root (same directory as tsconfig.json).`
      );
    }

    // ── MP3 file info ─────────────────────────────────────────────────────────
    const mp3Stats = statSync(this.assetPath);
    this.logger.info("greeting.mp3 file found", {
      path: this.assetPath,
      mp3SizeBytes: mp3Stats.size,
      mp3SizeKB: (mp3Stats.size / 1024).toFixed(1),
      lastModified: mp3Stats.mtime.toISOString(),
    });

    this.logger.info("Converting greeting.mp3 → LPCM 16-bit 16 kHz mono…");

    this.cachedLpcmBuffer = await convertAudioFileToLpcm(this.assetPath);

    // ── LPCM buffer diagnostics ───────────────────────────────────────────────
    const lpcmBytes = this.cachedLpcmBuffer.byteLength;
    const sampleCount = lpcmBytes / 2;                       // 16-bit = 2 bytes/sample
    const durationMs = Math.round((sampleCount / 16_000) * 1000);

    // First 32 bytes as hex — if all zeros the FFmpeg conversion produced silence
    const first32hex = this.cachedLpcmBuffer.slice(0, 32).toString("hex").match(/.{2}/g)!.join(" ");

    // RMS energy: sqrt(mean of squares of all 16-bit samples)
    // A value near 0 means silence; a healthy recording is typically 500–20000
    let sumOfSquares = 0;
    for (let i = 0; i < lpcmBytes - 1; i += 2) {
      const sample = this.cachedLpcmBuffer.readInt16LE(i);
      sumOfSquares += sample * sample;
    }
    const rms = Math.round(Math.sqrt(sumOfSquares / sampleCount));

    this.logger.info("greeting.mp3 → LPCM conversion complete", {
      lpcmSizeBytes: lpcmBytes,
      lpcmSizeKB: (lpcmBytes / 1024).toFixed(1),
      sampleCount,
      sampleRateHz: 16000,
      bitDepth: 16,
      channelCount: 1,
      durationMs,
      durationSec: (durationMs / 1000).toFixed(2),
      // Diagnostic fields:
      first32BytesHex: first32hex,
      rmsEnergy: rms,
      rmsNote:
        rms < 10
          ? "⚠ NEAR SILENCE — greeting.mp3 may be empty or conversion failed"
          : rms < 200
          ? "⚠ VERY QUIET — may not trigger Nova Sonic VAD reliably"
          : "✓ OK — audio energy looks normal",
    });

    if (rms < 10) {
      // Don't crash — let the dev see the full log — but warn loudly.
      this.logger.warn(
        "RMS energy is near zero. greeting.mp3 may be empty, corrupt, or the " +
          "FFmpeg LPCM conversion produced silence. Nova Sonic will not respond " +
          "to this audio. Replace greeting.mp3 with audible speech and restart.",
        { assetPath: this.assetPath, rmsEnergy: rms }
      );
    }
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

  /**
   * Returns key diagnostics about the cached greeting audio.
   * Safe to call any time after initialize() has resolved.
   * Used by SessionUseCase to log file details right before each send.
   */
  getInfo(): {
    path: string;
    lpcmSizeBytes: number;
    lpcmSizeKB: string;
    durationMs: number;
  } {
    const lpcmSizeBytes = this.cachedLpcmBuffer?.byteLength ?? 0;
    return {
      path: this.assetPath,
      lpcmSizeBytes,
      lpcmSizeKB: (lpcmSizeBytes / 1024).toFixed(1),
      durationMs: Math.round((lpcmSizeBytes / 2 / 16_000) * 1000),
    };
  }
}
