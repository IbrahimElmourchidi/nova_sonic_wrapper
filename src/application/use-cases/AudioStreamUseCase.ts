// src/application/use-cases/AudioStreamUseCase.ts
import "reflect-metadata";
import { inject, injectable } from "tsyringe";

import { TOKENS } from "../../infrastructure/config/tokens";
import type { ILogger } from "../../infrastructure/logging/ILogger";
import type { AppConfig } from "../../infrastructure/config/AppConfig";
import { SessionUseCase } from "./SessionUseCase";

interface AudioQueue {
  chunks: Buffer[];
  isProcessing: boolean;
  isActive: boolean;
}

/**
 * Manages per-session audio buffer queues.
 * Handles back-pressure, batching, and graceful draining.
 */
@injectable()
export class AudioStreamUseCase {
  private readonly queues = new Map<string, AudioQueue>();

  constructor(
    @inject(TOKENS.SessionUseCase)
    private readonly sessionUseCase: SessionUseCase,

    @inject(TOKENS.AppConfig)
    private readonly config: AppConfig,

    @inject(TOKENS.Logger)
    private readonly logger: ILogger
  ) {}

  initQueue(sessionId: string): void {
    this.queues.set(sessionId, {
      chunks: [],
      isProcessing: false,
      isActive: true,
    });
  }

  enqueueAudio(sessionId: string, audioData: Buffer): void {
    const queue = this.queues.get(sessionId);
    if (!queue || !queue.isActive) return;

    const { audioQueueMaxSize } = this.config.session;

    if (queue.chunks.length >= audioQueueMaxSize) {
      // Drop oldest chunk (back-pressure strategy)
      queue.chunks.shift();
      this.logger.warn("Audio queue full – dropping oldest chunk", {
        sessionId,
      });
    }

    queue.chunks.push(audioData);
    this.processQueue(sessionId);
  }

  destroyQueue(sessionId: string): void {
    const queue = this.queues.get(sessionId);
    if (queue) {
      queue.isActive = false;
      queue.chunks = [];
    }
    this.queues.delete(sessionId);
  }

  private processQueue(sessionId: string): void {
    const queue = this.queues.get(sessionId);
    if (!queue || queue.isProcessing || queue.chunks.length === 0 || !queue.isActive) {
      return;
    }

    queue.isProcessing = true;

    const processBatch = async (): Promise<void> => {
      try {
        const { audioQueueBatchSize } = this.config.session;
        let processed = 0;

        while (
          queue.chunks.length > 0 &&
          processed < audioQueueBatchSize &&
          queue.isActive
        ) {
          const chunk = queue.chunks.shift();
          if (chunk) {
            this.sessionUseCase.streamAudio(sessionId, chunk);
            processed++;
          }
        }
      } finally {
        queue.isProcessing = false;

        // Schedule next batch if needed
        if (queue.chunks.length > 0 && queue.isActive) {
          setImmediate(() => this.processQueue(sessionId));
        }
      }
    };

    // Run async without blocking caller
    processBatch().catch((err) => {
      this.logger.error("Error processing audio queue", { sessionId, err });
      queue.isProcessing = false;
    });
  }
}
