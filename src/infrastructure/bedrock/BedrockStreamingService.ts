// src/infrastructure/bedrock/BedrockStreamingService.ts
import "reflect-metadata";
import { inject, injectable } from "tsyringe";
import {
  BedrockRuntimeClient,
  InvokeModelWithBidirectionalStreamCommand,
  type InvokeModelWithBidirectionalStreamInput,
} from "@aws-sdk/client-bedrock-runtime";
import { NodeHttp2Handler } from "@smithy/node-http-handler";
import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { firstValueFrom } from "rxjs";
import { take } from "rxjs/operators";

import { TOKENS } from "../config/tokens";
import type { IStreamingService } from "../../domain/services/IStreamingService";
import type { IToolService } from "../../domain/services/IToolService";
import type { ISessionRepository } from "../../domain/repositories/ISessionRepository";
import type { ILogger } from "../logging/ILogger";
import type { AppConfig } from "../config/AppConfig";
import type { AudioConfiguration, TextConfiguration } from "../../domain/types";
import type { SessionData } from "../../domain/entities/Session";
import { StreamingError, SessionNotFoundError } from "../../domain/errors";
import {
  DefaultAudioOutputConfiguration,
  DefaultToolSchema,
  WeatherToolSchema,
  DefaultAudioInputConfiguration,
} from "../config/defaults";

@injectable()
export class BedrockStreamingService implements IStreamingService {
  private readonly bedrockClient: BedrockRuntimeClient;
  // Tracks raw events for session-level debug logging
  private readonly sessionEventLog = new Map<string, unknown[]>();

  constructor(
    @inject(TOKENS.SessionRepository)
    private readonly sessions: ISessionRepository,

    @inject(TOKENS.ToolService)
    private readonly toolService: IToolService,

    @inject(TOKENS.AppConfig)
    private readonly config: AppConfig,

    @inject(TOKENS.Logger)
    private readonly logger: ILogger
  ) {
    const handler = new NodeHttp2Handler({
      requestTimeout: config.bedrock.requestTimeoutMs,
      sessionTimeout: config.bedrock.sessionTimeoutMs,
      disableConcurrentStreams: false,
      maxConcurrentStreams: config.bedrock.maxConcurrentStreams,
    });

    this.bedrockClient = new BedrockRuntimeClient({
      region: config.aws.region,
      requestHandler: handler,
    });
  }

  enqueueGreetingSilence(sessionId: string): void {
    const session = this.requireSession(sessionId);

    // Use a dedicated content ID so it never collides with the live mic block.
    // randomUUID is already imported at the top of BedrockStreamingService
    const greetingContentId = randomUUID();

    const SILENCE_MS = 300;
    const SAMPLE_RATE = 16_000;   // Hz  — must match DefaultAudioInputConfiguration
    const BYTES_PER_SAMPLE = 2;        // 16-bit
    const CHANNELS = 1;        // mono
    const silenceBytes = Math.ceil(SILENCE_MS / 1000 * SAMPLE_RATE * BYTES_PER_SAMPLE * CHANNELS);
    const silenceBase64 = Buffer.alloc(silenceBytes, 0).toString("base64");

    // 1. Open the audio content block (non-interactive — one-shot input)
    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: greetingContentId,
          type: "AUDIO",
          interactive: false,
          role: "USER",
          audioInputConfiguration: DefaultAudioInputConfiguration,
        },
      },
    });

    // 2. Send the silence payload
    this.enqueue(sessionId, {
      event: {
        audioInput: {
          promptName: session.promptName,
          contentName: greetingContentId,
          content: silenceBase64,
        },
      },
    });

    // 3. Close the audio content block
    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName: session.promptName,
          contentName: greetingContentId,
        },
      },
    });

    this.logger.debug("Greeting silence enqueued", {
      sessionId,
      silenceMs: SILENCE_MS,
      silenceBytes,
    });
  }

  // ── IStreamingService implementation ────────────────────────────────────

  async initiateStream(sessionId: string): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);

    try {
      this.logger.info("Initiating bidirectional stream", { sessionId });

      const asyncIterable = this.buildAsyncIterable(sessionId);

      const response: any = await this.bedrockClient.send(
        new InvokeModelWithBidirectionalStreamCommand({
          modelId: this.config.bedrock.modelId,
          body: asyncIterable,
        })
      );

      this.logger.info("Stream established, processing responses", {
        sessionId,
      });

      // Unblock any code awaiting session.streamReady (e.g. auto-greeting).
      session.resolveStreamReady();

      await this.processResponseStream(sessionId, response);
    } catch (err) {
      this.logger.error("Stream error", { sessionId, err });

      // Unblock streamReady waiters so they don't hang indefinitely.
      // rejectStreamReady is a no-op if the promise was already resolved.
      session.rejectStreamReady(err);

      this.dispatchEvent(sessionId, "error", {
        source: "bidirectionalStream",
        error: err,
      });

      const s = this.sessions.findById(sessionId);
      if (s?.isActive) {
        this.sessions.delete(sessionId);
      }

      throw new StreamingError(sessionId, err);
    }
  }

  enqueueSessionStart(sessionId: string): void {
    const session = this.requireSession(sessionId);

    // Guard: Nova Sonic rejects duplicate sessionStart events on the same stream.
    if (session.isSessionStartSent) {
      this.logger.debug("enqueueSessionStart skipped — already sent", { sessionId });
      return;
    }

    this.enqueue(sessionId, {
      event: {
        sessionStart: { inferenceConfiguration: session.inferenceConfig },
      },
    });
    session.isSessionStartSent = true;
  }

  enqueuePromptStart(sessionId: string): void {
    const session = this.requireSession(sessionId);

    this.enqueue(sessionId, {
      event: {
        promptStart: {
          promptName: session.promptName,
          textOutputConfiguration: { mediaType: "text/plain" },
          audioOutputConfiguration: DefaultAudioOutputConfiguration,
          toolUseOutputConfiguration: { mediaType: "application/json" },
          toolConfiguration: {
            tools: [
              {
                toolSpec: {
                  name: "getDateAndTimeTool",
                  description: "Get information about the current date and time.",
                  inputSchema: { json: DefaultToolSchema },
                },
              },
              {
                toolSpec: {
                  name: "getWeatherTool",
                  description:
                    "Get the current weather for a given location, based on its WGS84 coordinates.",
                  inputSchema: { json: WeatherToolSchema },
                },
              },
            ],
          },
        },
      },
    });

    session.isPromptStartSent = true;
    this.logger.debug("Prompt start enqueued", { sessionId });
  }

  enqueueSystemPrompt(
    sessionId: string,
    content: string,
    textConfig: TextConfiguration
  ): void {
    const session = this.requireSession(sessionId);
    const contentId = randomUUID();

    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: contentId,
          type: "TEXT",
          interactive: false,
          role: "SYSTEM",
          textInputConfiguration: textConfig,
        },
      },
    });

    this.enqueue(sessionId, {
      event: {
        textInput: {
          promptName: session.promptName,
          contentName: contentId,
          content,
        },
      },
    });

    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName: session.promptName,
          contentName: contentId,
        },
      },
    });
  }

  enqueueUserText(sessionId: string, content: string): void {
    const session = this.requireSession(sessionId);

    // Nova Sonic requires audio content before text in the same prompt.
    // If an audio block is open, close it first so the events are non-interleaved:
    //   contentEnd(AUDIO) → contentStart(TEXT) → textInput → contentEnd(TEXT)
    // Then endAudioContent in gracefulClose will skip (isAudioContentStartSent=false).
    if (session.isAudioContentStartSent) {
      this.enqueue(sessionId, {
        event: {
          contentEnd: {
            promptName: session.promptName,
            contentName: session.audioContentId,
          },
        },
      });
      session.isAudioContentStartSent = false;
    }

    const contentId = randomUUID();

    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: contentId,
          type: "TEXT",
          interactive: false,
          role: "USER",
          textInputConfiguration: { mediaType: "text/plain" },
        },
      },
    });

    this.enqueue(sessionId, {
      event: {
        textInput: {
          promptName: session.promptName,
          contentName: contentId,
          content,
        },
      },
    });

    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName: session.promptName,
          contentName: contentId,
        },
      },
    });
  }

  enqueueAudioContentStart(
    sessionId: string,
    audioConfig: AudioConfiguration
  ): void {
    const session = this.requireSession(sessionId);

    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: session.audioContentId,
          type: "AUDIO",
          interactive: true,
          role: "USER",
          audioInputConfiguration: audioConfig,
        },
      },
    });

    session.isAudioContentStartSent = true;
    this.logger.debug("Audio content start enqueued", { sessionId });
  }

  enqueueAudioChunk(sessionId: string, audioData: Buffer): void {
    const session = this.requireSession(sessionId);
    const base64 = audioData.toString("base64");

    this.enqueue(sessionId, {
      event: {
        audioInput: {
          promptName: session.promptName,
          contentName: session.audioContentId,
          content: base64,
        },
      },
    });
  }

  async enqueueContentEnd(sessionId: string): Promise<void> {
    const session = this.requireSession(sessionId);
    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName: session.promptName,
          contentName: session.audioContentId,
        },
      },
    });
    await this.delay(500);
  }

  async enqueuePromptEnd(sessionId: string): Promise<void> {
    const session = this.requireSession(sessionId);
    this.enqueue(sessionId, {
      event: { promptEnd: { promptName: session.promptName } },
    });
    await this.delay(300);
  }

  async enqueueSessionEnd(sessionId: string): Promise<void> {
    const session = this.requireSession(sessionId);

    this.enqueue(sessionId, { event: { sessionEnd: {} } });
    await this.delay(300);

    // Flush debug log if enabled
    if (this.config.logging.logSessionEvents) {
      const events = this.sessionEventLog.get(sessionId) ?? [];
      const outputPath = join(process.cwd(), "lastSession.json");
      writeFileSync(outputPath, JSON.stringify(events, null, 2), "utf-8");
      this.logger.info("Session events written", {
        sessionId,
        path: outputPath,
        eventCount: events.length,
      });
      this.sessionEventLog.delete(sessionId);
    }

    // Signal closure
    session.isActive = false;
    session.closeSignal.next();
    session.closeSignal.complete();
    this.sessions.delete(sessionId);

    this.logger.info("Session ended", { sessionId });
  }

  // ── Private helpers ──────────────────────────────────────────────────────

  private requireSession(sessionId: string): SessionData {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);
    return session;
  }

  private enqueue(sessionId: string, event: unknown): void {
    const session = this.sessions.findById(sessionId);
    if (!session || !session.isActive) return;

    this.sessions.updateActivity(sessionId);
    session.queue.push(event);
    session.queueSignal.next();

    // Record for debug log (strip audio payload)
    if (this.config.logging.logSessionEvents) {
      if (!this.sessionEventLog.has(sessionId)) {
        this.sessionEventLog.set(sessionId, []);
      }
      const logEntry = this.sanitizeForLog(event);
      this.sessionEventLog.get(sessionId)!.push(logEntry);
    }
  }

  private sanitizeForLog(event: unknown): unknown {
    const e = event as Record<string, unknown>;
    const audioInput = (e?.event as Record<string, unknown>)?.audioInput as
      | Record<string, unknown>
      | undefined;
    if (audioInput) {
      return {
        event: {
          audioInput: {
            ...audioInput,
            content: `<audio: ${Buffer.byteLength(audioInput.content as string, "base64")} bytes>`,
          },
        },
      };
    }
    return event;
  }

  private buildAsyncIterable(
    sessionId: string
  ): AsyncIterable<InvokeModelWithBidirectionalStreamInput> {
    const session = this.sessions.findById(sessionId);
    if (!session?.isActive) {
      return {
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ value: undefined as any, done: true as const }),
        }),
      };
    }

    // Per-iterator flag so that return()/throw() only stop THIS iterator,
    // not the entire session. Without this, the SDK calling return() on a
    // finished stream would set session.isActive = false and kill the next
    // stream's iterator too.
    let aborted = false;

    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<IteratorResult<InvokeModelWithBidirectionalStreamInput>> => {
          try {
            if (aborted || !session.isActive || !this.sessions.has(sessionId)) {
              return { value: undefined as any, done: true };
            }

            // Wait for queue items or close signal
            if (session.queue.length === 0) {
              try {
                await Promise.race([
                  firstValueFrom(session.queueSignal.pipe(take(1))),
                  firstValueFrom(session.closeSignal.pipe(take(1))).then(() => {
                    throw new Error("Stream closed");
                  }),
                ]);
              } catch (err) {
                const isClose =
                  err instanceof Error && err.message === "Stream closed";
                if (isClose || aborted || !session.isActive) {
                  return { value: undefined as any, done: true };
                }
                this.logger.error("Unexpected race error", { sessionId, err });
              }
            }

            if (aborted || session.queue.length === 0 || !session.isActive) {
              return { value: undefined as any, done: true };
            }

            const nextEvent = session.queue.shift();
            return {
              value: {
                chunk: {
                  bytes: new TextEncoder().encode(JSON.stringify(nextEvent)),
                },
              },
              done: false,
            };
          } catch (err) {
            this.logger.error("Iterator error", { sessionId, err });
            aborted = true;
            return { value: undefined as any, done: true };
          }
        },

        return: async () => {
          aborted = true;
          return { value: undefined as any, done: true };
        },

        throw: async (err: unknown) => {
          aborted = true;
          throw err;
        },
      }),
    };
  }

  private async processResponseStream(
    sessionId: string,
    response: Awaited<
      ReturnType<BedrockRuntimeClient["send"]>
    > & { body: AsyncIterable<any>; }
  ): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session) return;

    const decoder = new TextDecoder();

    try {
      for await (const event of (response as any).body) {
        if (!session.isActive) {
          this.logger.debug("Session no longer active, stopping stream", {
            sessionId,
          });
          break;
        }

        if (event.chunk?.bytes) {
          this.sessions.updateActivity(sessionId);
          const text = decoder.decode(event.chunk.bytes);
          this.handleResponseEvent(sessionId, session, text);
        } else if (event.modelStreamErrorException) {
          this.logger.error("Model stream error", {
            sessionId,
            details: event.modelStreamErrorException,
          });
          this.dispatchEvent(sessionId, "error", {
            type: "modelStreamErrorException",
            details: event.modelStreamErrorException,
          });
        } else if (event.internalServerException) {
          this.logger.error("Internal server error from Bedrock", {
            sessionId,
            details: event.internalServerException,
          });
          this.dispatchEvent(sessionId, "error", {
            type: "internalServerException",
            details: event.internalServerException,
          });
        }
      }

      // Only dispatch streamComplete if the session is still active (natural end).
      // If session.isActive is false, the session was force-closed by gracefulClose.
      // In that case a new session may already exist under the same socketId in the
      // repository — dispatching here would wrongly trigger the NEW session's handler.
      if (session.isActive) {
        this.logger.info("Response stream complete", { sessionId });
        this.dispatchEvent(sessionId, "streamComplete", {
          timestamp: new Date().toISOString(),
        });
      } else {
        this.logger.info("Response stream ended after session force-closed", {
          sessionId,
        });
      }
    } catch (err) {
      this.logger.error("Error processing response stream", { sessionId, err });
      this.dispatchEvent(sessionId, "error", {
        source: "responseStream",
        message: "Error processing response stream",
        details: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private handleResponseEvent(
    sessionId: string,
    session: SessionData,
    rawText: string
  ): void {
    let json: Record<string, unknown>;
    try {
      json = JSON.parse(rawText);
    } catch {
      this.logger.debug("Failed to parse response chunk", {
        sessionId,
        rawText,
      });
      return;
    }

    const ev = json.event as Record<string, unknown> | undefined;
    if (!ev) {
      this.dispatchEvent(sessionId, "unknown", json);
      return;
    }

    if (ev.contentStart) {
      this.dispatchEvent(sessionId, "contentStart", ev.contentStart);
    } else if (ev.textOutput) {
      this.dispatchEvent(sessionId, "textOutput", ev.textOutput);
    } else if (ev.audioOutput) {
      this.dispatchEvent(sessionId, "audioOutput", ev.audioOutput);
    } else if (ev.toolUse) {
      this.dispatchEvent(sessionId, "toolUse", ev.toolUse);
      const toolUse = ev.toolUse as Record<string, unknown>;
      session.toolUseContent = toolUse;
      session.toolUseId = toolUse.toolUseId as string;
      session.toolName = toolUse.toolName as string;
    } else if (ev.contentEnd && (ev.contentEnd as Record<string, unknown>).type === "TOOL") {
      this.dispatchEvent(sessionId, "toolEnd", {
        toolUseContent: session.toolUseContent,
        toolUseId: session.toolUseId,
        toolName: session.toolName,
      });

      // Execute tool asynchronously
      this.toolService
        .execute(session.toolName, session.toolUseContent)
        .then((result) => {
          this.sendToolResult(sessionId, session.toolUseId, result);
          this.dispatchEvent(sessionId, "toolResult", {
            toolUseId: session.toolUseId,
            result,
          });
        })
        .catch((err) => {
          this.logger.error("Tool execution failed", {
            sessionId,
            toolName: session.toolName,
            err,
          });
          this.dispatchEvent(sessionId, "error", {
            source: "toolExecution",
            toolName: session.toolName,
            details: err instanceof Error ? err.message : String(err),
          });
        });
    } else if (ev.contentEnd) {
      this.dispatchEvent(sessionId, "contentEnd", ev.contentEnd);
    } else {
      const keys = Object.keys(ev);
      if (keys.length > 0) {
        this.dispatchEvent(sessionId, keys[0] as any, ev);
      } else {
        this.dispatchEvent(sessionId, "unknown", json);
      }
    }
  }

  private sendToolResult(
    sessionId: string,
    toolUseId: string,
    result: unknown
  ): void {
    const session = this.sessions.findById(sessionId);
    if (!session?.isActive) return;

    const contentId = randomUUID();
    const resultContent =
      typeof result === "string" ? result : JSON.stringify(result);

    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: contentId,
          interactive: false,
          type: "TOOL",
          role: "TOOL",
          toolResultInputConfiguration: {
            toolUseId,
            type: "TEXT",
            textInputConfiguration: { mediaType: "text/plain" },
          },
        },
      },
    });

    this.enqueue(sessionId, {
      event: {
        toolResult: {
          promptName: session.promptName,
          contentName: contentId,
          content: resultContent,
        },
      },
    });

    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName: session.promptName,
          contentName: contentId,
        },
      },
    });

    this.logger.debug("Tool result enqueued", { sessionId, toolUseId });
  }

  private dispatchEvent(
    sessionId: string,
    eventType: string,
    data: unknown
  ): void {
    const session = this.sessions.findById(sessionId);
    if (!session) return;

    const handler = session.responseHandlers.get(eventType);
    if (handler) {
      try {
        handler(data);
      } catch (err) {
        this.logger.error(`Handler error for event '${eventType}'`, {
          sessionId,
          err,
        });
      }
    }

    // Wildcard handler
    const anyHandler = session.responseHandlers.get("any");
    if (anyHandler) {
      try {
        anyHandler({ type: eventType, data });
      } catch (err) {
        this.logger.error("Wildcard handler error", { sessionId, err });
      }
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}