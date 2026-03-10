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
} from "../config/defaults";

@injectable()
export class BedrockStreamingService implements IStreamingService {
  private readonly bedrockClient: BedrockRuntimeClient;
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

  // ── IStreamingService ─────────────────────────────────────────────────────

  async initiateStream(sessionId: string): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);

    try {
      this.logger.info("Initiating bidirectional stream", { sessionId });

      this.logger.info("[DEBUG] Queue snapshot before send()", {
        sessionId,
        queueLength: session.queue.length,
        events: (session.queue as Array<unknown>).map((e: any) => {
          const ev = e?.event ?? {};
          const key = Object.keys(ev)[0] ?? "unknown";
          const val = (ev[key] as Record<string, unknown>) ?? {};
          if (key === "audioInput") {
            return {
              [key]: {
                ...val,
                content: `<${Buffer.byteLength(val.content as string, "base64")} bytes>`,
              },
            };
          }
          return { [key]: val };
        }),
      });

      const asyncIterable = this.buildAsyncIterable(sessionId);

      const response: any = await this.bedrockClient.send(
        new InvokeModelWithBidirectionalStreamCommand({
          modelId: this.config.bedrock.modelId,
          body: asyncIterable,
        })
      );

      this.logger.info("Stream established, processing responses", { sessionId });
      this.logger.info("[DEBUG] Queue state after send() resolved", {
        sessionId,
        remainingQueueLength: session.queue.length,
      });

      session.resolveStreamReady();
      await this.processResponseStream(sessionId, response);
    } catch (err) {
      this.logger.error("Stream error", { sessionId, err });
      session.rejectStreamReady(err);
      this.dispatchEvent(sessionId, "error", { source: "bidirectionalStream", error: err });

      const s = this.sessions.findById(sessionId);
      if (s?.isActive) this.sessions.delete(sessionId);

      throw new StreamingError(sessionId, err);
    }
  }

  enqueueSessionStart(sessionId: string): void {
    const session = this.requireSession(sessionId);
    if (session.isSessionStartSent) {
      this.logger.debug("enqueueSessionStart skipped — already sent", { sessionId });
      return;
    }
    this.enqueue(sessionId, {
      event: { sessionStart: { inferenceConfiguration: session.inferenceConfig } },
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

    this.logger.debug("System prompt enqueued", { sessionId });
  }

  enqueueUserText(sessionId: string, content: string): void {
    const session = this.requireSession(sessionId);

    // Close any open audio content block before sending text
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

  enqueueAudioContentStart(sessionId: string, audioConfig: AudioConfiguration): void {
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
    this.enqueue(sessionId, {
      event: {
        audioInput: {
          promptName: session.promptName,
          contentName: session.audioContentId,
          content: audioData.toString("base64"),
        },
      },
    });
  }

  /**
   * Enqueues a complete pre-recorded audio greeting as a USER turn.
   *
   * Uses a FRESH contentId (not session.audioContentId) so it is fully
   * self-contained and does not interfere with the live-mic audio block that
   * the client opens later in the same session.
   *
   * WHY interactive: true
   *   interactive: false = Nova Sonic treats the content as background context
   *                        and produces ZERO output (outputTokens: 0).
   *   interactive: true  = Nova Sonic treats it as a real user turn. The
   *                        contentEnd after the final chunk acts as an explicit
   *                        end-of-speech signal so the model does not wait for
   *                        VAD silence detection.
   *
   * WHY chunked (3 200 bytes = 100 ms per chunk)
   *   Sending a single large blob delivers all audio data instantaneously.
   *   Nova Sonic's VAD/response engine expects audio to arrive over time, as a
   *   real microphone streams it. Small chunks matching the mic cadence give
   *   the model time to buffer and process incrementally — the same pattern
   *   that works for every Turn 2+ live-mic turn.
   *
   * IMPORTANT: call this ONLY after session.streamReady has resolved.
   *   Pre-queuing this audio before the HTTP/2 stream is live causes
   *   speechTokens to be counted but outputTokens to be 0 (stream hangs).
   */
  /**
   * Streams the pre-recorded LPCM greeting into the LIVE bidirectional stream,
   * delivering one 100ms chunk every 100ms so Nova Sonic's VAD/response engine
   * receives audio at real microphone cadence and generates a spoken response.
   *
   * WHY async with real delays (not just chunked queue items):
   *   The async iterable drains the queue as fast as next() is called.
   *   Even with 16 separately-enqueued chunks, all 16 items drain in ~25ms
   *   because the SDK calls next() with no back-pressure.  Nova Sonic's VAD
   *   receives the full 1.6s as an instant burst — its response engine never
   *   fires — and outputTokens stays 0 while the stream hangs indefinitely.
   *
   *   Awaiting 100ms between each enqueue() call forces the queue to empty
   *   before the next chunk arrives.  The async generator blocks on its
   *   internal "queue not empty" promise until the next chunk is ready.
   *   From Nova Sonic's perspective audio trickles in at ~100ms intervals —
   *   identical to a live microphone — and VAD fires normally.
   *
   * MUST be awaited.  MUST be called after session.streamReady resolves.
   */
  async enqueueAudioGreeting(sessionId: string, audioData: Buffer): Promise<void> {
    const session = this.requireSession(sessionId);

    // Dedicated content ID — separate from session.audioContentId so it does
    // not interfere with the live-mic content block opened later.
    const greetingContentId = randomUUID();

    // contentStart
    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: greetingContentId,
          type: "AUDIO",
          interactive: true,   // MUST be true — false silences Nova Sonic
          role: "USER",
          audioInputConfiguration: {
            audioType: "SPEECH",
            encoding: "base64",
            mediaType: "audio/lpcm",
            sampleRateHertz: 16000,
            sampleSizeBits: 16,
            channelCount: 1,
          },
        },
      },
    });

    // audioInput — one chunk every 100ms to simulate live mic cadence.
    // 3200 bytes = 16000 samples/s × 2 bytes/sample × 0.1s = exactly 100ms.
    // The await forces the queue to drain between chunks so the iterable
    // delivers audio at the same rate a microphone would.
    const CHUNK_BYTES = 3200;
    const CHUNK_DELAY_MS = 100;
    let offset = 0;
    let chunkCount = 0;

    while (offset < audioData.byteLength) {
      const end = Math.min(offset + CHUNK_BYTES, audioData.byteLength);
      const chunk = audioData.slice(offset, end);

      this.enqueue(sessionId, {
        event: {
          audioInput: {
            promptName: session.promptName,
            contentName: greetingContentId,
            content: chunk.toString("base64"),
          },
        },
      });

      offset += CHUNK_BYTES;
      chunkCount++;

      // Critical: wait for this chunk to be consumed before queuing the next.
      await this.delay(CHUNK_DELAY_MS);
    }

    // contentEnd — explicit end-of-speech signal; Nova Sonic responds immediately
    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName: session.promptName,
          contentName: greetingContentId,
        },
      },
    });

    this.logger.debug("Audio greeting streamed at mic cadence", {
      sessionId,
      audioBytes: audioData.byteLength,
      chunks: chunkCount,
      durationMs: Math.round((audioData.byteLength / 2 / 16_000) * 1000),
    });

    // ── promptEnd — signals end of user turn to Nova Sonic ───────────────────
    //
    // Without promptEnd, Nova Sonic sits waiting for more events indefinitely
    // and times out with "Timed out waiting for input events" (~60 s later).
    //
    // WHY NOT sessionEnd:
    //   enqueueSessionEnd() deletes the session from the repository and fires
    //   closeSignal, which would make the streamComplete handler crash when it
    //   calls prepareNextStream() (session not found).  We intentionally keep
    //   the session alive — the async iterable just idles (empty queue) while
    //   Nova Sonic generates the greeting response, and Turn 2's sessionStart
    //   (enqueued by prepareNextStream) wakes the iterable back up.
    this.enqueue(sessionId, {
      event: { promptEnd: { promptName: session.promptName } },
    });

    this.logger.debug("promptEnd enqueued after greeting audio", { sessionId });
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

    session.isActive = false;
    session.closeSignal.next();
    session.closeSignal.complete();
    this.sessions.delete(sessionId);
    this.logger.info("Session ended", { sessionId });
  }

  // ── Private helpers ───────────────────────────────────────────────────────

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

    if (this.config.logging.logSessionEvents) {
      if (!this.sessionEventLog.has(sessionId)) {
        this.sessionEventLog.set(sessionId, []);
      }
      this.sessionEventLog.get(sessionId)!.push(this.sanitizeForLog(event));
    }
  }

  private sanitizeForLog(event: unknown): unknown {
    const audioInput = (
      (event as any)?.event as Record<string, unknown>
    )?.audioInput as Record<string, unknown> | undefined;

    if (audioInput) {
      return {
        event: {
          audioInput: {
            ...audioInput,
            content: `<audio: ${Buffer.byteLength(
              audioInput.content as string,
              "base64"
            )} bytes>`,
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

    let aborted = false;

    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<
          IteratorResult<InvokeModelWithBidirectionalStreamInput>
        > => {
          try {
            if (aborted || !session.isActive || !this.sessions.has(sessionId)) {
              return { value: undefined as any, done: true };
            }

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

            if (
              aborted ||
              session.queue.length === 0 ||
              !session.isActive
            ) {
              return { value: undefined as any, done: true };
            }

            const nextEvent = session.queue.shift();
            const evKey =
              Object.keys((nextEvent as any)?.event ?? {})[0] ?? "unknown";

            this.logger.debug("[DEBUG] Sending event to Bedrock", {
              sessionId,
              eventType: evKey,
              remainingQueue: session.queue.length,
            });

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
    response: Awaited<ReturnType<BedrockRuntimeClient["send"]>> & {
      body: AsyncIterable<any>;
    }
  ): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session) return;

    const decoder = new TextDecoder();
    let chunkCount = 0;

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
          chunkCount++;

          try {
            const parsed = JSON.parse(text);
            const evKey = parsed?.event
              ? Object.keys(parsed.event)[0]
              : "non-event";
            this.logger.info("[DEBUG] Bedrock → response event", {
              sessionId,
              chunkIndex: chunkCount,
              eventType: evKey,
              event:
                evKey === "audioOutput"
                  ? {
                      audioOutput: {
                        contentName:
                          parsed.event.audioOutput?.contentName,
                        bytes: `<${
                          (parsed.event.audioOutput?.content ?? "").length
                        } base64-chars>`,
                      },
                    }
                  : parsed?.event,
            });
          } catch {
            this.logger.info("[DEBUG] Non-JSON chunk from Bedrock", {
              sessionId,
              text,
            });
          }

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

      this.logger.info("[DEBUG] Response loop ended", {
        sessionId,
        totalChunks: chunkCount,
        isActive: session.isActive,
      });

      if (session.isActive) {
        this.logger.info("Response stream complete", { sessionId });
        this.dispatchEvent(sessionId, "streamComplete", {
          timestamp: new Date().toISOString(),
        });
      } else {
        this.logger.info(
          "Response stream ended after session force-closed",
          { sessionId }
        );
      }
    } catch (err) {
      this.logger.error("Error processing response stream", {
        sessionId,
        err,
      });
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
    } else if (
      ev.contentEnd &&
      (ev.contentEnd as Record<string, unknown>).type === "TOOL"
    ) {
      this.dispatchEvent(sessionId, "toolEnd", {
        toolUseContent: session.toolUseContent,
        toolUseId: session.toolUseId,
        toolName: session.toolName,
      });
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