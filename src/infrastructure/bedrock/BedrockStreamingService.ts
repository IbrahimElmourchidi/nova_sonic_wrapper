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

  /**
   * Enqueues a non-interactive TEXT user turn that triggers Nova Sonic to
   * generate a spoken response on the subsequent promptEnd.
   * interactive:false bypasses VAD so the model waits for promptEnd rather
   * than using audio silence detection to decide the turn is over.
   *
   * MUST only be called after the required audio content for the same prompt
   * has already been enqueued (Nova Sonic requires at least one audio chunk).
   * Does NOT enqueue promptEnd — caller is responsible for that.
   */
  enqueueGreetingTrigger(sessionId: string, triggerText = "Hello!"): void {
    const session = this.requireSession(sessionId);
    const contentId = randomUUID();

    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: contentId,
          type: "TEXT",
          interactive: false,  // false = bypass VAD, respond on promptEnd
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
          content: triggerText,
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

    this.logger.debug("Greeting trigger text enqueued", { sessionId, triggerText });
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
   * ── FIX: Redesigned greeting strategy ─────────────────────────────────────
   *
   * PROBLEM:
   * Streaming 1.6s of pre-recorded greeting audio at real mic cadence did NOT
   * reliably trigger Nova Sonic's VAD. The model counted input speech tokens
   * but produced 0 output tokens — no audioOutput events, no response.
   *
   * FIX (v5): Send the ACTUAL greeting audio (from greeting.mp3 LPCM) in
   * small ~100ms chunks with interactive:true so VAD detects the speech.
   * Previous approaches using 100ms of silence all produced 0 output —
   * Nova Sonic needs real audio energy to trigger a response.
   *
   * The text trigger is kept as a non-interactive fallback hint.
   *
   * Event sequence:
   *   → contentStart (AUDIO, interactive:true, role:USER)
   *   → audioInput × N chunks (~3200 bytes each, real audio)
   *   → contentEnd (AUDIO)
   *   → contentStart (TEXT, interactive:false, role:USER)
   *   → textInput ("Hello! Please greet the user warmly.")
   *   → contentEnd (TEXT)
   *   → promptEnd
   *
   * MUST be awaited.  MUST be called after session.streamReady resolves.
   */
  async enqueueAudioGreeting(sessionId: string, audioData: Buffer): Promise<void> {
    const session = this.requireSession(sessionId);

    const CHUNK_SIZE = 3200; // 100ms at 16 kHz, 16-bit mono
    const greetingContentId = randomUUID();

    // ── Step 1: Stream actual greeting audio with VAD enabled ─────────────
    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: greetingContentId,
          type: "AUDIO",
          interactive: true,  // enable VAD — detects speech in the audio
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

    // Send audio in ~100ms chunks to mimic real-time streaming
    let chunkCount = 0;
    for (let offset = 0; offset < audioData.length; offset += CHUNK_SIZE) {
      const chunk = audioData.subarray(offset, offset + CHUNK_SIZE);
      this.enqueue(sessionId, {
        event: {
          audioInput: {
            promptName: session.promptName,
            contentName: greetingContentId,
            content: chunk.toString("base64"),
          },
        },
      });
      chunkCount++;
    }

    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName: session.promptName,
          contentName: greetingContentId,
        },
      },
    });

    this.logger.debug("Greeting audio enqueued", {
      sessionId,
      totalBytes: audioData.length,
      chunks: chunkCount,
    });

    // ── Step 2: Non-interactive TEXT trigger ───────────────────────────────
    this.enqueueGreetingTrigger(sessionId, "Hello! Please greet the user warmly.");

    this.logger.debug("Greeting text trigger appended after audio", { sessionId });

    // ── Step 3: promptEnd — close the user turn ───────────────────────────
    this.enqueue(sessionId, {
      event: { promptEnd: { promptName: session.promptName } },
    });

    this.logger.debug("promptEnd enqueued — greeting turn complete", { sessionId });
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

  /**
   * ── FIX: Reduced log noise for audioInput events ──────────────────────────
   *
   * Previously, every single audioInput chunk (~100ms of mic audio) produced
   * a DEBUG log line. With 10 chunks/second, this created ~80 lines per 8s
   * of speech — drowning out meaningful events.
   *
   * Now: audioInput events are sampled (logged every 50th chunk) with a
   * running count. All other event types are still logged individually.
   */
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
            // Loop until we have an item or must close.
            // A stale queueSignal can wake us with an empty queue;
            // we simply go back to waiting instead of returning done.
            while (true) {
              if (aborted || !session.isActive || !this.sessions.has(sessionId)) {
                this.logger.debug("[DEBUG] Iterable returning done:true", {
                  sessionId,
                  reason: aborted ? "aborted" : !session.isActive ? "session_inactive" : "session_not_found",
                });
                return { value: undefined as any, done: true };
              }

              if (session.queue.length > 0) break; // item available
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
                  this.logger.debug("[DEBUG] Iterable returning done:true", {
                    sessionId,
                    reason: isClose ? "closeSignal" : aborted ? "aborted" : "session_inactive",
                  });
                  return { value: undefined as any, done: true };
                }
                this.logger.error("Unexpected race error", { sessionId, err });
              }
              // Loop back — re-check queue.length and termination conditions
            }

            const nextEvent = session.queue.shift();
            const evKey =
              Object.keys((nextEvent as any)?.event ?? {})[0] ?? "unknown";

            // ── FIX: Sampled logging for audioInput ─────────────────────────
            if (evKey === "audioInput") {
              session.audioChunksSent = (session.audioChunksSent ?? 0) + 1;
              // Log every 50th chunk, plus the 1st one
              if (session.audioChunksSent === 1 || session.audioChunksSent % 50 === 0) {
                this.logger.debug("[DEBUG] Sending audioInput to Bedrock", {
                  sessionId,
                  totalChunksSent: session.audioChunksSent,
                  remainingQueue: session.queue.length,
                });
              }
            } else {
              this.logger.debug("[DEBUG] Sending event to Bedrock", {
                sessionId,
                eventType: evKey,
                remainingQueue: session.queue.length,
              });
            }

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

  /**
   * ── FIX: Enhanced response tracking ───────────────────────────────────────
   *
   * Now tracks whether audioOutput / textOutput events were received during
   * the response stream. This makes it immediately obvious in logs when
   * Nova Sonic produces 0 output tokens (the greeting bug).
   */
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
    let audioOutputCount = 0;
    let textOutputCount = 0;

    try {
      for await (const event of (response as any).body) {
        if (!session.isActive) {
          this.logger.debug("Session no longer active, stopping stream", { sessionId });
          break;
        }

        if (event.chunk?.bytes) {
          this.sessions.updateActivity(sessionId);
          const text = decoder.decode(event.chunk.bytes);
          chunkCount++;

          try {
            const parsed = JSON.parse(text);
            const evKey = parsed?.event ? Object.keys(parsed.event)[0] : "non-event";

            // Track output types for diagnostics
            if (evKey === "audioOutput") audioOutputCount++;
            if (evKey === "textOutput") textOutputCount++;

            this.logger.info("[DEBUG] Bedrock → response event", {
              sessionId,
              chunkIndex: chunkCount,
              eventType: evKey,
              event:
                evKey === "audioOutput"
                  ? {
                      audioOutput: {
                        contentName: parsed.event.audioOutput?.contentName,
                        bytes: `<${(parsed.event.audioOutput?.content ?? "").length} base64-chars>`,
                      },
                    }
                  : parsed?.event,
            });
          } catch {
            this.logger.info("[DEBUG] Non-JSON chunk from Bedrock", { sessionId, text });
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

      // ── FIX: Enhanced end-of-stream diagnostics ───────────────────────────
      this.logger.info("[DEBUG] for-await loop exited", {
        sessionId,
        reason: session.isActive ? "bedrock_closed_stream" : "session_inactive",
        totalChunks: chunkCount,
      });

      session.receivedAudioOutput = audioOutputCount > 0;
      session.receivedTextOutput = textOutputCount > 0;

      this.logger.info("[DEBUG] Response loop ended", {
        sessionId,
        totalChunks: chunkCount,
        audioOutputChunks: audioOutputCount,
        textOutputChunks: textOutputCount,
        hadAudioOutput: audioOutputCount > 0,
        hadTextOutput: textOutputCount > 0,
        isActive: session.isActive,
      });

      if (audioOutputCount === 0 && textOutputCount === 0 && chunkCount > 0) {
        this.logger.warn(
          "⚠ Nova Sonic produced ZERO output (no audioOutput, no textOutput). " +
            "The model processed input tokens but did not generate a response. " +
            "This typically means the greeting/VAD strategy failed.",
          { sessionId, totalInputChunks: chunkCount }
        );
      }

      if (session.isActive) {
        this.logger.info("Response stream complete", { sessionId });
        this.dispatchEvent(sessionId, "streamComplete", {
          timestamp: new Date().toISOString(),
        });
      } else {
        this.logger.info("Response stream ended after session force-closed", { sessionId });
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
      this.logger.debug("Failed to parse response chunk", { sessionId, rawText });
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

  private dispatchEvent(sessionId: string, eventType: string, data: unknown): void {
    const session = this.sessions.findById(sessionId);
    if (!session) return;

    const handler = session.responseHandlers.get(eventType);
    if (handler) {
      try {
        handler(data);
      } catch (err) {
        this.logger.error(`Handler error for event '${eventType}'`, { sessionId, err });
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