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
   * Enqueues a short interactive TEXT user turn that reliably triggers Nova
   * Sonic to generate a spoken response. MUST only be called after the
   * required audio turn for the same prompt has already been enqueued,
   * because Nova Sonic requires every prompt to contain at least one audio chunk.
   *
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
          interactive: true,   // bypasses VAD — guarantees Nova Sonic responds
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
   * Streams the pre-recorded LPCM greeting audio into the LIVE bidirectional
   * stream at real microphone cadence, then appends an interactive TEXT turn
   * ("Hello!") in the same prompt to guarantee Nova Sonic generates a response.
   *
   * ── Why both audio AND text ───────────────────────────────────────────────
   * Nova Sonic requires every prompt to contain at least one audio chunk —
   * a text-only prompt is rejected by the API.
   *
   * However, pre-recorded audio delivered over HTTP/2 does NOT reliably fire
   * Nova Sonic's VAD: speech tokens are counted (inputSpeechTokens > 0) but
   * outputSpeechTokens stays 0 and the stream ends with no audioOutput events.
   *
   * Sending BOTH satisfies both constraints:
   *   • Audio turn  → fulfils the "must include audio" API requirement.
   *   • TEXT turn (interactive:true) → bypasses VAD, guarantees a response.
   *
   * Event sequence:
   *   contentStart (AUDIO, interactive:true, role:USER)
   *   → audioInput × N  (100ms cadence)
   *   → contentEnd (AUDIO)
   *   → contentStart (TEXT, interactive:true, role:USER)   ← triggers response
   *   → textInput ("Hello!")
   *   → contentEnd (TEXT)
   *   → promptEnd
   *
   * MUST be awaited.  MUST be called after session.streamReady resolves.
   */
  async enqueueAudioGreeting(sessionId: string, audioData: Buffer): Promise<void> {
    const session = this.requireSession(sessionId);

    // ── Step 1: Audio turn (satisfies Nova Sonic "must include audio" constraint) ──
    const greetingContentId = randomUUID();

    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: greetingContentId,
          type: "AUDIO",
          interactive: true,
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

    // Stream at real mic cadence: 3200 bytes = 16000Hz × 2 bytes × 0.1s = 100ms
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

      await this.delay(CHUNK_DELAY_MS);
    }

    // Close audio content block
    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName: session.promptName,
          contentName: greetingContentId,
        },
      },
    });

    this.logger.debug("Greeting audio turn enqueued at mic cadence", {
      sessionId,
      audioBytes: audioData.byteLength,
      chunks: chunkCount,
      durationMs: Math.round((audioData.byteLength / 2 / 16_000) * 1000),
    });

    // ── Step 2: Interactive text turn (bypasses VAD, guarantees Nova Sonic responds) ──
    //
    // Pre-recorded audio over HTTP/2 fires inputSpeechTokens but NOT outputSpeechTokens.
    // An interactive TEXT turn processes immediately and always produces audioOutput.
    this.enqueueGreetingTrigger(sessionId);

    this.logger.debug("Greeting text trigger appended after audio turn", { sessionId });

    // ── Step 3: promptEnd — close the user turn ────────────────────────────────
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

            if (aborted || session.queue.length === 0 || !session.isActive) {
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