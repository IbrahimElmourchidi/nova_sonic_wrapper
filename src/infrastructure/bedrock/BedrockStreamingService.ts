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
import { execSync } from "node:child_process";
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

// ── Clock skew constants ───────────────────────────────────────────────────────
//
// AWS SigV4 rejects signatures when the server clock is more than 5 minutes
// (300 000 ms) ahead or behind AWS's time servers.  The AWS SDK has built-in
// clock skew correction, but it only applies to smaller skews: once the
// difference hits the hard limit the request is rejected even after the SDK's
// correction pass (as evidenced by clockSkewCorrected:true in the error
// metadata, alongside the 403 InvalidSignatureException).
//
// Strategy
// ────────
// 1. On the first InvalidSignatureException we attempt an in-process clock
//    sync via `chronyc makestep` (recommended on AL2/AL2023/Ubuntu EC2) or
//    `ntpdate` as a fallback.  This fixes the OS clock and is the permanent
//    cure.
//
// 2. After syncing, we re-create the BedrockRuntimeClient from scratch.  The
//    existing client instance caches a stale clock offset internally; a new
//    instance re-measures the skew against the current (now-correct) OS time.
//
// 3. We retry the stream up to MAX_CLOCK_RETRIES times with an exponential
//    back-off starting at CLOCK_RETRY_BASE_MS.  On a healthy server after an
//    NTP sync this succeeds on the first retry.
//
// 4. We log a prominent warning on every clock-skew detection so the operator
//    knows to investigate the root cause (EC2 clock drift, missing NTP daemon,
//    hibernation/resume, etc.).

const MAX_CLOCK_RETRIES   = 3;
const CLOCK_RETRY_BASE_MS = 500; // 500 ms, 1 s, 2 s

/** Returns true if the error is an AWS InvalidSignatureException caused by clock skew. */
function isClockSkewError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const e = err as Record<string, unknown>;
  return (
    e["name"] === "InvalidSignatureException" &&
    typeof e["message"] === "string" &&
    (e["message"] as string).includes("Signature expired")
  );
}

/**
 * Attempts to synchronise the OS clock using chronyc or ntpdate.
 * Swallows errors — if the sync tool is unavailable the retry will still run
 * and the SDK's own clock-skew correction may be sufficient for small drifts.
 */
function tryNtpSync(logger: ILogger): void {
  const cmds = [
    "chronyc makestep",          // Amazon Linux 2, AL2023, Ubuntu 20+
    "ntpdate -u pool.ntp.org",   // older Amazon Linux / Ubuntu
    "w32tm /resync /force",      // Windows (not typical for Node servers)
  ];

  for (const cmd of cmds) {
    try {
      logger.warn(`[ClockSync] Running: ${cmd}`);
      execSync(cmd, { timeout: 10_000, stdio: "pipe" });
      logger.warn(`[ClockSync] Clock synchronised successfully via: ${cmd}`);
      return;
    } catch {
      // command not found or permission denied — try the next one
    }
  }

  logger.warn(
    "[ClockSync] Could not sync clock automatically. " +
    "Please run one of the following on the server and restart if the " +
    "issue persists:\n" +
    "  sudo chronyc makestep\n" +
    "  sudo ntpdate -u pool.ntp.org\n" +
    "  sudo systemctl restart chronyd\n" +
    "  sudo systemctl restart systemd-timesyncd"
  );
}

/** Builds a fresh BedrockRuntimeClient with a new HTTP/2 handler instance. */
function buildBedrockClient(config: AppConfig): BedrockRuntimeClient {
  const handler = new NodeHttp2Handler({
    requestTimeout:         config.bedrock.requestTimeoutMs,
    sessionTimeout:         config.bedrock.sessionTimeoutMs,
    disableConcurrentStreams: false,
    maxConcurrentStreams:   config.bedrock.maxConcurrentStreams,
  });

  return new BedrockRuntimeClient({
    region:         config.aws.region,
    requestHandler: handler,
  });
}

@injectable()
export class BedrockStreamingService implements IStreamingService {
  private bedrockClient: BedrockRuntimeClient;
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
    this.bedrockClient = buildBedrockClient(config);
  }

  // ── IStreamingService ──────────────────────────────────────────────────────

  /**
   * Initiates the bidirectional Bedrock stream for a session.
   *
   * Clock-skew resilience
   * ─────────────────────
   * If AWS returns InvalidSignatureException (clock skew ≥ 5 min) we:
   *   1. Log a prominent warning with the exact clock skew from the error.
   *   2. Attempt an OS-level NTP sync (chronyc makestep / ntpdate).
   *   3. Re-create the BedrockRuntimeClient (clears the stale clock offset).
   *   4. Retry with exponential back-off up to MAX_CLOCK_RETRIES times.
   */
  async initiateStream(sessionId: string): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);

    let lastError: unknown;

    for (let attempt = 0; attempt <= MAX_CLOCK_RETRIES; attempt++) {
      try {
        await this._doInitiateStream(sessionId);
        return; // success — exit retry loop
      } catch (err) {
        lastError = err;

        if (!isClockSkewError(err)) {
          // Not a clock-skew error — fail immediately, no retry.
          throw err;
        }

        // ── Clock-skew recovery ──────────────────────────────────────────────
        const msg = (err as Error).message ?? "";
        this.logger.warn(
          `[ClockSync] ⚠ AWS InvalidSignatureException (clock skew detected). ` +
          `Attempt ${attempt + 1}/${MAX_CLOCK_RETRIES}. ` +
          `Error: ${msg}`,
          { sessionId }
        );

        if (attempt >= MAX_CLOCK_RETRIES) break; // exhausted retries

        // Step 1: Sync the OS clock.
        tryNtpSync(this.logger);

        // Step 2: Re-create the Bedrock client so it picks up the corrected time.
        this.logger.warn(
          "[ClockSync] Re-creating BedrockRuntimeClient with fresh clock offset.",
          { sessionId }
        );
        this.bedrockClient = buildBedrockClient(this.config);

        // Step 3: Exponential back-off before retrying.
        const delay = CLOCK_RETRY_BASE_MS * Math.pow(2, attempt);
        this.logger.warn(`[ClockSync] Retrying in ${delay} ms...`, { sessionId });
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }

    // All retries exhausted.
    this.logger.error(
      `[ClockSync] Stream failed after ${MAX_CLOCK_RETRIES} clock-skew retries. ` +
      "Please sync the server clock manually and restart the process.\n" +
      "  sudo chronyc makestep   (recommended on EC2)\n" +
      "  sudo ntpdate -u pool.ntp.org",
      { sessionId, err: lastError }
    );
    throw lastError;
  }

  /** Single stream attempt — extracted so the retry wrapper stays clean. */
  private async _doInitiateStream(sessionId: string): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);

    try {
      const activeSessions = this.sessions.getAllIds().length;
      this.logger.info("Initiating bidirectional stream", { sessionId, activeSessions });

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
      event: {
        sessionStart: {
          sessionId:             session.bedrockSessionId,
          inferenceConfiguration: session.inferenceConfig,
        },
      },
    });
    session.isSessionStartSent = true;
  }

  enqueuePromptStart(sessionId: string): void {
    const session = this.requireSession(sessionId);

    const audioOutputConfiguration = {
      ...DefaultAudioOutputConfiguration,
      voiceId: session.voiceId,
    };

    this.enqueue(sessionId, {
      event: {
        promptStart: {
          promptName: session.promptName,
          textOutputConfiguration:    { mediaType: "text/plain" },
          audioOutputConfiguration,
          toolUseOutputConfiguration: { mediaType: "application/json" },
          toolConfiguration: {
            tools: [
              {
                toolSpec: {
                  name:        "getDateAndTimeTool",
                  description: "Get information about the current date and time.",
                  inputSchema: { json: DefaultToolSchema },
                },
              },
              {
                toolSpec: {
                  name:        "getWeatherTool",
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
    this.logger.debug("Prompt start enqueued", {
      sessionId,
      voiceId: session.voiceId,
    });
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
    this.logger.info("[TRANSCRIPT] User (text)", { sessionId, text: content });
  }

  enqueueGreetingTrigger(sessionId: string, triggerText = "Hello!"): void {
    const session = this.requireSession(sessionId);
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

    // Guard: drop audio that arrives before the audio content block is open.
    // Between turns the Flutter recorder may still be streaming (gated)
    // silent frames.  Sending audioInput without a prior contentStart
    // causes Bedrock "No open content found" errors.
    if (!session.isAudioContentStartSent) return;

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

  async enqueueAudioGreeting(sessionId: string, audioData: Buffer): Promise<void> {
    const session = this.requireSession(sessionId);

    const CHUNK_SIZE = 3200;

    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: session.audioContentId,
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
    session.isAudioContentStartSent = true;

    let chunkCount = 0;
    for (let offset = 0; offset < audioData.length; offset += CHUNK_SIZE) {
      const chunk = audioData.subarray(offset, offset + CHUNK_SIZE);
      this.enqueue(sessionId, {
        event: {
          audioInput: {
            promptName: session.promptName,
            contentName: session.audioContentId,
            content: chunk.toString("base64"),
          },
        },
      });
      chunkCount++;
    }

    this.logger.debug("Greeting audio enqueued (content block left open for mic)", {
      sessionId,
      totalBytes: audioData.length,
      chunks: chunkCount,
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

  // ── Private helpers ────────────────────────────────────────────────────────

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
    const myGeneration = session.streamGeneration;

    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<
          IteratorResult<InvokeModelWithBidirectionalStreamInput>
        > => {
          try {
            while (true) {
              if (
                aborted ||
                !session.isActive ||
                !this.sessions.has(sessionId) ||
                session.streamGeneration !== myGeneration
              ) {
                this.logger.debug("[DEBUG] Iterable returning done:true", {
                  sessionId,
                  reason: aborted
                    ? "aborted"
                    : session.streamGeneration !== myGeneration
                    ? "generation_changed"
                    : !session.isActive
                    ? "session_inactive"
                    : "session_not_found",
                });
                return { value: undefined as any, done: true };
              }

              if (session.queue.length > 0) break;
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
                    reason: isClose
                      ? "closeSignal"
                      : aborted
                      ? "aborted"
                      : "session_inactive",
                  });
                  return { value: undefined as any, done: true };
                }
                this.logger.error("Unexpected race error", { sessionId, err });
              }
            }

            const nextEvent = session.queue.shift();
            const evKey =
              Object.keys((nextEvent as any)?.event ?? {})[0] ?? "unknown";

            if (evKey === "audioInput") {
              session.audioChunksSent = (session.audioChunksSent ?? 0) + 1;
              if (
                session.audioChunksSent === 1 ||
                session.audioChunksSent % 50 === 0
              ) {
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

  private async processResponseStream(
    sessionId: string,
    response: Awaited<ReturnType<BedrockRuntimeClient["send"]>> & {
      body: AsyncIterable<any>;
    }
  ): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session) return;

    // Reset per-turn speculative tracking.
    session.speculativeContentIds.clear();

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

      this.logger.info("[DEBUG] for-await loop exited", {
        sessionId,
        reason: session.isActive ? "bedrock_closed_stream" : "session_inactive",
        totalChunks: chunkCount,
      });

      session.receivedAudioOutput = audioOutputCount > 0;
      session.receivedTextOutput  = textOutputCount  > 0;

      this.logger.info("[DEBUG] Response loop ended", {
        sessionId,
        totalChunks:       chunkCount,
        audioOutputChunks: audioOutputCount,
        textOutputChunks:  textOutputCount,
        hadAudioOutput:    audioOutputCount > 0,
        hadTextOutput:     textOutputCount  > 0,
        isActive:          session.isActive,
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
        source:  "responseStream",
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
      const cs = ev.contentStart as Record<string, unknown>;
      // Bedrock uses "contentId" on response contentStart events.
      const contentId = (cs.contentId ?? cs.contentName) as string | undefined;
      // Track SPECULATIVE content blocks so we can suppress their audio.
      if (contentId) {
        let stage: string | undefined;
        try {
          const extra = typeof cs.additionalModelFields === "string"
            ? JSON.parse(cs.additionalModelFields)
            : cs.additionalModelFields;
          stage = (extra as Record<string, unknown>)?.generationStage as string | undefined;
        } catch { /* ignore parse errors */ }
        if (stage === "SPECULATIVE") {
          session.speculativeContentIds.add(contentId);
          this.logger.debug("[GenerationStage] SPECULATIVE content block — audio will be suppressed", {
            sessionId, contentId,
          });
        } else if (stage === "FINAL") {
          this.logger.debug("[GenerationStage] FINAL content block — audio will be forwarded", {
            sessionId, contentId,
          });
        }
      }
      this.dispatchEvent(sessionId, "contentStart", ev.contentStart);
    } else if (ev.textOutput) {
      const textOut = ev.textOutput as Record<string, unknown>;
      this.logger.info("[TRANSCRIPT] Assistant", {
        sessionId,
        role: textOut.role ?? "ASSISTANT",
        text: textOut.content,
      });
      this.dispatchEvent(sessionId, "textOutput", ev.textOutput);
    } else if (ev.audioOutput) {
      const ao = ev.audioOutput as Record<string, unknown>;
      // Bedrock uses "contentId" on contentStart but "contentName" on audioOutput —
      // check both to be safe.
      const audioContentId = (ao.contentId ?? ao.contentName) as string | undefined;
      if (audioContentId && session.speculativeContentIds.has(audioContentId)) {
        // Silently drop SPECULATIVE audio — only FINAL audio reaches the client.
        return;
      }
      this.dispatchEvent(sessionId, "audioOutput", ev.audioOutput);
    } else if (ev.toolUse) {
      this.dispatchEvent(sessionId, "toolUse", ev.toolUse);
      const toolUse = ev.toolUse as Record<string, unknown>;
      session.toolUseContent = toolUse;
      session.toolUseId      = toolUse.toolUseId as string;
      session.toolName       = toolUse.toolName  as string;
    } else if (
      ev.contentEnd &&
      (ev.contentEnd as Record<string, unknown>).type === "TOOL"
    ) {
      this.dispatchEvent(sessionId, "toolEnd", {
        toolUseContent: session.toolUseContent,
        toolUseId:      session.toolUseId,
        toolName:       session.toolName,
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
            source:   "toolExecution",
            toolName: session.toolName,
            details:  err instanceof Error ? err.message : String(err),
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

    const contentId    = randomUUID();
    const resultContent =
      typeof result === "string" ? result : JSON.stringify(result);

    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName:  session.promptName,
          contentName: contentId,
          interactive: false,
          type:        "TOOL",
          role:        "TOOL",
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
          promptName:  session.promptName,
          contentName: contentId,
          content:     resultContent,
        },
      },
    });
    this.enqueue(sessionId, {
      event: {
        contentEnd: {
          promptName:  session.promptName,
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