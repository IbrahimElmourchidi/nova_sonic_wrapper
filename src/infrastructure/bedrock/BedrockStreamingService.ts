// src/infrastructure/bedrock/BedrockStreamingService.ts
//
// SETUP: run `npm install @aws-sdk/client-polly` before starting.
// IAM: the server's role needs polly:SynthesizeSpeech permission.
//
import "reflect-metadata";
import { inject, injectable } from "tsyringe";
import {
  BedrockRuntimeClient,
  InvokeModelWithBidirectionalStreamCommand,
  type InvokeModelWithBidirectionalStreamInput,
} from "@aws-sdk/client-bedrock-runtime";
import {
  PollyClient,
  SynthesizeSpeechCommand,
  Engine,
  OutputFormat,
  VoiceId,
} from "@aws-sdk/client-polly";
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
  DefaultAudioInputConfiguration,
  DefaultAudioOutputConfiguration,
  DefaultToolSchema,
  WeatherToolSchema,
} from "../config/defaults";

// ── Greeting audio chunk size ────────────────────────────────────────────────
//
// Nova Sonic's ASR works best with audio delivered in small chunks that
// simulate real microphone input.  Sending the entire Polly blob as a single
// audioInput event with interactive:true (real-time VAD mode) results in
// speechTokens:0 because Nova's VAD never fires on a pre-queued blob.
//
// Fix:
//   1. Use interactive:false  →  Nova treats audio as pre-recorded (no VAD).
//   2. Split audio into GREETING_CHUNK_BYTES chunks  →  Nova's ASR pipeline
//      processes each chunk in sequence, matching the expected framing.
//
// 3200 bytes = 100 ms of 16 kHz / 16-bit / mono LPCM (the same format as the
// live microphone stream).  This is the same cadence Flutter sends mic audio.
// ─────────────────────────────────────────────────────────────────────────────
const GREETING_CHUNK_BYTES = 3200; // 100 ms per chunk

@injectable()
export class BedrockStreamingService implements IStreamingService {
  private readonly bedrockClient: BedrockRuntimeClient;
  private readonly pollyClient: PollyClient;
  private readonly sessionEventLog = new Map<string, unknown[]>();

  // Cache Polly audio keyed by greeting text — Polly is only called once per
  // unique phrase for the lifetime of the server process.
  private readonly greetingAudioCache = new Map<string, string>();

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

    this.pollyClient = new PollyClient({ region: config.aws.region });
  }

  // ── IStreamingService ────────────────────────────────────────────────────

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
            return { [key]: { ...val, content: `<${Buffer.byteLength(val.content as string, "base64")} bytes>` } };
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
                  description: "Get the current weather for a given location, based on its WGS84 coordinates.",
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

  enqueueSystemPrompt(sessionId: string, content: string, textConfig: TextConfiguration): void {
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
      event: { textInput: { promptName: session.promptName, contentName: contentId, content } },
    });
    this.enqueue(sessionId, {
      event: { contentEnd: { promptName: session.promptName, contentName: contentId } },
    });

    this.logger.debug("System prompt enqueued", { sessionId });
  }

  enqueueUserText(sessionId: string, content: string): void {
    const session = this.requireSession(sessionId);

    if (session.isAudioContentStartSent) {
      this.enqueue(sessionId, {
        event: { contentEnd: { promptName: session.promptName, contentName: session.audioContentId } },
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
      event: { textInput: { promptName: session.promptName, contentName: contentId, content } },
    });
    this.enqueue(sessionId, {
      event: { contentEnd: { promptName: session.promptName, contentName: contentId } },
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
   * Synthesises greetingText via Amazon Polly (LPCM 16 kHz / 16-bit / mono)
   * and enqueues it as a complete audio content block to trigger a Nova Sonic
   * greeting response.
   *
   * ── Key fixes vs. the broken approach ────────────────────────────────────
   *
   * BROKEN (before):
   *   - interactive: true  →  Nova applies real-time VAD, expects continuous
   *     live mic chunks.  A single pre-queued blob never triggers VAD end →
   *     speechTokens: 0 → Nova stays silent.
   *   - Single audioInput event for the whole Polly blob.
   *
   * FIXED (this version):
   *   - interactive: false  →  Nova treats the audio as a complete pre-recorded
   *     block.  No VAD; Nova's ASR processes the audio as-is when contentEnd
   *     is received.
   *   - Audio split into GREETING_CHUNK_BYTES (100 ms) chunks  →  matches the
   *     framing Nova's audio pipeline expects, identical to live mic cadence.
   *
   * A dedicated greetingContentId is used so session.audioContentId (the live
   * mic block opened later) is never touched and isAudioContentStartSent stays
   * false until the user's first real mic turn.
   * ─────────────────────────────────────────────────────────────────────────
   */
  async enqueueGreetingAudio(sessionId: string, greetingText: string): Promise<void> {
    const session = this.requireSession(sessionId);
    const greetingContentId = randomUUID();
    const audioBase64 = await this.getOrSynthesiseGreetingAudio(greetingText);
    const audioBuffer = Buffer.from(audioBase64, "base64");

    // ── 1. Content start — pre-recorded, no VAD ───────────────────────────
    this.enqueue(sessionId, {
      event: {
        contentStart: {
          promptName: session.promptName,
          contentName: greetingContentId,
          type: "AUDIO",
          interactive: false,   // ← FIX: pre-recorded audio, not live mic
          role: "USER",
          audioInputConfiguration: DefaultAudioInputConfiguration,
        },
      },
    });

    // ── 2. Audio in chunks (100 ms each) ─────────────────────────────────
    // Chunking mirrors the cadence of real microphone audio so Nova's ASR
    // pipeline receives properly-framed audio segments.
    let chunkCount = 0;
    for (let offset = 0; offset < audioBuffer.length; offset += GREETING_CHUNK_BYTES) {
      const end = Math.min(offset + GREETING_CHUNK_BYTES, audioBuffer.length);
      const chunk = audioBuffer.subarray(offset, end);
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

    // ── 3. Content end — signals complete pre-recorded audio block ────────
    this.enqueue(sessionId, {
      event: { contentEnd: { promptName: session.promptName, contentName: greetingContentId } },
    });

    this.logger.debug("Greeting audio enqueued", {
      sessionId,
      greetingText,
      audioBytes: audioBuffer.length,
      chunkCount,
      chunkSizeBytes: GREETING_CHUNK_BYTES,
    });
  }

  enqueueGreetingSilence(sessionId: string): void {
    const session = this.requireSession(sessionId);
    const greetingContentId = randomUUID();
    const GREETING_SILENCE_MS = 300;
    const silenceBytes = Math.ceil((GREETING_SILENCE_MS / 1000) * 16000 * 2);

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
    this.enqueue(sessionId, {
      event: {
        audioInput: {
          promptName: session.promptName,
          contentName: greetingContentId,
          content: Buffer.alloc(silenceBytes, 0).toString("base64"),
        },
      },
    });
    this.enqueue(sessionId, {
      event: { contentEnd: { promptName: session.promptName, contentName: greetingContentId } },
    });

    this.logger.debug("Greeting silence enqueued", { sessionId, silenceMs: GREETING_SILENCE_MS });
  }

  async enqueueContentEnd(sessionId: string): Promise<void> {
    const session = this.requireSession(sessionId);
    this.enqueue(sessionId, {
      event: { contentEnd: { promptName: session.promptName, contentName: session.audioContentId } },
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
      this.logger.info("Session events written", { sessionId, path: outputPath, eventCount: events.length });
      this.sessionEventLog.delete(sessionId);
    }

    session.isActive = false;
    session.closeSignal.next();
    session.closeSignal.complete();
    this.sessions.delete(sessionId);
    this.logger.info("Session ended", { sessionId });
  }

  // ── Polly synthesis ──────────────────────────────────────────────────────

  private async getOrSynthesiseGreetingAudio(text: string): Promise<string> {
    const cached = this.greetingAudioCache.get(text);
    if (cached) return cached;

    try {
      this.logger.info("Synthesising greeting audio via Polly", { text });

      const response = await this.pollyClient.send(
        new SynthesizeSpeechCommand({
          Engine:       Engine.NEURAL,
          OutputFormat: OutputFormat.PCM,
          SampleRate:   "16000",        // Matches DefaultAudioInputConfiguration
          VoiceId:      VoiceId.Joanna,
          Text:         text,
        })
      );

      if (!response.AudioStream) throw new Error("Polly returned no AudioStream");

      const chunks: Uint8Array[] = [];
      for await (const chunk of response.AudioStream as AsyncIterable<Uint8Array>) {
        chunks.push(chunk);
      }

      const audioBuffer = Buffer.concat(chunks);
      const audioBase64 = audioBuffer.toString("base64");
      this.greetingAudioCache.set(text, audioBase64);

      this.logger.info("Polly synthesis complete, audio cached", {
        text,
        audioBytes: audioBuffer.byteLength,
        durationMs: Math.round((audioBuffer.byteLength / (16000 * 2)) * 1000),
      });

      return audioBase64;
    } catch (err) {
      this.logger.warn(
        "Polly synthesis failed — Nova WILL NOT greet. " +
        "Ensure the IAM role/user has polly:SynthesizeSpeech permission.",
        { text, err }
      );
      // 1 s LPCM silence as last-resort fallback (Nova will stay silent)
      return Buffer.alloc(16_000 * 2, 0).toString("base64");
    }
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

    if (this.config.logging.logSessionEvents) {
      if (!this.sessionEventLog.has(sessionId)) this.sessionEventLog.set(sessionId, []);
      this.sessionEventLog.get(sessionId)!.push(this.sanitizeForLog(event));
    }
  }

  private sanitizeForLog(event: unknown): unknown {
    const audioInput = ((event as any)?.event as Record<string, unknown>)
      ?.audioInput as Record<string, unknown> | undefined;
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

    let aborted = false;

    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<IteratorResult<InvokeModelWithBidirectionalStreamInput>> => {
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
                const isClose = err instanceof Error && err.message === "Stream closed";
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
            const evKey = Object.keys((nextEvent as any)?.event ?? {})[0] ?? "unknown";

            this.logger.debug("[DEBUG] Sending event to Bedrock", {
              sessionId,
              eventType: evKey,
              remainingQueue: session.queue.length,
            });

            return {
              value: {
                chunk: { bytes: new TextEncoder().encode(JSON.stringify(nextEvent)) },
              },
              done: false,
            };
          } catch (err) {
            this.logger.error("Iterator error", { sessionId, err });
            aborted = true;
            return { value: undefined as any, done: true };
          }
        },

        return: async () => { aborted = true; return { value: undefined as any, done: true }; },
        throw: async (err: unknown) => { aborted = true; throw err; },
      }),
    };
  }

  private async processResponseStream(
    sessionId: string,
    response: Awaited<ReturnType<BedrockRuntimeClient["send"]>> & { body: AsyncIterable<any> }
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
                  ? { audioOutput: { contentName: parsed.event.audioOutput?.contentName, bytes: `<${(parsed.event.audioOutput?.content ?? "").length} base64-chars>` } }
                  : parsed?.event,
            });
          } catch {
            this.logger.info("[DEBUG] Non-JSON chunk from Bedrock", { sessionId, text });
          }

          this.handleResponseEvent(sessionId, session, text);
        } else if (event.modelStreamErrorException) {
          this.logger.error("Model stream error", { sessionId, details: event.modelStreamErrorException });
          this.dispatchEvent(sessionId, "error", { type: "modelStreamErrorException", details: event.modelStreamErrorException });
        } else if (event.internalServerException) {
          this.logger.error("Internal server error from Bedrock", { sessionId, details: event.internalServerException });
          this.dispatchEvent(sessionId, "error", { type: "internalServerException", details: event.internalServerException });
        }
      }

      this.logger.info("[DEBUG] Response loop ended", { sessionId, totalChunks: chunkCount, isActive: session.isActive });

      if (session.isActive) {
        this.logger.info("Response stream complete", { sessionId });
        this.dispatchEvent(sessionId, "streamComplete", { timestamp: new Date().toISOString() });
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

  private handleResponseEvent(sessionId: string, session: SessionData, rawText: string): void {
    let json: Record<string, unknown>;
    try {
      json = JSON.parse(rawText);
    } catch {
      this.logger.debug("Failed to parse response chunk", { sessionId, rawText });
      return;
    }

    const ev = json.event as Record<string, unknown> | undefined;
    if (!ev) { this.dispatchEvent(sessionId, "unknown", json); return; }

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
      this.toolService
        .execute(session.toolName, session.toolUseContent)
        .then((result) => {
          this.sendToolResult(sessionId, session.toolUseId, result);
          this.dispatchEvent(sessionId, "toolResult", { toolUseId: session.toolUseId, result });
        })
        .catch((err) => {
          this.logger.error("Tool execution failed", { sessionId, toolName: session.toolName, err });
          this.dispatchEvent(sessionId, "error", { source: "toolExecution", toolName: session.toolName, details: err instanceof Error ? err.message : String(err) });
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

  private sendToolResult(sessionId: string, toolUseId: string, result: unknown): void {
    const session = this.sessions.findById(sessionId);
    if (!session?.isActive) return;

    const contentId = randomUUID();
    const resultContent = typeof result === "string" ? result : JSON.stringify(result);

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
      event: { toolResult: { promptName: session.promptName, contentName: contentId, content: resultContent } },
    });
    this.enqueue(sessionId, {
      event: { contentEnd: { promptName: session.promptName, contentName: contentId } },
    });

    this.logger.debug("Tool result enqueued", { sessionId, toolUseId });
  }

  private dispatchEvent(sessionId: string, eventType: string, data: unknown): void {
    const session = this.sessions.findById(sessionId);
    if (!session) return;

    const handler = session.responseHandlers.get(eventType);
    if (handler) {
      try { handler(data); }
      catch (err) { this.logger.error(`Handler error for event '${eventType}'`, { sessionId, err }); }
    }

    const anyHandler = session.responseHandlers.get("any");
    if (anyHandler) {
      try { anyHandler({ type: eventType, data }); }
      catch (err) { this.logger.error("Wildcard handler error", { sessionId, err }); }
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}