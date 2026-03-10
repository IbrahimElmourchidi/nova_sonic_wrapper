// src/application/use-cases/SessionUseCase.ts
import "reflect-metadata";
import { inject, injectable } from "tsyringe";
import { randomUUID } from "node:crypto";

import { TOKENS } from "../../infrastructure/config/tokens";
import type { ISessionRepository } from "../../domain/repositories/ISessionRepository";
import type { IStreamingService } from "../../domain/services/IStreamingService";
import type { ILogger } from "../../infrastructure/logging/ILogger";

import {
  SessionNotFoundError,
  SessionAlreadyExistsError,
  SessionInactiveError,
} from "../../domain/errors";
import {
  DefaultAudioInputConfiguration,
  DefaultAudioOutputConfiguration,
  DefaultSystemPrompt,
  DefaultTextConfiguration,
  DefaultInferenceConfiguration,
} from "../../infrastructure/config/defaults";

import type {
  InitializeSessionRequest,
  SystemPromptRequest,
  AudioStartDto,
} from "../dtos/SessionDtos";
import type { EventHandler } from "../../domain/types";
import type { SessionData } from "../../domain/entities/Session";

@injectable()
export class SessionUseCase {
  constructor(
    @inject(TOKENS.SessionRepository)
    private readonly sessions: ISessionRepository,

    @inject(TOKENS.StreamingService)
    private readonly streaming: IStreamingService,

    @inject(TOKENS.Logger)
    private readonly logger: ILogger
  ) {}

  // ── Session lifecycle ─────────────────────────────────────────────────────

  createSession(request: InitializeSessionRequest): SessionData {
    const sessionId = request.sessionId ?? randomUUID();

    if (this.sessions.has(sessionId)) {
      throw new SessionAlreadyExistsError(sessionId);
    }

    const inferenceConfig = {
      ...DefaultInferenceConfiguration,
      ...request.inferenceConfig,
    };

    const session = this.sessions.create(sessionId, inferenceConfig);
    this.logger.info("Session created", { sessionId });
    return session;
  }

  /**
   * Starts the bidirectional Bedrock stream. Fire-and-forget; promise is NOT awaited
   * by the caller so the socket handler returns immediately.
   */
  startStream(sessionId: string): void {
    this.streaming.initiateStream(sessionId).catch((err) => {
      this.logger.error("Stream terminated with error", { sessionId, err });
    });
  }

  // ── Session setup events (in order) ──────────────────────────────────────

  setupSessionAndPromptStart(sessionId: string): void {
    this.requireActiveSession(sessionId);
    this.streaming.enqueueSessionStart(sessionId);
    this.streaming.enqueuePromptStart(sessionId);
    this.logger.debug("Session + prompt start enqueued", { sessionId });
  }

  /**
   * Starts a new prompt within an existing session.
   * Sends sessionStart only on the first call; resets prompt-level state each time.
   */
  startPrompt(sessionId: string): void {
    const session = this.requireActiveSession(sessionId);

    if (!session.isSessionStartSent) {
      this.streaming.enqueueSessionStart(sessionId);
    }

    // Reset prompt-level state for the new turn
    session.promptName = randomUUID();
    session.audioContentId = randomUUID();
    session.isPromptStartSent = false;
    session.isAudioContentStartSent = false;

    this.streaming.enqueuePromptStart(sessionId);
    this.logger.debug("Prompt started", { sessionId, firstTurn: !session.isSessionStartSent });
  }

  setupSystemPrompt(
    sessionId: string,
    request: SystemPromptRequest = { content: DefaultSystemPrompt }
  ): void {
    this.requireActiveSession(sessionId);
    const textConfig = request.textConfig ?? DefaultTextConfiguration;
    this.streaming.enqueueSystemPrompt(sessionId, request.content, textConfig);
    this.logger.debug("System prompt enqueued", { sessionId });
  }

  sendUserText(sessionId: string, content: string): void {
    this.requireActiveSession(sessionId);
    this.streaming.enqueueUserText(sessionId, content);
    this.logger.debug("User text enqueued", { sessionId, content });
  }

  setupAudioStart(sessionId: string, dto: AudioStartDto = {}): void {
    this.requireActiveSession(sessionId);
    const audioConfig = {
      ...DefaultAudioInputConfiguration,
      ...dto.audioConfig,
    };
    this.streaming.enqueueAudioContentStart(sessionId, audioConfig);
    this.logger.debug("Audio start enqueued", { sessionId });
  }

  // ── Auto-greeting ─────────────────────────────────────────────────────────

  /**
   * Pre-fills the session queue with the full greeting event sequence so that
   * when startStream() opens the Bedrock HTTP/2 connection the SDK immediately
   * has events to send.
   *
   * MUST be called BEFORE startStream() — this is what breaks the deadlock:
   *
   *   bedrockClient.send() resolves only after Bedrock receives the first event.
   *   Bedrock only gets events when the async-iterable queue is non-empty.
   *   If we wait for streamReady before enqueuing (as we did before), the queue
   *   is always empty when send() starts → send() blocks forever → deadlock.
   *
   * Sequence enqueued: sessionStart → promptStart → systemPrompt →
   *   userText → contentEnd → promptEnd
   *
   * All enqueue* methods are synchronous (they just push to session.queue).
   * The SDK drains the queue as soon as the connection is open.
   */
  preEnqueueAutoGreeting(
    sessionId: string,
    greetingText = "hi",
    systemPrompt?: SystemPromptRequest
  ): void {
    const session = this.requireActiveSession(sessionId);

    // Safety: if something already put events in the queue, bail out.
    if (session.isSessionStartSent || session.isPromptStartSent) {
      this.logger.warn("preEnqueueAutoGreeting skipped — queue already seeded", {
        sessionId,
      });
      return;
    }

    this.streaming.enqueueSessionStart(sessionId);
    this.streaming.enqueuePromptStart(sessionId);
    this.setupSystemPrompt(sessionId, systemPrompt);

    // Nova Sonic requires at least one AUDIO content block per prompt — a
    // text-only prompt is rejected with "must have at least one audio content".
    // We satisfy this by sending a short block of raw LPCM silence (300 ms).
    // This is preferable to a real audio file because:
    //   • No file I/O or format conversion needed at startup
    //   • Zero bytes = guaranteed silence, no accidental noise
    //   • Silence + the system prompt is enough for Nova to generate a greeting
    this.streaming.enqueueGreetingSilence(sessionId);

    // promptEnd tells Nova to start generating. enqueuePromptEnd is declared
    // async only because of a post-enqueue delay; the actual push is sync.
    void this.streaming.enqueuePromptEnd(sessionId);

    this.logger.info("Auto-greeting pre-enqueued (stream not started yet)", {
      sessionId,
    });
  }


  // ── Audio streaming ───────────────────────────────────────────────────────

  streamAudio(sessionId: string, audioData: Buffer): void {
    this.requireActiveSession(sessionId);
    this.streaming.enqueueAudioChunk(sessionId, audioData);
  }

  // ── Teardown sequence ─────────────────────────────────────────────────────

  async endAudioContent(sessionId: string): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session || !session.isAudioContentStartSent) return;
    await this.streaming.enqueueContentEnd(sessionId);
    this.logger.debug("Audio content end enqueued", { sessionId });
  }

  async endPrompt(sessionId: string): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session || !session.isPromptStartSent) return;
    await this.streaming.enqueuePromptEnd(sessionId);
    this.logger.debug("Prompt end enqueued", { sessionId });
  }

  async closeSession(sessionId: string): Promise<void> {
    const session = this.sessions.findById(sessionId);
    if (!session) return;
    await this.streaming.enqueueSessionEnd(sessionId);
    this.logger.info("Session closed", { sessionId });
  }

  forceCloseSession(sessionId: string): void {
    const session = this.sessions.findById(sessionId);
    if (!session) return;

    session.isActive = false;
    session.closeSignal.next();
    session.closeSignal.complete();
    this.sessions.delete(sessionId);
    this.logger.warn("Session force-closed", { sessionId });
  }

  // ── Event handlers ────────────────────────────────────────────────────────

  registerEventHandler(
    sessionId: string,
    eventType: string,
    handler: EventHandler
  ): void {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);
    session.responseHandlers.set(eventType, handler as (data: unknown) => void);
  }

  // ── Queries ───────────────────────────────────────────────────────────────

  getSession(sessionId: string): SessionData {
    return this.requireActiveSession(sessionId);
  }

  isSessionActive(sessionId: string): boolean {
    const session = this.sessions.findById(sessionId);
    return !!session?.isActive;
  }

  getActiveSessions(): string[] {
    return this.sessions.getAllIds();
  }

  getLastActivityTime(sessionId: string): number {
    return this.sessions.findById(sessionId)?.lastActivity ?? 0;
  }

  cleanupIdleSessions(idleThresholdMs: number): void {
    const idle = this.sessions.getIdleSessionIds(idleThresholdMs);
    idle.forEach((sessionId) => {
      this.logger.warn("Force-closing idle session", {
        sessionId,
        idleThresholdMs,
      });
      this.forceCloseSession(sessionId);
    });
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private requireActiveSession(sessionId: string): SessionData {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);
    if (!session.isActive) throw new SessionInactiveError(sessionId);
    return session;
  }
}