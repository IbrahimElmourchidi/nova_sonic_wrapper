// src/application/use-cases/SessionUseCase.ts
import "reflect-metadata";
import { inject, injectable } from "tsyringe";
import { randomUUID } from "node:crypto";

import { TOKENS } from "../../infrastructure/config/tokens";
import type { ISessionRepository } from "../../domain/repositories/ISessionRepository";
import type { IStreamingService }   from "../../domain/services/IStreamingService";
import type { ILogger }             from "../../infrastructure/logging/ILogger";
import { GreetingAudioService }     from "../../infrastructure/audio/GreetingAudioService";

import {
  SessionNotFoundError,
  SessionAlreadyExistsError,
  SessionInactiveError,
} from "../../domain/errors";

import {
  DefaultAudioInputConfiguration,
  DefaultTextConfiguration,
  DefaultInferenceConfiguration,
  buildGermanTutorSystemPrompt,
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
    private readonly logger: ILogger,

    @inject(TOKENS.GreetingAudioService)
    private readonly greetingAudio: GreetingAudioService
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

    // Pass topic and voiceId so they are stored on the session entity for use
    // when building the system prompt and the audio output configuration.
    const session = this.sessions.create(
      sessionId,
      inferenceConfig,
      request.topic,
      request.voiceId
    );

    this.logger.info("Session created", {
      sessionId,
      topic:   session.topic,
      voiceId: session.voiceId,
    });
    return session;
  }

  startStream(sessionId: string): void {
    this.streaming.initiateStream(sessionId).catch((err) => {
      this.logger.error("Stream terminated with error", { sessionId, err });
    });
  }

  /**
   * Prepares the session for the next stream turn (Turn 2+).
   * Resets session-start tracking so a fresh sessionStart + promptStart are
   * enqueued before the new HTTP/2 connection is opened by `startStream`.
   */
  prepareNextStream(sessionId: string): void {
    const session = this.requireActiveSession(sessionId);

    session.isSessionStartSent      = false;
    session.promptName              = randomUUID();
    session.audioContentId          = randomUUID();
    session.isPromptStartSent       = false;
    session.isAudioContentStartSent = false;

    this.streaming.enqueueSessionStart(sessionId);
    this.streaming.enqueuePromptStart(sessionId);
    this.logger.debug("Next stream prepared (session + prompt start enqueued)", { sessionId });
  }

  // ── Session setup events (in order) ──────────────────────────────────────

  setupSessionAndPromptStart(sessionId: string): void {
    this.requireActiveSession(sessionId);
    this.streaming.enqueueSessionStart(sessionId);
    this.streaming.enqueuePromptStart(sessionId);
    this.logger.debug("Session + prompt start enqueued", { sessionId });
  }

  startPrompt(sessionId: string): void {
    const session = this.requireActiveSession(sessionId);

    if (!session.isSessionStartSent) {
      this.streaming.enqueueSessionStart(sessionId);
    }

    session.promptName              = randomUUID();
    session.audioContentId          = randomUUID();
    session.isPromptStartSent       = false;
    session.isAudioContentStartSent = false;

    this.streaming.enqueuePromptStart(sessionId);
    this.logger.debug("Prompt started", { sessionId });
  }

  /**
   * Enqueues a system prompt.
   *
   * If `request` is provided its `content` is used verbatim — this supports
   * the legacy `socket.on("systemPrompt", …)` path.
   *
   * If `request` is omitted the prompt is auto-built from the session's
   * stored `topic` using the professional German tutor template.  This is the
   * path taken by `preEnqueueAutoGreeting`.
   */
  setupSystemPrompt(
    sessionId: string,
    request?: SystemPromptRequest
  ): void {
    const session    = this.requireActiveSession(sessionId);
    const textConfig = request?.textConfig ?? DefaultTextConfiguration;

    const content = request?.content ?? buildGermanTutorSystemPrompt(session.topic);

    this.streaming.enqueueSystemPrompt(sessionId, content, textConfig);
    this.logger.debug("System prompt enqueued", {
      sessionId,
      topic:        session.topic,
      promptSource: request?.content ? "caller-provided" : "auto-generated",
    });
  }

  sendUserText(sessionId: string, content: string): void {
    this.requireActiveSession(sessionId);
    this.streaming.enqueueUserText(sessionId, content);
    this.logger.debug("User text enqueued", { sessionId, content });
  }

  setupAudioStart(sessionId: string, dto: AudioStartDto = {}): void {
    const session = this.requireActiveSession(sessionId);

    if (session.isAudioContentStartSent) {
      this.logger.debug(
        "Audio start skipped — content block already open (greeting)",
        { sessionId }
      );
      return;
    }

    const audioConfig = { ...DefaultAudioInputConfiguration, ...dto.audioConfig };
    this.streaming.enqueueAudioContentStart(sessionId, audioConfig);
    this.logger.debug("Audio start enqueued", { sessionId });
  }

  // ── Auto-greeting ─────────────────────────────────────────────────────────

  /**
   * Pre-fills the session queue with: sessionStart → promptStart → systemPrompt.
   * These three events are enough to unblock `bedrockClient.send()`.
   *
   * The German tutor prompt is built automatically from `session.topic`.
   * An explicit `systemPrompt` override is accepted for testing / admin use.
   *
   * MUST be awaited BEFORE `startStream()`.
   */
  async preEnqueueAutoGreeting(
    sessionId: string,
    systemPrompt?: SystemPromptRequest
  ): Promise<void> {
    const session = this.requireActiveSession(sessionId);

    if (session.isSessionStartSent || session.isPromptStartSent) {
      this.logger.warn("preEnqueueAutoGreeting skipped — queue already seeded", {
        sessionId,
      });
      return;
    }

    this.streaming.enqueueSessionStart(sessionId);
    this.streaming.enqueuePromptStart(sessionId);
    this.setupSystemPrompt(sessionId, systemPrompt); // auto-builds from topic if no override

    this.logger.info("Session + German-tutor system prompt pre-enqueued", {
      sessionId,
      topic:   session.topic,
      voiceId: session.voiceId,
    });
  }

  /**
   * Sends the greeting audio into the live Bedrock stream.
   * MUST be called after `session.streamReady` has resolved.
   */
  async sendGreetingAudio(sessionId: string): Promise<void> {
    this.requireActiveSession(sessionId);

    const info = this.greetingAudio.getInfo();
    this.logger.info("Sending greeting into stream", {
      sessionId,
      greetingFilePath: info.path,
      lpcmSizeKB:       info.lpcmSizeKB,
      lpcmSizeBytes:    info.lpcmSizeBytes,
      durationMs:       info.durationMs,
    });

    await this.streaming.enqueueAudioGreeting(
      sessionId,
      this.greetingAudio.getLpcmBuffer()
    );

    this.logger.info("Greeting fully streamed", { sessionId });
  }

  // ── Continued from original (unchanged methods kept for completeness) ────

  streamAudio(sessionId: string, audioData: Buffer): void {
    this.requireActiveSession(sessionId);
    this.streaming.enqueueAudioChunk(sessionId, audioData);
  }

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

  registerEventHandler(
    sessionId: string,
    eventType: string,
    handler: EventHandler
  ): void {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);
    session.responseHandlers.set(eventType, handler as (data: unknown) => void);
  }

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
      this.logger.warn("Force-closing idle session", { sessionId, idleThresholdMs });
      this.forceCloseSession(sessionId);
    });
  }

  private requireActiveSession(sessionId: string): SessionData {
    const session = this.sessions.findById(sessionId);
    if (!session) throw new SessionNotFoundError(sessionId);
    if (!session.isActive) throw new SessionInactiveError(sessionId);
    return session;
  }
}