// src/application/use-cases/SessionUseCase.ts
import "reflect-metadata";
import { inject, injectable } from "tsyringe";
import { randomUUID } from "node:crypto";

import { TOKENS } from "../../infrastructure/config/tokens";
import type { ISessionRepository } from "../../domain/repositories/ISessionRepository";
import type { IStreamingService } from "../../domain/services/IStreamingService";
import type { ILogger } from "../../infrastructure/logging/ILogger";
import { GreetingAudioService } from "../../infrastructure/audio/GreetingAudioService";

import {
  SessionNotFoundError,
  SessionAlreadyExistsError,
  SessionInactiveError,
} from "../../domain/errors";
import {
  DefaultAudioInputConfiguration,
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

    const session = this.sessions.create(sessionId, inferenceConfig);
    this.logger.info("Session created", { sessionId });
    return session;
  }

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

  startPrompt(sessionId: string): void {
    const session = this.requireActiveSession(sessionId);

    if (!session.isSessionStartSent) {
      this.streaming.enqueueSessionStart(sessionId);
    }

    session.promptName = randomUUID();
    session.audioContentId = randomUUID();
    session.isPromptStartSent = false;
    session.isAudioContentStartSent = false;

    this.streaming.enqueuePromptStart(sessionId);
    this.logger.debug("Prompt started", {
      sessionId,
      firstTurn: !session.isSessionStartSent,
    });
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
   * Pre-fills the session queue with ONLY the session + system setup events
   * needed to unblock bedrockClient.send().
   *
   * MUST be awaited BEFORE startStream().
   *
   * Sequence enqueued:
   *   sessionStart → promptStart → systemPrompt (SYSTEM TEXT)
   *
   * ── Why NOT the audio greeting ─────────────────────────────────────────────
   * Nova Sonic is a real-time speech model.  When audio is pre-queued and
   * sent as a batch burst before the HTTP/2 stream opens, Nova Sonic processes
   * the bytes for token accounting only — its VAD/response engine is not yet
   * in real-time mode, so it never triggers response generation.
   * (Observed: speechTokens:157, outputTokens:0, stream hangs forever.)
   *
   * The audio greeting must be sent AFTER the stream is live.
   * See sendGreetingAudio() and the SocketGateway call site.
   *
   * ── Why this is still needed ───────────────────────────────────────────────
   * bedrockClient.send() blocks until Bedrock sends its first response event.
   * The system prompt alone is enough: Bedrock returns a usageEvent for those
   * tokens, which unblocks send() and resolves session.streamReady.
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
    this.setupSystemPrompt(sessionId, systemPrompt);

    this.logger.info("Session + system prompt pre-enqueued (stream not started yet)", {
      sessionId,
    });
  }

  /**
   * Sends the pre-recorded audio greeting into the LIVE bidirectional stream.
   *
   * MUST be called AFTER session.streamReady resolves (i.e. after
   * bedrockClient.send() has received its first Bedrock response and the
   * HTTP/2 connection is fully established).
   *
   * Sending audio into a live stream mirrors exactly how Turn 2+ works when
   * the user speaks.  Nova Sonic's VAD and response engine are active and
   * process the audio in real-time, generating a spoken response.
   */
  sendGreetingAudio(sessionId: string): void {
    this.requireActiveSession(sessionId);
    this.streaming.enqueueAudioGreeting(
      sessionId,
      this.greetingAudio.getLpcmBuffer()
    );
    this.logger.info("Greeting audio sent into live stream", { sessionId });
  }

  /**
   * Prepares the session for a new Bedrock stream after turnComplete.
   * Resets all per-turn state and pre-enqueues sessionStart so send()
   * has at least one event to transmit when the new HTTP/2 connection opens.
   *
   * Call this BEFORE startStream() in the streamComplete handler.
   */
  prepareNextStream(sessionId: string): void {
    const session = this.requireActiveSession(sessionId);

    session.isSessionStartSent = false;
    session.promptName = randomUUID();
    session.audioContentId = randomUUID();
    session.isPromptStartSent = false;
    session.isAudioContentStartSent = false;

    this.streaming.enqueueSessionStart(sessionId);

    this.logger.debug("Next stream prepared — sessionStart pre-enqueued", {
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