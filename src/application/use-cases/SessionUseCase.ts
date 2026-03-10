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

// ── Default greeting text ────────────────────────────────────────────────────
//
// "hi" (600 ms) is too short — Nova Sonic's ASR may not accumulate enough
// speech energy to produce a reliable transcript and respond.
//
// A full sentence gives Polly ~2 s of speech, which is long enough for Nova's
// ASR to confidently transcribe and for the LLM to decide to respond.
//
// This text is cached by Polly on first call; subsequent sessions are instant.
// ─────────────────────────────────────────────────────────────────────────────
const DEFAULT_GREETING_TEXT = "Hello, how are you today?";

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
   * Starts the bidirectional Bedrock stream. Fire-and-forget; promise is NOT
   * awaited by the caller so the socket handler returns immediately.
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
   * Synthesises the greeting via Polly then pre-fills the session queue with
   * the full event sequence so that when startStream() opens the HTTP/2
   * connection the SDK has data to transmit immediately.
   *
   * MUST be awaited BEFORE startStream():
   *   bedrockClient.send() blocks until Bedrock sends its first response.
   *   Bedrock only responds after receiving sessionStart.
   *   If the queue is empty when send() starts → deadlock.
   *   Pre-filling the queue here breaks the cycle.
   *
   * Polly is called only once per unique greetingText; subsequent calls return
   * the cached audio instantly.
   *
   * Sequence enqueued:
   *   sessionStart → promptStart → systemPrompt →
   *   greetingAudio(Polly LPCM, interactive:false, chunked) → userText → promptEnd
   *
   * Why interactive:false matters:
   *   interactive:true = real-time VAD mode (for live mic streams).
   *   interactive:false = pre-recorded mode (no VAD, Nova processes audio as-is).
   *   Sending a pre-queued Polly blob with interactive:true causes speechTokens:0
   *   because Nova's VAD never fires on a pre-packaged blob → no response.
   *   With interactive:false, Nova's ASR processes the complete audio block
   *   and the LLM generates a greeting response.
   */
  async preEnqueueAutoGreeting(
    sessionId: string,
    greetingText = DEFAULT_GREETING_TEXT,
    systemPrompt?: SystemPromptRequest
  ): Promise<void> {
    const session = this.requireActiveSession(sessionId);

    // Safety: bail out if the queue was already seeded.
    if (session.isSessionStartSent || session.isPromptStartSent) {
      this.logger.warn("preEnqueueAutoGreeting skipped — queue already seeded", {
        sessionId,
      });
      return;
    }

    this.streaming.enqueueSessionStart(sessionId);
    this.streaming.enqueuePromptStart(sessionId);
    this.setupSystemPrompt(sessionId, systemPrompt);

    // Polly audio (interactive:false, chunked) → Nova ASR transcribes the
    // greeting → LLM generates and streams an audio response.
    await this.streaming.enqueueGreetingAudio(sessionId, greetingText);

    // Text hint: reinforces the transcript in case ASR confidence is marginal.
    this.streaming.enqueueUserText(sessionId, greetingText);

    // promptEnd tells Nova to start generating. The actual queue push is
    // synchronous; we call without awaiting the post-enqueue delay.
    void this.streaming.enqueuePromptEnd(sessionId);

    this.logger.info("Auto-greeting pre-enqueued (stream not started yet)", {
      sessionId,
      greetingText,
    });
  }

  /**
   * Prepares the session for a new Bedrock stream after turnComplete.
   *
   * Each call to InvokeModelWithBidirectionalStreamCommand opens a brand-new
   * HTTP/2 connection that needs its own sessionStart event. Without this,
   * the turn-2+ queue is empty when send() starts, send() blocks waiting for
   * Bedrock, Bedrock waits for sessionStart, sessionStart never arrives
   * → deadlock → Flutter "cannot restart mic (timeout)".
   *
   * Call this BEFORE startStream() in the streamComplete handler.
   */
  prepareNextStream(sessionId: string): void {
    const session = this.requireActiveSession(sessionId);

    // Reset the flag so enqueueSessionStart will actually enqueue (not skip).
    session.isSessionStartSent = false;

    // Reset prompt-level IDs for the fresh turn.
    session.promptName = randomUUID();
    session.audioContentId = randomUUID();
    session.isPromptStartSent = false;
    session.isAudioContentStartSent = false;

    // Pre-enqueue sessionStart so the queue is non-empty when send() starts.
    // The client will subsequently send promptStart → systemPrompt → audioStart
    // which enqueues the rest.
    this.streaming.enqueueSessionStart(sessionId);

    this.logger.debug("Next stream prepared — sessionStart pre-enqueued", { sessionId });
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