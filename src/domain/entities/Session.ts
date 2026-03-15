// src/domain/entities/Session.ts
import { randomUUID } from "node:crypto";
import { Subject } from "rxjs";
import type { InferenceConfig } from "../types";

export type SessionId = string;

export enum SessionStatus {
  INITIALIZING = "initializing",
  READY        = "ready",
  ACTIVE       = "active",
  CLOSING      = "closing",
  CLOSED       = "closed",
}

export interface SessionData {
  readonly sessionId: SessionId;
  /** Stable Bedrock-side session UUID. Reused across HTTP/2 turns to preserve model memory. */
  readonly bedrockSessionId: string;
  /** Incremented each turn (starts at 1). Used to suppress re-greeting on Turn 2+. */
  turnNumber: number;
  promptName: string;
  audioContentId: string;
  readonly inferenceConfig: InferenceConfig;
  queue: Array<unknown>;
  readonly queueSignal: Subject<void>;
  readonly closeSignal: Subject<void>;
  responseHandlers: Map<string, (data: unknown) => void>;
  toolUseContent: unknown;
  toolUseId: string;
  toolName: string;
  isActive: boolean;
  isSessionStartSent: boolean;
  isPromptStartSent: boolean;
  isAudioContentStartSent: boolean;
  lastActivity: number;
  status: SessionStatus;
  streamReady: Promise<void>;
  resolveStreamReady: () => void;
  rejectStreamReady: (err: unknown) => void;

  // Per-turn response tracking
  audioChunksSent: number;
  receivedAudioOutput: boolean;
  receivedTextOutput: boolean;

  /** Incremented each time a new stream starts so stale iterators self-terminate. */
  streamGeneration: number;

  /**
   * Set to true by prepareNextStream() so that startPrompt() knows the
   * sessionStart + promptStart have already been enqueued for this turn
   * and should not be re-sent.
   */
  nextStreamPrepared: boolean;

  /** True once a system prompt has been enqueued for the current prompt turn. */
  isSystemPromptSent: boolean;

  /**
   * contentIds whose generationStage is "SPECULATIVE".
   * audioOutput events for these contentIds are suppressed — we only play
   * FINAL audio so the SPECULATIVE→FINAL transition gap is never heard.
   */
  speculativeContentIds: Set<string>;

  // ── German tutor metadata ─────────────────────────────────────────────────
  /** Healthcare sub-topic for the lesson (e.g. "General Health"). */
  topic: string;
  /** Nova Sonic voice ID used for audio output (e.g. "tiffany", "matthew"). */
  voiceId: string;
}

export function resetStreamReady(session: SessionData): void {
  let resolve!: () => void;
  let reject!:  (err: unknown) => void;
  session.streamReady        = new Promise<void>((res, rej) => { resolve = res; reject = rej; });
  session.resolveStreamReady = resolve;
  session.rejectStreamReady  = reject;
}

export function createSessionData(
  sessionId: SessionId,
  inferenceConfig: InferenceConfig,
  topic   = "General Health",
  voiceId = "tiffany"
): SessionData {
  let resolveStreamReady!: () => void;
  let rejectStreamReady!:  (err: unknown) => void;
  const streamReady = new Promise<void>((resolve, reject) => {
    resolveStreamReady = resolve;
    rejectStreamReady  = reject;
  });

  return {
    sessionId,
    bedrockSessionId:      randomUUID(),
    turnNumber:            1,
    promptName:            randomUUID(),
    audioContentId:        randomUUID(),
    inferenceConfig,
    queue:                 [],
    queueSignal:           new Subject<void>(),
    closeSignal:           new Subject<void>(),
    responseHandlers:      new Map(),
    toolUseContent:        null,
    toolUseId:             "",
    toolName:              "",
    isActive:              true,
    isSessionStartSent:    false,
    isPromptStartSent:     false,
    isAudioContentStartSent: false,
    lastActivity:          Date.now(),
    status:                SessionStatus.INITIALIZING,
    streamReady,
    resolveStreamReady,
    rejectStreamReady,
    audioChunksSent:       0,
    receivedAudioOutput:   false,
    receivedTextOutput:    false,
    streamGeneration:      0,
    nextStreamPrepared:    false,
    isSystemPromptSent:    false,
    speculativeContentIds: new Set(),
    topic,
    voiceId,
  };
}