// src/domain/entities/Session.ts
import { randomUUID } from "node:crypto";
import { Subject } from "rxjs";
import type { InferenceConfig } from "../types";

export type SessionId = string;

export enum SessionStatus {
  INITIALIZING = "initializing",
  READY = "ready",
  ACTIVE = "active",
  CLOSING = "closing",
  CLOSED = "closed",
}

export interface AudioContentId extends String {
  readonly _brand: "AudioContentId";
}

export interface SessionData {
  readonly sessionId: SessionId;
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
  /** Rejects streamReady — called by the streaming service on fatal stream error. */
  rejectStreamReady: (err: unknown) => void;

  // ── Per-turn response tracking (FIX: helps diagnose zero-output issues) ──
  audioChunksSent: number;
  receivedAudioOutput: boolean;
  receivedTextOutput: boolean;
}

/**
 * Resets the streamReady promise for a new Bedrock stream (called between turns).
 * The new promise is both resolvable and rejectable so callers never hang.
 */
export function resetStreamReady(session: SessionData): void {
  let resolve!: () => void;
  let reject!: (err: unknown) => void;
  session.streamReady = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  session.resolveStreamReady = resolve;
  session.rejectStreamReady = reject;
}

export function createSessionData(
  sessionId: SessionId,
  inferenceConfig: InferenceConfig
): SessionData {
  let resolveStreamReady!: () => void;
  let rejectStreamReady!: (err: unknown) => void;
  const streamReady = new Promise<void>((resolve, reject) => {
    resolveStreamReady = resolve;
    rejectStreamReady = reject;
  });

  return {
    sessionId,
    promptName: randomUUID(),
    audioContentId: randomUUID(),
    inferenceConfig,
    queue: [],
    queueSignal: new Subject<void>(),
    closeSignal: new Subject<void>(),
    responseHandlers: new Map(),
    toolUseContent: null,
    toolUseId: "",
    toolName: "",
    isActive: true,
    isSessionStartSent: false,
    isPromptStartSent: false,
    isAudioContentStartSent: false,
    lastActivity: Date.now(),
    status: SessionStatus.INITIALIZING,
    streamReady,
    resolveStreamReady,
    rejectStreamReady,
    // Per-turn tracking
    audioChunksSent: 0,
    receivedAudioOutput: false,
    receivedTextOutput: false,
  };
}
