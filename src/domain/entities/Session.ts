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
}

/** Resets the streamReady promise for a new Bedrock stream (called between turns). */
export function resetStreamReady(session: SessionData): void {
  let resolve!: () => void;
  session.streamReady = new Promise<void>((r) => { resolve = r; });
  session.resolveStreamReady = resolve;
}

export function createSessionData(
  sessionId: SessionId,
  inferenceConfig: InferenceConfig
): SessionData {
  let resolveStreamReady!: () => void;
  const streamReady = new Promise<void>((resolve) => {
    resolveStreamReady = resolve;
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
  };
}