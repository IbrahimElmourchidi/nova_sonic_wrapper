// src/domain/repositories/ISessionRepository.ts
import type { SessionData, SessionId } from "../entities/Session";
import type { InferenceConfig } from "../types";

export interface ISessionRepository {
  create(
    sessionId: SessionId,
    inferenceConfig: InferenceConfig,
    topic?: string,
    voiceId?: string
  ): SessionData;

  findById(sessionId: SessionId): SessionData | undefined;
  has(sessionId: SessionId): boolean;
  delete(sessionId: SessionId): void;
  getAllIds(): SessionId[];
  updateActivity(sessionId: SessionId): void;
  getIdleSessionIds(idleThresholdMs: number): SessionId[];
}