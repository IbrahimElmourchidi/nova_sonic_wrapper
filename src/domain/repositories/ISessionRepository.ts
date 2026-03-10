// src/domain/repositories/ISessionRepository.ts
import type { SessionData, SessionId } from "../entities/Session";
import type { InferenceConfig } from "../types";

export interface ISessionRepository {
  /**
   * Create and persist a new session.
   */
  create(sessionId: SessionId, inferenceConfig: InferenceConfig): SessionData;

  /**
   * Retrieve a session by ID. Returns undefined if not found.
   */
  findById(sessionId: SessionId): SessionData | undefined;

  /**
   * Check if a session exists.
   */
  has(sessionId: SessionId): boolean;

  /**
   * Remove a session permanently.
   */
  delete(sessionId: SessionId): void;

  /**
   * List all active session IDs.
   */
  getAllIds(): SessionId[];

  /**
   * Update the last activity timestamp of a session.
   */
  updateActivity(sessionId: SessionId): void;

  /**
   * Get all sessions with their last activity time.
   */
  getIdleSessionIds(idleThresholdMs: number): SessionId[];
}
