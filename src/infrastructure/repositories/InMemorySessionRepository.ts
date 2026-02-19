// src/infrastructure/repositories/InMemorySessionRepository.ts
import "reflect-metadata";
import { injectable } from "tsyringe";

import type { ISessionRepository } from "../../domain/repositories/ISessionRepository";
import { createSessionData, type SessionData, type SessionId } from "../../domain/entities/Session";
import { SessionAlreadyExistsError } from "../../domain/errors";
import type { InferenceConfig } from "../../domain/types";

@injectable()
export class InMemorySessionRepository implements ISessionRepository {
  private readonly store = new Map<SessionId, SessionData>();

  create(sessionId: SessionId, inferenceConfig: InferenceConfig): SessionData {
    if (this.store.has(sessionId)) {
      throw new SessionAlreadyExistsError(sessionId);
    }
    const session = createSessionData(sessionId, inferenceConfig);
    this.store.set(sessionId, session);
    return session;
  }

  findById(sessionId: SessionId): SessionData | undefined {
    return this.store.get(sessionId);
  }

  has(sessionId: SessionId): boolean {
    return this.store.has(sessionId);
  }

  delete(sessionId: SessionId): void {
    this.store.delete(sessionId);
  }

  getAllIds(): SessionId[] {
    return Array.from(this.store.keys());
  }

  updateActivity(sessionId: SessionId): void {
    const session = this.store.get(sessionId);
    if (session) {
      session.lastActivity = Date.now();
    }
  }

  getIdleSessionIds(idleThresholdMs: number): SessionId[] {
    const now = Date.now();
    const idle: SessionId[] = [];
    for (const [id, session] of this.store) {
      if (now - session.lastActivity > idleThresholdMs) {
        idle.push(id);
      }
    }
    return idle;
  }
}