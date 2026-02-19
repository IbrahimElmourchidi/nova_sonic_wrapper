// src/domain/errors/index.ts

export class DomainError extends Error {
  constructor(
    message: string,
    public readonly code: string
  ) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class SessionNotFoundError extends DomainError {
  constructor(sessionId: string) {
    super(`Session '${sessionId}' not found`, "SESSION_NOT_FOUND");
  }
}

export class SessionAlreadyExistsError extends DomainError {
  constructor(sessionId: string) {
    super(
      `Session '${sessionId}' already exists`,
      "SESSION_ALREADY_EXISTS"
    );
  }
}

export class SessionInactiveError extends DomainError {
  constructor(sessionId: string) {
    super(`Session '${sessionId}' is not active`, "SESSION_INACTIVE");
  }
}

export class StreamingError extends DomainError {
  constructor(
    sessionId: string,
    public readonly originalError: unknown
  ) {
    super(
      `Streaming error in session '${sessionId}': ${originalError instanceof Error ? originalError.message : String(originalError)}`,
      "STREAMING_ERROR"
    );
  }
}

export class ToolNotSupportedError extends DomainError {
  constructor(toolName: string) {
    super(`Tool '${toolName}' is not supported`, "TOOL_NOT_SUPPORTED");
  }
}

export class ConfigurationError extends DomainError {
  constructor(message: string) {
    super(message, "CONFIGURATION_ERROR");
  }
}

export class AudioQueueFullError extends DomainError {
  constructor() {
    super("Audio buffer queue is full", "AUDIO_QUEUE_FULL");
  }
}