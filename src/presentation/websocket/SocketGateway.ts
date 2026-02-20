// src/presentation/websocket/SocketGateway.ts
import "reflect-metadata";
import { inject, injectable } from "tsyringe";
import type { Server, Socket } from "socket.io";

import { TOKENS } from "../../infrastructure/config/tokens";
import { SessionUseCase } from "../../application/use-cases/SessionUseCase";
import { AudioStreamUseCase } from "../../application/use-cases/AudioStreamUseCase";
import type { ILogger } from "../../infrastructure/logging/ILogger";
import type { AppConfig } from "../../infrastructure/config/AppConfig";

import {
  InitializeSessionRequestSchema,
  SystemPromptRequestSchema,
  AudioStartSchema,
} from "../../application/dots/SessionDtos";
import { DomainError } from "../../domain/errors";
import { resetStreamReady } from "../../domain/entities/Session";

enum SessionState {
  CLOSED = "closed",
  INITIALIZING = "initializing",
  READY = "ready",
  ACTIVE = "active",
}

interface SocketContext {
  state: SessionState;
  cleanupInProgress: boolean;
}

@injectable()
export class SocketGateway {
  private readonly contexts = new Map<string, SocketContext>();

  constructor(
    @inject(TOKENS.SessionUseCase)
    private readonly sessionUseCase: SessionUseCase,

    @inject(TOKENS.AudioStreamUseCase)
    private readonly audioStream: AudioStreamUseCase,

    @inject(TOKENS.AppConfig)
    private readonly config: AppConfig,

    @inject(TOKENS.Logger)
    private readonly logger: ILogger
  ) {}

  attach(io: Server): void {
    io.on("connection", (socket) => this.onConnection(socket));
  }

  private onConnection(socket: Socket): void {
    this.logger.info("Client connected", { socketId: socket.id });
    this.contexts.set(socket.id, {
      state: SessionState.CLOSED,
      cleanupInProgress: false,
    });

    // Heartbeat log (non-blocking)
    const heartbeat = setInterval(() => {
      this.logger.debug("Active connections", {
        count: (socket.nsp.sockets as Map<string, Socket>).size,
      });
    }, 60_000);

    this.registerHandlers(socket, heartbeat);
  }

  private registerHandlers(socket: Socket, heartbeat: NodeJS.Timeout): void {
    socket.on("initializeConnection", (callback) =>
      this.handleInitialize(socket, callback)
    );
    socket.on("startNewChat", () => this.handleStartNewChat(socket));
    socket.on("promptStart", () => this.handlePromptStart(socket));
    socket.on("systemPrompt", (data: unknown) =>
      this.handleSystemPrompt(socket, data)
    );
    socket.on("audioStart", (data: unknown) =>
      this.handleAudioStart(socket, data)
    );
    socket.on("userText", (data: unknown) =>
      this.handleUserText(socket, data)
    );
    socket.on("audioInput", (data: unknown) =>
      this.handleAudioInput(socket, data)
    );
    socket.on("stopAudio", () => this.handleStopAudio(socket));
    socket.on("disconnect", () => this.handleDisconnect(socket, heartbeat));
  }

  // ── Handlers ─────────────────────────────────────────────────────────────

  private async handleInitialize(
    socket: Socket,
    callback?: (result: unknown) => void
  ): Promise<void> {
    const ctx = this.getContext(socket.id);
    this.logger.info("initializeConnection", {
      socketId: socket.id,
      state: ctx.state,
    });

    if (
      ctx.state === SessionState.INITIALIZING ||
      ctx.state === SessionState.READY ||
      ctx.state === SessionState.ACTIVE
    ) {
      callback?.({ success: true });
      return;
    }

    try {
      ctx.state = SessionState.INITIALIZING;

      const request = InitializeSessionRequestSchema.parse({
        sessionId: socket.id,
      });

      const session = this.sessionUseCase.createSession(request);
      this.audioStream.initQueue(session.sessionId);
      this.setupSessionEventHandlers(session.sessionId, socket);

      // Fire bidirectional stream – do NOT await
      this.sessionUseCase.startStream(session.sessionId);

      ctx.state = SessionState.ACTIVE;
      callback?.({ success: true });
    } catch (err) {
      ctx.state = SessionState.CLOSED;
      this.logger.error("Failed to initialize session", {
        socketId: socket.id,
        err,
      });
      callback?.({
        success: false,
        error: this.formatError(err),
      });
      socket.emit("error", {
        message: "Failed to initialize session",
        details: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async handleStartNewChat(socket: Socket): Promise<void> {
    const ctx = this.getContext(socket.id);
    this.logger.info("startNewChat", {
      socketId: socket.id,
      state: ctx.state,
    });

    try {
      // Tear down existing session if present
      if (this.sessionUseCase.isSessionActive(socket.id)) {
        await this.gracefulClose(socket.id);
      }

      const request = InitializeSessionRequestSchema.parse({
        sessionId: socket.id,
      });

      const session = this.sessionUseCase.createSession(request);
      this.audioStream.initQueue(session.sessionId);
      this.setupSessionEventHandlers(session.sessionId, socket);
      this.sessionUseCase.startStream(session.sessionId);
      ctx.state = SessionState.ACTIVE;
    } catch (err) {
      ctx.state = SessionState.CLOSED;
      this.logger.error("Failed to start new chat", {
        socketId: socket.id,
        err,
      });
      socket.emit("error", {
        message: "Failed to start new chat",
        details: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async handlePromptStart(socket: Socket): Promise<void> {
    try {
      this.requireState(socket.id, [SessionState.ACTIVE, SessionState.READY]);
      this.sessionUseCase.startPrompt(socket.id);
    } catch (err) {
      this.logger.error("promptStart error", { socketId: socket.id, err });
      socket.emit("error", this.formatError(err));
    }
  }

  private async handleSystemPrompt(
    socket: Socket,
    rawData: unknown
  ): Promise<void> {
    try {
      this.requireState(socket.id, [SessionState.ACTIVE]);
      const parsed = SystemPromptRequestSchema.parse(
        typeof rawData === "string" ? { content: rawData } : rawData
      );
      this.sessionUseCase.setupSystemPrompt(socket.id, parsed);
    } catch (err) {
      this.logger.error("systemPrompt error", { socketId: socket.id, err });
      socket.emit("error", this.formatError(err));
    }
  }

  private async handleAudioStart(
    socket: Socket,
    rawData: unknown
  ): Promise<void> {
    try {
      this.requireState(socket.id, [SessionState.ACTIVE]);
      const parsed = AudioStartSchema.safeParse(rawData ?? {});
      this.sessionUseCase.setupAudioStart(
        socket.id,
        parsed.success ? parsed.data : {}
      );

      // Wait for the Bedrock stream to be established before telling the client
      // to proceed. Without this, fast clients (e.g. Flutter's auto-connect flow)
      // can send all events before the stream is ready, causing 0 output tokens.
      const session = this.sessionUseCase.getSession(socket.id);
      await session.streamReady;

      socket.emit("audioReady");
    } catch (err) {
      this.logger.error("audioStart error", { socketId: socket.id, err });
      socket.emit("error", this.formatError(err));
    }
  }

  private handleUserText(socket: Socket, rawData: unknown): void {
    try {
      this.requireState(socket.id, [SessionState.ACTIVE]);
      const content =
        typeof rawData === "string"
          ? rawData
          : (rawData as { content?: string })?.content ?? "";
      if (!content) return;
      this.sessionUseCase.sendUserText(socket.id, content);
    } catch (err) {
      this.logger.error("userText error", { socketId: socket.id, err });
      socket.emit("error", this.formatError(err));
    }
  }

  private handleAudioInput(socket: Socket, rawData: unknown): void {
    try {
      const ctx = this.getContext(socket.id);
      if (ctx.state !== SessionState.ACTIVE) {
        socket.emit("error", {
          message: "Session must be ACTIVE to receive audio",
          code: "SESSION_NOT_ACTIVE",
        });
        return;
      }

      const audioBuffer =
        typeof rawData === "string"
          ? Buffer.from(rawData, "base64")
          : Buffer.from(rawData as ArrayBuffer);

      this.audioStream.enqueueAudio(socket.id, audioBuffer);
    } catch (err) {
      this.logger.error("audioInput error", { socketId: socket.id, err });
      socket.emit("error", this.formatError(err));
    }
  }

  private async handleStopAudio(socket: Socket): Promise<void> {
    const ctx = this.getContext(socket.id);

    if (!this.sessionUseCase.isSessionActive(socket.id) || ctx.cleanupInProgress) {
      this.logger.debug("stopAudio: no active session or cleanup in progress", {
        socketId: socket.id,
      });
      return;
    }

    // Stop accepting new audio input, but do NOT close the session.
    // The Bedrock response stream is still running and will send audio back.
    this.audioStream.destroyQueue(socket.id);

    try {
      await this.sessionUseCase.endAudioContent(socket.id);
      await this.sessionUseCase.endPrompt(socket.id);
      this.logger.info("stopAudio: input ended, waiting for AI response", {
        socketId: socket.id,
      });
    } catch (err) {
      this.logger.error("stopAudio error", { socketId: socket.id, err });
      socket.emit("error", this.formatError(err));
    }
  }

  private async handleDisconnect(
    socket: Socket,
    heartbeat: NodeJS.Timeout
  ): Promise<void> {
    clearInterval(heartbeat);
    this.logger.info("Client disconnected", { socketId: socket.id });

    const ctx = this.getContext(socket.id);

    if (
      this.sessionUseCase.isSessionActive(socket.id) &&
      !ctx.cleanupInProgress
    ) {
      ctx.cleanupInProgress = true;
      this.audioStream.destroyQueue(socket.id);

      try {
        await this.withTimeout(
          this.gracefulClose(socket.id),
          this.config.session.cleanupTimeoutMs - 2_000,
          "Disconnect cleanup timeout"
        );
        this.logger.info("Session cleaned up after disconnect", {
          socketId: socket.id,
        });
      } catch (err) {
        this.logger.error("Disconnect cleanup error", {
          socketId: socket.id,
          err,
        });
        this.sessionUseCase.forceCloseSession(socket.id);
      }
    }

    this.contexts.delete(socket.id);
  }

  // ── Session event forwarding ──────────────────────────────────────────────

  private setupSessionEventHandlers(sessionId: string, socket: Socket): void {
    const forward = (event: string) => {
      this.sessionUseCase.registerEventHandler(sessionId, event, (data) => {
        socket.emit(event, data);
      });
    };

    [
      "usageEvent",
      "completionStart",
      "contentStart",
      "textOutput",
      "audioOutput",
      "toolUse",
      "toolResult",
      "contentEnd",
      "error",
    ].forEach(forward);

    this.sessionUseCase.registerEventHandler(sessionId, "streamComplete", () => {
      socket.emit("streamComplete");
      this.logger.info("Turn complete, restarting stream for next turn", {
        socketId: socket.id,
      });

      // Re-initialize audio queue for the next turn's mic input.
      this.audioStream.initQueue(sessionId);

      // Reset streamReady for the next turn so audioReady waits for the new stream.
      const session = this.sessionUseCase.getSession(sessionId);
      resetStreamReady(session);

      // The Bedrock bidirectional stream closes after every promptEnd/response
      // cycle. Start a new stream that will drain whatever is already in the
      // queue (or wait for the next signal).
      this.sessionUseCase.startStream(sessionId);

      socket.emit("turnComplete");
    });
  }

  // ── Shared utilities ──────────────────────────────────────────────────────

  private async gracefulClose(sessionId: string): Promise<void> {
    await this.sessionUseCase.endAudioContent(sessionId);
    await this.sessionUseCase.endPrompt(sessionId);
    await this.sessionUseCase.closeSession(sessionId);
  }

  private getContext(socketId: string): SocketContext {
    let ctx = this.contexts.get(socketId);
    if (!ctx) {
      ctx = { state: SessionState.CLOSED, cleanupInProgress: false };
      this.contexts.set(socketId, ctx);
    }
    return ctx;
  }

  private requireState(socketId: string, allowed: SessionState[]): void {
    const ctx = this.getContext(socketId);
    if (!allowed.includes(ctx.state)) {
      throw new Error(
        `Invalid session state: expected ${allowed.join("|")}, got ${ctx.state}`
      );
    }
  }

  private formatError(err: unknown): { message: string; code?: string } {
    if (err instanceof DomainError) {
      return { message: err.message, code: err.code };
    }
    return {
      message: err instanceof Error ? err.message : String(err),
    };
  }

  private withTimeout<T>(
    promise: Promise<T>,
    ms: number,
    label: string
  ): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) =>
        setTimeout(() => reject(new Error(label)), ms)
      ),
    ]);
  }
}