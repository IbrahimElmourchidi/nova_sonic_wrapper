// src/presentation/http/App.ts
import "reflect-metadata";
import { inject, injectable } from "tsyringe";
import express, { type Application } from "express";
import http from "http";
import path from "path";
import { Server as SocketServer } from "socket.io";

import { TOKENS } from "../../infrastructure/config/tokens";
import type { AppConfig } from "../../infrastructure/config/AppConfig";
import type { ILogger } from "../../infrastructure/logging/ILogger";
import { HealthRouter } from "./HealthRouter";
import { SocketGateway } from "../websocket/SocketGateway";
import { SessionUseCase } from "../../application/use-cases/SessionUseCase";

@injectable()
export class App {
  readonly express: Application;
  readonly httpServer: http.Server;
  readonly io: SocketServer;

  constructor(
    @inject(TOKENS.AppConfig)
    private readonly config: AppConfig,

    @inject(TOKENS.Logger)
    private readonly logger: ILogger,

    @inject(TOKENS.SessionUseCase)
    private readonly sessionUseCase: SessionUseCase,

    private readonly healthRouter: HealthRouter,
    private readonly socketGateway: SocketGateway
  ) {
    this.express = express();
    this.httpServer = http.createServer(this.express);
    this.io = new SocketServer(this.httpServer, {
      cors: {
        origin: '*',
        methods: ['GET', 'POST'],
      },
      pingTimeout: 60_000,
      pingInterval: 25_000,
      maxHttpBufferSize: 1e7,
    });

    this.setupMiddleware();
    this.setupRoutes();
    this.setupWebSocket();
    this.setupSessionCleanup();
  }

  private setupMiddleware(): void {
    this.express.use(express.json());
    this.express.use(express.urlencoded({ extended: false }));

    // Serve static files (e.g. simple test client)
    const publicDir = path.join(process.cwd(), "client");
    this.express.use(express.static(publicDir));
  }

  private setupRoutes(): void {
    // Mount health endpoint, pass io for socket count
    this.express.use("/", this.healthRouter.router);

    // Override health to include socket count
    this.express.get("/health", (_req, res) => {
      const activeSessions = this.sessionUseCase.getActiveSessions().length;
      const socketConnections = this.io.sockets.sockets.size;

      res.status(200).json({
        status: "ok",
        timestamp: new Date().toISOString(),
        activeSessions,
        socketConnections,
      });
    });
  }

  private setupWebSocket(): void {
    this.socketGateway.attach(this.io);
  }

  private setupSessionCleanup(): void {
    setInterval(() => {
      this.logger.debug("Running idle session cleanup");
      this.sessionUseCase.cleanupIdleSessions(
        this.config.session.inactivityTimeoutMs
      );
    }, this.config.session.cleanupIntervalMs);
  }

  start(): void {
    const { port } = this.config.server;

    this.httpServer.listen(port, () => {
      this.logger.info(`Server listening on port ${port}`, {
        nodeEnv: this.config.server.nodeEnv,
        port,
      });
      this.logger.info(`Test client: http://localhost:${port}/nova-sonic-client.html`);
    });
  }

  async shutdown(): Promise<void> {
    this.logger.info("Shutting down server...");

    // Close Socket.IO (stops accepting new connections)
    await new Promise<void>((resolve) => this.io.close(() => resolve()));
    this.logger.info("Socket.IO closed");

    // Close all active Bedrock sessions
    const activeSessions = this.sessionUseCase.getActiveSessions();
    this.logger.info(`Closing ${activeSessions.length} active sessions`);

    await Promise.allSettled(
      activeSessions.map(async (sessionId) => {
        try {
          await this.sessionUseCase.endAudioContent(sessionId);
          await this.sessionUseCase.endPrompt(sessionId);
          await this.sessionUseCase.closeSession(sessionId);
        } catch {
          this.sessionUseCase.forceCloseSession(sessionId);
        }
      })
    );

    // Close HTTP server
    await new Promise<void>((resolve, reject) =>
      this.httpServer.close((err) => (err ? reject(err) : resolve()))
    );

    this.logger.info("Server shut down cleanly");
  }
}
