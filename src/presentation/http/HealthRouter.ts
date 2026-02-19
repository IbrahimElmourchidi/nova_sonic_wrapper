// src/presentation/http/HealthRouter.ts
import "reflect-metadata";
import { inject, injectable } from "tsyringe";
import { Router, type Request, type Response } from "express";
import type { Server as SocketServer } from "socket.io";

import { TOKENS } from "../../infrastructure/config/tokens";
import { SessionUseCase } from "../../application/use-cases/SessionUseCase";
import type { HealthCheckResponse } from "../../application/dots/SessionDtos";

@injectable()
export class HealthRouter {
  readonly router: Router;

  constructor(
    @inject(TOKENS.SessionUseCase)
    private readonly sessionUseCase: SessionUseCase
  ) {
    this.router = Router();
    this.registerRoutes();
  }

  private registerRoutes(): void {
    this.router.get("/health", (req: Request, res: Response) =>
      this.health(req, res)
    );
  }

  private health(_req: Request, res: Response): void {
    const activeSessions = this.sessionUseCase.getActiveSessions().length;

    const body: HealthCheckResponse = {
      status: "ok",
      timestamp: new Date().toISOString(),
      activeSessions,
      socketConnections: 0, // will be enriched by App
    };

    res.status(200).json(body);
  }
}