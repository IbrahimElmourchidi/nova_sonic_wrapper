// src/main.ts
import "dotenv/config";
import "reflect-metadata";
import { buildContainer } from "./infrastructure/config/container";
import { App } from "./presentation/http/App";
import { HealthRouter } from "./presentation/http/HealthRouter";
import { SocketGateway } from "./presentation/websocket/SocketGateway";
import { TOKENS } from "./infrastructure/config/tokens";
import { SessionUseCase } from "./application/use-cases/SessionUseCase";
import type { ILogger } from "./infrastructure/logging/ILogger";

async function bootstrap(): Promise<void> {
  // Build the DI container
  const ioc = buildContainer(process.env);

  // Manually resolve classes that need tsyringe injection
  const logger = ioc.resolve<ILogger>(TOKENS.Logger);
  const sessionUseCase = ioc.resolve<SessionUseCase>(TOKENS.SessionUseCase);
  const healthRouter = ioc.resolve(HealthRouter);
  const socketGateway = ioc.resolve(SocketGateway);
  const config = ioc.resolve(TOKENS.AppConfig);

  const app = new App(
    config as any,
    logger,
    sessionUseCase,
    healthRouter,
    socketGateway
  );

  app.start();

  // ── Graceful shutdown ───────────────────────────────────────────────────

  const forceExitMs = 10_000;

  const gracefulShutdown = async (signal: string): Promise<void> => {
    logger.info(`Received ${signal}, starting graceful shutdown`);

    const forceTimer = setTimeout(() => {
      logger.error("Forced shutdown after timeout");
      process.exit(1);
    }, forceExitMs);

    try {
      await app.shutdown();
      clearTimeout(forceTimer);
      process.exit(0);
    } catch (err) {
      logger.error("Error during shutdown", { err });
      process.exit(1);
    }
  };

  process.on("SIGINT", () => gracefulShutdown("SIGINT"));
  process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

  process.on("unhandledRejection", (reason) => {
    logger.error("Unhandled promise rejection", {
      reason: String(reason),
    });
  });

  process.on("uncaughtException", (err) => {
    logger.error("Uncaught exception", { err });
    process.exit(1);
  });
}

bootstrap().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});