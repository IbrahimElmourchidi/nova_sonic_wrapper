// src/infrastructure/config/container.ts
import "reflect-metadata";
import { container } from "tsyringe";

import { TOKENS } from "./tokens";
import { loadConfig, type AppConfig } from "./AppConfig";
import { WinstonLogger } from "../logging/WinstonLogger";
import { InMemorySessionRepository } from "../repositories/InMemorySessionRepository";
import { BedrockStreamingService } from "../bedrock/BedrockStreamingService";
import { ToolService } from "../tools/ToolService";
import { GreetingAudioService } from "../audio/GreetingAudioService";
import { SessionUseCase } from "../../application/use-cases/SessionUseCase";
import { AudioStreamUseCase } from "../../application/use-cases/AudioStreamUseCase";
import type { ILogger } from "../logging/ILogger";
import type { ISessionRepository } from "../../domain/repositories/ISessionRepository";
import type { IStreamingService } from "../../domain/services/IStreamingService";
import type { IToolService } from "../../domain/services/IToolService";

export function buildContainer(env: NodeJS.ProcessEnv = process.env) {
  const config = loadConfig(env);

  // ── Singletons ────────────────────────────────────────────────────────────

  // Config
  container.registerInstance<AppConfig>(TOKENS.AppConfig, config);

  // Logger
  const logger = new WinstonLogger(
    config.logging.level,
    config.server.isProduction
  );
  container.registerInstance<ILogger>(TOKENS.Logger, logger);

  // Session repository
  container.registerSingleton<ISessionRepository>(
    TOKENS.SessionRepository,
    InMemorySessionRepository
  );

  // Tool service
  container.registerSingleton<IToolService>(TOKENS.ToolService, ToolService);

  // Streaming service (depends on repository, tool service, config, logger)
  container.registerSingleton<IStreamingService>(
    TOKENS.StreamingService,
    BedrockStreamingService
  );

  // Greeting audio service — converts MP3 once at startup, caches LPCM buffer
  container.registerSingleton<GreetingAudioService>(
    TOKENS.GreetingAudioService,
    GreetingAudioService
  );

  // Application use cases
  container.registerSingleton<SessionUseCase>(
    TOKENS.SessionUseCase,
    SessionUseCase
  );

  container.registerSingleton<AudioStreamUseCase>(
    TOKENS.AudioStreamUseCase,
    AudioStreamUseCase
  );

  return container;
}
