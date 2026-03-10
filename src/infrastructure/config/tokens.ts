// src/infrastructure/config/tokens.ts

export const TOKENS = {
  // Domain repositories
  SessionRepository: Symbol("ISessionRepository"),

  // Domain services
  StreamingService: Symbol("IStreamingService"),
  ToolService: Symbol("IToolService"),

  // Application use cases
  SessionUseCase: Symbol("SessionUseCase"),
  AudioStreamUseCase: Symbol("AudioStreamUseCase"),

  // Infrastructure
  BedrockClient: Symbol("BedrockClient"),
  AppConfig: Symbol("AppConfig"),
  Logger: Symbol("ILogger"),
  GreetingAudioService: Symbol("GreetingAudioService"),
} as const;