// src/infrastructure/config/AppConfig.ts
import { z } from "zod";

const EnvSchema = z.object({
  // Server
  PORT: z.coerce.number().int().positive().default(3000),
  NODE_ENV: z.enum(["development", "production", "test"]).default("development"),

  // AWS
  AWS_REGION: z.string().default("us-east-1"),
  AWS_PROFILE: z.string().optional(),

  // Bedrock
  BEDROCK_MODEL_ID: z.string().default("amazon.nova-sonic-v1:0"),
  BEDROCK_MAX_CONCURRENT_STREAMS: z.coerce.number().int().positive().default(20),
  BEDROCK_REQUEST_TIMEOUT_MS: z.coerce.number().int().positive().default(300_000),
  BEDROCK_SESSION_TIMEOUT_MS: z.coerce.number().int().positive().default(300_000),

  // Session management
  SESSION_INACTIVITY_TIMEOUT_MS: z.coerce.number().int().positive().default(300_000),
  SESSION_CLEANUP_INTERVAL_MS: z.coerce.number().int().positive().default(60_000),
  SESSION_CLEANUP_TIMEOUT_MS: z.coerce.number().int().positive().default(5_000),
  AUDIO_QUEUE_MAX_SIZE: z.coerce.number().int().positive().default(200),
  AUDIO_QUEUE_BATCH_SIZE: z.coerce.number().int().positive().default(5),

  // Inference defaults
  INFERENCE_MAX_TOKENS: z.coerce.number().int().positive().default(1024),
  INFERENCE_TOP_P: z.coerce.number().min(0).max(1).default(0.9),
  INFERENCE_TEMPERATURE: z.coerce.number().min(0).max(1).default(0.7),

  // Logging
  LOG_LEVEL: z.enum(["error", "warn", "info", "debug"]).default("info"),
  LOG_SESSION_EVENTS: z.coerce.boolean().default(true),
});

export type Env = z.infer<typeof EnvSchema>;

export interface AppConfig {
  server: {
    port: number;
    nodeEnv: string;
    isDevelopment: boolean;
    isProduction: boolean;
  };
  aws: {
    region: string;
    profile?: string;
  };
  bedrock: {
    modelId: string;
    maxConcurrentStreams: number;
    requestTimeoutMs: number;
    sessionTimeoutMs: number;
  };
  session: {
    inactivityTimeoutMs: number;
    cleanupIntervalMs: number;
    cleanupTimeoutMs: number;
    audioQueueMaxSize: number;
    audioQueueBatchSize: number;
  };
  inference: {
    maxTokens: number;
    topP: number;
    temperature: number;
  };
  logging: {
    level: string;
    logSessionEvents: boolean;
  };
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const parsed = EnvSchema.parse(env);

  return {
    server: {
      port: parsed.PORT,
      nodeEnv: parsed.NODE_ENV,
      isDevelopment: parsed.NODE_ENV === "development",
      isProduction: parsed.NODE_ENV === "production",
    },
    aws: {
      region: parsed.AWS_REGION,
      profile: parsed.AWS_PROFILE,
    },
    bedrock: {
      modelId: parsed.BEDROCK_MODEL_ID,
      maxConcurrentStreams: parsed.BEDROCK_MAX_CONCURRENT_STREAMS,
      requestTimeoutMs: parsed.BEDROCK_REQUEST_TIMEOUT_MS,
      sessionTimeoutMs: parsed.BEDROCK_SESSION_TIMEOUT_MS,
    },
    session: {
      inactivityTimeoutMs: parsed.SESSION_INACTIVITY_TIMEOUT_MS,
      cleanupIntervalMs: parsed.SESSION_CLEANUP_INTERVAL_MS,
      cleanupTimeoutMs: parsed.SESSION_CLEANUP_TIMEOUT_MS,
      audioQueueMaxSize: parsed.AUDIO_QUEUE_MAX_SIZE,
      audioQueueBatchSize: parsed.AUDIO_QUEUE_BATCH_SIZE,
    },
    inference: {
      maxTokens: parsed.INFERENCE_MAX_TOKENS,
      topP: parsed.INFERENCE_TOP_P,
      temperature: parsed.INFERENCE_TEMPERATURE,
    },
    logging: {
      level: parsed.LOG_LEVEL,
      logSessionEvents: parsed.LOG_SESSION_EVENTS,
    },
  };
}
