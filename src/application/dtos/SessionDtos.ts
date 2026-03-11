// src/application/dtos/SessionDtos.ts
import { z } from "zod";

// ── Nova Sonic supported voice IDs ───────────────────────────────────────────

export const NOVA_VOICE_IDS = ["tiffany", "matthew", "amy"] as const;
export type NovaVoiceId = (typeof NOVA_VOICE_IDS)[number];
export const DEFAULT_VOICE_ID: NovaVoiceId = "tiffany";

// ── Validation schemas ──────────────────────────────────────────────────────

export const InferenceConfigSchema = z.object({
  maxTokens: z.number().int().positive().max(8192).default(1024),
  topP: z.number().min(0).max(1).default(0.9),
  temperature: z.number().min(0).max(1).default(0.7),
});

export const AudioConfigSchema = z.object({
  audioType: z.literal("SPEECH").default("SPEECH"),
  encoding: z.string().default("base64"),
  mediaType: z.literal("audio/lpcm").default("audio/lpcm"),
  sampleRateHertz: z.number().int().positive().default(16000),
  sampleSizeBits: z.number().int().positive().default(16),
  channelCount: z.number().int().min(1).max(8).default(1),
  voiceId: z.string().optional(),
});

export const TextConfigSchema = z.object({
  mediaType: z.enum(["text/plain", "application/json"]).default("text/plain"),
});

export const InitializeSessionRequestSchema = z.object({
  sessionId: z.string().optional(),
  inferenceConfig: InferenceConfigSchema.optional(),
  // ── German tutor session metadata ──────────────────────────────────────
  /** Healthcare topic for this lesson, e.g. "General Health", "Medication". */
  topic: z.string().min(1).max(200).optional(),
  /** Nova Sonic voice to use for AI audio output. */
  voiceId: z.enum(NOVA_VOICE_IDS).optional(),
});

export const SystemPromptRequestSchema = z.object({
  content: z.string().min(1).max(10000),
  textConfig: TextConfigSchema.optional(),
});

export const AudioInputSchema = z.object({
  data: z.union([z.string(), z.instanceof(Buffer)]),
});

export const AudioStartSchema = z.object({
  audioConfig: AudioConfigSchema.optional(),
});

// ── Inferred types ──────────────────────────────────────────────────────────

export type InferenceConfigDto    = z.infer<typeof InferenceConfigSchema>;
export type AudioConfigDto        = z.infer<typeof AudioConfigSchema>;
export type TextConfigDto         = z.infer<typeof TextConfigSchema>;
export type InitializeSessionRequest = z.infer<typeof InitializeSessionRequestSchema>;
export type SystemPromptRequest   = z.infer<typeof SystemPromptRequestSchema>;
export type AudioInputDto         = z.infer<typeof AudioInputSchema>;
export type AudioStartDto         = z.infer<typeof AudioStartSchema>;

// ── Response shapes ─────────────────────────────────────────────────────────

export interface SessionCreatedResponse {
  sessionId: string;
  success: true;
}

export interface ErrorResponse {
  success: false;
  error: { code: string; message: string };
}

export interface HealthCheckResponse {
  status: "ok" | "degraded";
  timestamp: string;
  activeSessions: number;
  socketConnections: number;
}