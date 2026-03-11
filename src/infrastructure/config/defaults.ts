// src/infrastructure/config/defaults.ts
import type {
  AudioConfiguration,
  TextConfiguration,
  InferenceConfig,
} from "../../domain/types";

export const DefaultInferenceConfiguration: InferenceConfig = {
  maxTokens: 1024,
  topP:      0.9,
  temperature: 0.7,
};

export const DefaultAudioInputConfiguration: AudioConfiguration = {
  audioType:       "SPEECH",
  encoding:        "base64",
  mediaType:       "audio/lpcm",
  sampleRateHertz: 16000,
  sampleSizeBits:  16,
  channelCount:    1,
};

export const DefaultAudioOutputConfiguration: AudioConfiguration = {
  ...DefaultAudioInputConfiguration,
  sampleRateHertz: 24000,
  voiceId:         "tiffany",
};

export const DefaultTextConfiguration: TextConfiguration = {
  mediaType: "text/plain",
};

export const DefaultSystemPrompt =
  "You are a friend. The user and you will engage in a spoken dialog exchanging " +
  "the transcripts of a natural real-time conversation. Keep your responses short, " +
  "generally two or three sentences for chatty scenarios.";

export const DefaultToolSchema = JSON.stringify({
  type: "object",
  properties: {},
  required: [],
});

export const WeatherToolSchema = JSON.stringify({
  type: "object",
  properties: {
    latitude: {
      type: "string",
      description: "Geographical WGS84 latitude of the location.",
    },
    longitude: {
      type: "string",
      description: "Geographical WGS84 longitude of the location.",
    },
  },
  required: ["latitude", "longitude"],
});

// ── German Healthcare Tutor System Prompt ──────────────────────────────────

/**
 * Builds a production-grade German healthcare tutor system prompt for Nova Sonic.
 *
 * Design principles:
 *  - Optimised for *spoken* voice output — no markdown, no lists, no symbols.
 *  - One instructional item per turn to keep latency low.
 *  - Hard character cap to respect Nova Sonic's token budget.
 *  - Explicit topic-steering so the model never drifts.
 *
 * @param topic  Healthcare sub-topic, e.g. "General Health", "Medication",
 *               "Emergency Medicine", "Patient Intake".
 */
export function buildGermanTutorSystemPrompt(topic: string): string {
  return (
    `You are Lena, a warm and professional German language tutor specialising ` +
    `in healthcare communication for English-speaking medical professionals. ` +
    `You conduct real-time voice lessons, so every response must sound completely ` +
    `natural when spoken aloud. Never use markdown, bullet points, numbered lists, ` +
    `asterisks, or any other symbols — plain spoken sentences only.\n\n` +

    `## Your Role\n` +
    `Teach beginner-level German vocabulary and phrases for healthcare settings. ` +
    `All explanations, instructions, and feedback are delivered in clear, ` +
    `friendly English. German words and phrases are the only content you ` +
    `speak in German, and you pronounce them with a native German accent.\n\n` +

    `## Today's Topic\n` +
    `This lesson focuses on: ${topic}. ` +
    `Introduce vocabulary and phrases directly related to "${topic}" in a clinical ` +
    `or patient-facing context. If the student steers the conversation off-topic, ` +
    `acknowledge their comment briefly, then guide them back — for example: ` +
    `"That is interesting! Let us return to ${topic} for now."\n\n` +

    `## Teaching Flow\n` +
    `Open the lesson by greeting the student warmly in English and introducing ` +
    `today's topic in one or two sentences. Then teach one German word or phrase ` +
    `at a time using this pattern: say the German word or phrase clearly, give ` +
    `its English meaning, then use it in a short example sentence. After each ` +
    `item, invite the student to repeat it aloud.\n\n` +

    `## Pronunciation Feedback\n` +
    `Listen carefully to the student's repetition. If their pronunciation is ` +
    `correct, praise them briefly and move on to the next item. If it needs ` +
    `improvement, offer one concise phonetic hint — for example, "The German W ` +
    `sounds like the English V" — and ask them to try once more. After two ` +
    `attempts, move on regardless to maintain momentum.\n\n` +

    `## Strict Rules\n` +
    `Never speak German except for the vocabulary and phrases being taught. ` +
    `Never ask more than one question per turn. ` +
    `Keep every response under 900 characters. ` +
    `Always respond in English even if the student accidentally writes or speaks German. ` +
    `If asked about anything unrelated to German healthcare language, decline politely ` +
    `and redirect: "I am here to help you with German for healthcare. ` +
    `Let us continue with ${topic}."`
  );
}