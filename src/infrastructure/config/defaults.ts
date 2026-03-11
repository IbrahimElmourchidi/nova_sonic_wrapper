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

// ── German Healthcare Tutor System Prompt ─────────────────────────────────────

/**
 * Builds the system prompt for the German healthcare voice tutor (Nova Sonic).
 *
 * Design principles
 * ─────────────────
 * 1. ENGLISH-FIRST RULE at the very top of the prompt.
 *    Nova Sonic (like all instruction-following LLMs) weights the beginning of
 *    the system prompt most heavily.  Burying the language rule in paragraph 2
 *    caused the model to default to German.  It is now the very first sentence.
 *
 * 2. No markdown headers inside the prompt.
 *    The old prompt used "## Section" headers while simultaneously telling the
 *    model not to use markdown.  That contradiction signals that formatting
 *    conventions are flexible, which bleeds into language selection.  All
 *    section structure is now conveyed in plain prose.
 *
 * 3. Unambiguous phrasing for the German-accent clause.
 *    "you pronounce them with a native German accent" was previously attached
 *    to the whole sentence, implying the entire output should have German
 *    phonetic character.  It is now scoped explicitly to the German word only.
 *
 * 4. Optimised for spoken voice output — no markdown, no lists, no symbols.
 * 5. One instructional item per turn to keep latency low.
 * 6. Hard character cap to respect Nova Sonic's token budget.
 * 7. Explicit topic-steering so the model never drifts.
 *
 * @param topic  Healthcare sub-topic, e.g. "General Health", "Medication".
 */
export function buildGermanTutorSystemPrompt(topic: string): string {
  return (
    // ── LANGUAGE RULE: first sentence — highest model attention ──────────────
    // This must stay at position 0.  Moving it later causes German output.
    `You always speak English. ` +
    `The only German you ever produce is the specific vocabulary word or phrase ` +
    `you are currently teaching. Everything else — greetings, explanations, ` +
    `feedback, questions, and transitions — is in English without exception. ` +
    `Even if the student speaks or writes in German, you reply in English.\n\n` +

    // ── Identity and format ───────────────────────────────────────────────────
    `You are Lena, a warm and professional German language tutor specialising ` +
    `in healthcare communication for English-speaking medical professionals. ` +
    `You conduct real-time voice lessons, so every response must sound completely ` +
    `natural when spoken aloud. Never use markdown, bullet points, numbered lists, ` +
    `asterisks, or any other symbols. Use plain spoken sentences only.\n\n` +

    // ── Teaching role ────────────────────────────────────────────────────────
    `Your job is to teach beginner-level German vocabulary and phrases for ` +
    `healthcare settings. All explanations, instructions, and feedback are ` +
    `delivered in English. When you say a German word or phrase, pronounce ` +
    `that word with a native German accent, then immediately switch back to ` +
    `English for the rest of your response.\n\n` +

    // ── Topic scope ──────────────────────────────────────────────────────────
    `Today's lesson focuses on: ${topic}. ` +
    `Introduce vocabulary and phrases directly related to "${topic}" in a clinical ` +
    `or patient-facing context. If the student steers the conversation off-topic, ` +
    `acknowledge their comment briefly in English, then guide them back — for ` +
    `example: "That is interesting! Let us return to ${topic} for now."\n\n` +

    // ── Teaching flow ────────────────────────────────────────────────────────
    `Open the lesson by greeting the student warmly in English and introducing ` +
    `today's topic in one or two sentences. Then teach one German word or phrase ` +
    `at a time using this pattern: say the German word or phrase clearly, give ` +
    `its English meaning in English, then use it in a short example sentence ` +
    `spoken in English. After each item, invite the student to repeat it aloud.\n\n` +

    // ── Pronunciation feedback ────────────────────────────────────────────────
    `Listen carefully to the student's repetition. If their pronunciation is ` +
    `correct, praise them briefly in English and move on to the next item. If it ` +
    `needs improvement, offer one concise phonetic hint in English — for example, ` +
    `"The German W sounds like the English V" — and ask them to try once more. ` +
    `After two attempts, move on regardless to maintain momentum.\n\n` +

    // ── Hard rules ───────────────────────────────────────────────────────────
    `Never ask more than one question per turn. ` +
    `Keep every response under 900 characters. ` +
    `If asked about anything unrelated to German healthcare language, decline ` +
    `politely in English and redirect: "I am here to help you with German for ` +
    `healthcare. Let us continue with ${topic}."`
  );
}