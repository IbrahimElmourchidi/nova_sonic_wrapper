// src/domain/services/IStreamingService.ts
import type { SessionId } from "../entities/Session";
import type { AudioConfiguration, TextConfiguration } from "../types";

export interface IStreamingService {
  /**
   * Initiates the bidirectional stream with the model for a session.
   * Runs asynchronously and handles all incoming events.
   */
  initiateStream(sessionId: SessionId): Promise<void>;

  /**
   * Enqueue a session start event.
   */
  enqueueSessionStart(sessionId: SessionId): void;

  /**
   * Enqueue a prompt start event with tool configuration.
   */
  enqueuePromptStart(sessionId: SessionId): void;

  /**
   * Enqueue a system prompt event.
   */
  enqueueSystemPrompt(
    sessionId: SessionId,
    content: string,
    textConfig: TextConfiguration
  ): void;

  /**
   * Enqueue the audio content start event.
   */
  enqueueAudioContentStart(
    sessionId: SessionId,
    audioConfig: AudioConfiguration
  ): void;

  /**
   * Enqueue a user text turn (role: USER, type: TEXT, interactive: false).
   */
  enqueueUserText(sessionId: SessionId, content: string): void;

  /**
   * Enqueue a raw audio chunk.
   */
  enqueueAudioChunk(sessionId: SessionId, audioData: Buffer): void;

  /**
   * Enqueues a short interactive TEXT user turn (role: USER, interactive: true)
   * that reliably triggers Nova Sonic to generate a spoken response.
   *
   * MUST only be called after at least one audio chunk has already been
   * enqueued in the same prompt, because Nova Sonic requires every prompt
   * to contain audio. This method satisfies the response-generation side;
   * the audio turn satisfies the API constraint.
   *
   * Does NOT enqueue promptEnd — caller is responsible for that.
   */
  enqueueGreetingTrigger(sessionId: SessionId, triggerText?: string): void;

  /**
   * Sends a minimal silent audio chunk (to satisfy the Nova Sonic "must include
   * audio" API requirement) followed by an interactive TEXT turn that bypasses
   * VAD and guarantees a spoken response.
   *
   * Event sequence:
   *   contentStart (AUDIO, interactive:false) → audioInput (silent) → contentEnd (AUDIO)
   *   → contentStart (TEXT, interactive:true) → textInput → contentEnd (TEXT)
   *   → promptEnd
   *
   * MUST be awaited at the call site.
   * MUST be called ONLY after session.streamReady has resolved.
   */
  enqueueAudioGreeting(sessionId: SessionId, audioData: Buffer): Promise<void>;

  /**
   * Enqueue the content end event for audio.
   */
  enqueueContentEnd(sessionId: SessionId): Promise<void>;

  /**
   * Enqueue the prompt end event.
   */
  enqueuePromptEnd(sessionId: SessionId): Promise<void>;

  /**
   * Enqueue the session end event and flush the session event log.
   */
  enqueueSessionEnd(sessionId: SessionId): Promise<void>;
}
