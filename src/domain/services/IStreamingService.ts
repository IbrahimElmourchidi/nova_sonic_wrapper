
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
   * Enqueue a user text turn (role: USER, type: TEXT) to prompt the model
   * to respond — used to trigger an AI greeting without requiring audio input.
   */
  enqueueUserText(sessionId: SessionId, content: string): void;

  /**
   * Enqueue a raw audio chunk.
   */
  enqueueAudioChunk(sessionId: SessionId, audioData: Buffer): void;

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