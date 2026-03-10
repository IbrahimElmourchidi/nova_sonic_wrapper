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
   * Enqueue a user text turn (role: USER, type: TEXT).
   */
  enqueueUserText(sessionId: SessionId, content: string): void;

  /**
   * Enqueue a raw audio chunk.
   */
  enqueueAudioChunk(sessionId: SessionId, audioData: Buffer): void;

  /**
   * Streams the pre-recorded LPCM greeting into the LIVE bidirectional stream,
   * delivering one 100ms chunk every 100ms so Nova Sonic's VAD/response engine
   * receives audio at real microphone cadence and generates a spoken response.
   *
   * WHY async with real delays (not just chunked queue items):
   *   The async iterable drains the queue as fast as next() is called —
   *   16 chunks disappear in ~25ms regardless of chunk size.  Nova Sonic
   *   receives the full 1.6s burst instantly, its VAD never fires, and
   *   outputTokens remains 0.  Awaiting 100ms between each enqueue() call
   *   forces the queue to empty between chunks, so Nova Sonic receives audio
   *   at true microphone cadence over the actual audio duration.
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