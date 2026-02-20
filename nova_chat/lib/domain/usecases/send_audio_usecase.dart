// File: lib/domain/usecases/send_audio_usecase.dart
import 'package:flutter/foundation.dart';

import '../repositories/i_chat_repository.dart';

class SendAudioUseCase {
  final IChatRepository _repository;

  SendAudioUseCase(this._repository);

  // ── FIRST TURN ONLY ───────────────────────────────────────────────────────
  //
  // Sends promptStart → systemPrompt → audioStart to open the Nova Sonic
  // bidirectional stream.  Call this exactly ONCE per connection, from
  // autoConnect().  After this the stream stays open for the entire
  // conversation — never call it again between turns.
  //
  Future<bool> startRecordingSession(String systemPrompt) async {
    debugPrint('[SendAudioUseCase] startRecordingSession → promptStart');
    await _repository.sendPromptStart();
    await Future.delayed(const Duration(milliseconds: 100));

    debugPrint('[SendAudioUseCase] → systemPrompt');
    await _repository.sendSystemPrompt(systemPrompt);
    await Future.delayed(const Duration(milliseconds: 100));

    debugPrint('[SendAudioUseCase] → audioStart');
    await _repository.sendAudioStart();
    return true;
  }

  // ── AUDIO CHUNKS ─────────────────────────────────────────────────────────
  //
  // Forward a base-64 PCM chunk to the server.  The stream must already be
  // open (startRecordingSession was called and audioReady was received).
  //
  Future<void> sendChunk(String base64Pcm) async {
    await _repository.sendAudioChunk(base64Pcm);
  }

  // ── END SESSION ───────────────────────────────────────────────────────────
  //
  // FIX: stopAudio is only emitted HERE — when the user truly ends the whole
  // conversation (e.g. navigates away).  It must NOT be sent between turns
  // because it tells the server to tear down the entire stream.
  //
  // Previously stopRecording() called sendStopAudio() after every VAD silence
  // event, which killed the session after the first AI response and made every
  // subsequent audioStart go unanswered (timeout waiting for audioReady).
  //
  Future<void> endSession() async {
    debugPrint('[SendAudioUseCase] endSession → stopAudio');
    await _repository.sendStopAudio();
  }
}
