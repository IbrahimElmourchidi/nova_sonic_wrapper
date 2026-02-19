import 'package:flutter/foundation.dart';

import '../repositories/i_chat_repository.dart';

class SendAudioUseCase {
  final IChatRepository _repository;

  SendAudioUseCase(this._repository);

  /// Called for every turn after the first.
  /// After streamComplete the backend sets socket state to CLOSED, so
  /// initializeSession (→ initializeConnection ack) creates a fresh session.
  /// Returns false if initialization fails so the caller can abort early.
  Future<bool> startRecordingSession(String systemPrompt) async {
    debugPrint('[SendAudioUseCase] startRecordingSession → initializeSession');
    final ok = await _repository.initializeSession();
    if (!ok) {
      debugPrint('[SendAudioUseCase] ❌ initializeSession failed');
      return false;
    }
    debugPrint('[SendAudioUseCase] → promptStart');
    await _repository.sendPromptStart();
    await Future.delayed(const Duration(milliseconds: 100));
    debugPrint('[SendAudioUseCase] → systemPrompt');
    await _repository.sendSystemPrompt(systemPrompt);
    await Future.delayed(const Duration(milliseconds: 100));
    debugPrint('[SendAudioUseCase] → audioStart');
    await _repository.sendAudioStart();
    return true;
  }

  /// Starts a new prompt within the existing session (no re-initialization).
  Future<bool> startNextTurn(String systemPrompt) async {
    debugPrint('[SendAudioUseCase] startNextTurn → promptStart');
    await _repository.sendPromptStart();
    await Future.delayed(const Duration(milliseconds: 100));
    debugPrint('[SendAudioUseCase] → systemPrompt');
    await _repository.sendSystemPrompt(systemPrompt);
    await Future.delayed(const Duration(milliseconds: 100));
    debugPrint('[SendAudioUseCase] → audioStart');
    await _repository.sendAudioStart();
    return true;
  }

  Future<void> sendChunk(String base64Pcm) async {
    await _repository.sendAudioChunk(base64Pcm);
  }

  Future<void> stopRecording() async {
    debugPrint('[SendAudioUseCase] stopRecording → stopAudio');
    await _repository.sendStopAudio();
  }
}
