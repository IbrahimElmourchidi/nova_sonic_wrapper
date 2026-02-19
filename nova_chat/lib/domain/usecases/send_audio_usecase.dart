import '../repositories/i_chat_repository.dart';

class SendAudioUseCase {
  final IChatRepository _repository;

  SendAudioUseCase(this._repository);

  Future<void> startRecordingSession(String systemPrompt) async {
    await _repository.initializeSession();
    await Future.delayed(const Duration(milliseconds: 100));
    await _repository.sendPromptStart();
    await Future.delayed(const Duration(milliseconds: 100));
    await _repository.sendSystemPrompt(systemPrompt);
    await Future.delayed(const Duration(milliseconds: 100));
    await _repository.sendAudioStart();
  }

  Future<void> sendChunk(String base64Pcm) async {
    await _repository.sendAudioChunk(base64Pcm);
  }

  Future<void> stopRecording() async {
    await _repository.sendStopAudio();
  }
}
