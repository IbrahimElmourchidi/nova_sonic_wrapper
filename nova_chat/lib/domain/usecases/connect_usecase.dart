import '../repositories/i_chat_repository.dart';

class ConnectUseCase {
  final IChatRepository _repository;

  ConnectUseCase(this._repository);

  Future<bool> execute(String serverUrl, String systemPrompt) async {
    final connected = await _repository.connect(serverUrl);
    if (!connected) return false;

    final initialized = await _repository.initializeSession();
    if (!initialized) return false;

    await Future.delayed(const Duration(milliseconds: 100));
    await _repository.sendPromptStart();
    await Future.delayed(const Duration(milliseconds: 100));
    await _repository.sendSystemPrompt(systemPrompt);
    await Future.delayed(const Duration(milliseconds: 100));
    await _repository.sendAudioStart();

    return true;
  }
}
