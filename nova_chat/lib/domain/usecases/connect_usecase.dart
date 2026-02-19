import 'package:flutter/foundation.dart';

import '../repositories/i_chat_repository.dart';

class ConnectUseCase {
  final IChatRepository _repository;

  ConnectUseCase(this._repository);

  Future<bool> execute(String serverUrl, String systemPrompt) async {
    debugPrint('[ConnectUseCase] step 1 → connect($serverUrl)');
    final connected = await _repository.connect(serverUrl);
    debugPrint('[ConnectUseCase] step 1 result → connected=$connected');
    if (!connected) return false;

    debugPrint('[ConnectUseCase] step 2 → initializeSession()');
    final initialized = await _repository.initializeSession();
    debugPrint('[ConnectUseCase] step 2 result → initialized=$initialized');
    if (!initialized) return false;

    await Future.delayed(const Duration(milliseconds: 100));
    debugPrint('[ConnectUseCase] step 3 → sendPromptStart()');
    await _repository.sendPromptStart();

    await Future.delayed(const Duration(milliseconds: 100));
    debugPrint('[ConnectUseCase] step 4 → sendSystemPrompt()');
    await _repository.sendSystemPrompt(systemPrompt);

    // No audioStart here — the greeting uses a text user turn (userText), not audio.
    // stopAudio will skip endAudioContent (isAudioContentStartSent=false) and just
    // send promptEnd + sessionEnd, which is correct for a text-only turn.
    debugPrint('[ConnectUseCase] ✅ session ready for greeting');
    return true;
  }
}
