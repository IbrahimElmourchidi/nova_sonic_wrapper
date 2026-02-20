import 'package:flutter/foundation.dart';

import '../repositories/i_chat_repository.dart';

class ConnectUseCase {
  final IChatRepository _repository;

  ConnectUseCase(this._repository);

  Future<bool> execute(String serverUrl) async {
    debugPrint('[ConnectUseCase] step 1 → connect($serverUrl)');
    final connected = await _repository.connect(serverUrl);
    debugPrint('[ConnectUseCase] step 1 result → connected=$connected');
    if (!connected) return false;

    debugPrint('[ConnectUseCase] step 2 → initializeSession()');
    final initialized = await _repository.initializeSession();
    debugPrint('[ConnectUseCase] step 2 result → initialized=$initialized');
    if (!initialized) return false;

    debugPrint('[ConnectUseCase] ✅ connected and session initialized');
    return true;
  }
}
