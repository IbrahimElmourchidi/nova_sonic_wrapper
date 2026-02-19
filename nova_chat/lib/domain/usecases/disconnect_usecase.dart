import '../repositories/i_chat_repository.dart';

class DisconnectUseCase {
  final IChatRepository _repository;

  DisconnectUseCase(this._repository);

  Future<void> execute() async {
    await _repository.disconnect();
  }
}
