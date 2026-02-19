import '../entities/chat_message.dart';
import '../enums/connection_status.dart';
import '../enums/session_status.dart';

abstract class IChatRepository {
  Stream<ConnectionStatus> get connectionStatusStream;
  Stream<SessionStatus> get sessionStatusStream;
  Stream<ChatMessage> get messageStream;
  Stream<List<int>> get audioOutputStream;
  Stream<String> get errorStream;

  Future<bool> connect(String serverUrl);
  Future<bool> initializeSession();
  Future<void> sendPromptStart();
  Future<void> sendSystemPrompt(String content);
  Future<void> sendAudioStart();
  Future<void> sendUserText(String content);
  Future<void> sendAudioChunk(String base64Pcm);
  Future<void> sendStopAudio();
  Future<void> disconnect();

  void dispose();
}
