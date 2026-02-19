import '../../domain/entities/chat_message.dart';
import '../../domain/enums/connection_status.dart';
import '../../domain/enums/session_status.dart';
import '../../domain/repositories/i_chat_repository.dart';
import '../datasources/socket_data_source.dart';

class ChatRepositoryImpl implements IChatRepository {
  final SocketDataSource _dataSource;

  ChatRepositoryImpl(this._dataSource);

  @override
  Stream<ConnectionStatus> get connectionStatusStream =>
      _dataSource.connectionStatusStream;

  @override
  Stream<SessionStatus> get sessionStatusStream =>
      _dataSource.sessionStatusStream;

  @override
  Stream<ChatMessage> get messageStream => _dataSource.messageStream;

  @override
  Stream<List<int>> get audioOutputStream => _dataSource.audioOutputStream;

  @override
  Stream<String> get errorStream => _dataSource.errorStream;

  @override
  Future<bool> connect(String serverUrl) => _dataSource.connect(serverUrl);

  @override
  Future<bool> initializeSession() => _dataSource.initializeConnection();

  @override
  Future<void> sendPromptStart() async => _dataSource.emitPromptStart();

  @override
  Future<void> sendSystemPrompt(String content) async =>
      _dataSource.emitSystemPrompt(content);

  @override
  Future<void> sendAudioStart() async => _dataSource.emitAudioStart();

  @override
  Future<void> sendUserText(String content) async =>
      _dataSource.emitUserText(content);

  @override
  Future<void> sendAudioChunk(String base64Pcm) async =>
      _dataSource.emitAudioInput(base64Pcm);

  @override
  Future<void> sendStopAudio() async => _dataSource.emitStopAudio();

  @override
  Future<void> disconnect() async => _dataSource.disconnect();

  @override
  void dispose() => _dataSource.dispose();
}
