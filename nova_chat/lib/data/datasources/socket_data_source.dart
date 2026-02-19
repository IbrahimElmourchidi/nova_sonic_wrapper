import 'dart:async';
import 'dart:convert';

import 'package:socket_io_client/socket_io_client.dart' as io;

import '../../domain/enums/connection_status.dart';
import '../../domain/enums/session_status.dart';
import '../models/chat_message_model.dart';

class SocketDataSource {
  io.Socket? _socket;

  final _connectionStatus = StreamController<ConnectionStatus>.broadcast();
  final _sessionStatus = StreamController<SessionStatus>.broadcast();
  final _messages = StreamController<ChatMessageModel>.broadcast();
  final _audioOutput = StreamController<List<int>>.broadcast();
  final _errors = StreamController<String>.broadcast();

  Stream<ConnectionStatus> get connectionStatusStream =>
      _connectionStatus.stream;
  Stream<SessionStatus> get sessionStatusStream => _sessionStatus.stream;
  Stream<ChatMessageModel> get messageStream => _messages.stream;
  Stream<List<int>> get audioOutputStream => _audioOutput.stream;
  Stream<String> get errorStream => _errors.stream;

  Future<bool> connect(String url) async {
    final completer = Completer<bool>();

    _socket = io.io(
      url,
      io.OptionBuilder()
          .setTransports(['websocket'])
          .disableAutoConnect()
          .build(),
    );

    _socket!.onConnect((_) {
      _connectionStatus.add(ConnectionStatus.connected);
      if (!completer.isCompleted) completer.complete(true);
    });

    _socket!.onConnectError((err) {
      _connectionStatus.add(ConnectionStatus.error);
      _errors.add('Connection error: $err');
      if (!completer.isCompleted) completer.complete(false);
    });

    _socket!.onDisconnect((_) {
      _connectionStatus.add(ConnectionStatus.disconnected);
    });

    _registerServerEvents();

    _connectionStatus.add(ConnectionStatus.connecting);
    _socket!.connect();

    return completer.future.timeout(
      const Duration(seconds: 10),
      onTimeout: () {
        _errors.add('Connection timeout');
        return false;
      },
    );
  }

  Future<bool> initializeConnection() async {
    final completer = Completer<bool>();
    _sessionStatus.add(SessionStatus.initializing);

    _socket?.emitWithAck('initializeConnection', [], ack: (dynamic response) {
      if (response is Map && response['success'] == true) {
        completer.complete(true);
      } else {
        _sessionStatus.add(SessionStatus.error);
        _errors.add(response?.toString() ?? 'Init failed');
        completer.complete(false);
      }
    });

    return completer.future.timeout(
      const Duration(seconds: 10),
      onTimeout: () => false,
    );
  }

  void emitPromptStart() => _socket?.emit('promptStart');

  void emitSystemPrompt(String content) =>
      _socket?.emit('systemPrompt', {'content': content});

  void emitAudioStart() => _socket?.emit('audioStart', {});

  void emitAudioInput(String base64Pcm) =>
      _socket?.emit('audioInput', base64Pcm);

  void emitStopAudio() => _socket?.emit('stopAudio');

  void _registerServerEvents() {
    _socket?.on('audioReady', (_) {
      _sessionStatus.add(SessionStatus.ready);
    });

    _socket?.on('contentStart', (data) {
      if (data is Map<String, dynamic>) {
        final type = data['type']?.toString().toUpperCase();
        final role = data['role']?.toString().toUpperCase();
        if (type == 'AUDIO' && role == 'ASSISTANT') {
          _sessionStatus.add(SessionStatus.aiSpeaking);
        }
      }
    });

    _socket?.on('textOutput', (data) {
      if (data is Map<String, dynamic>) {
        final content = data['content'] as String?;
        if (content != null && content.isNotEmpty) {
          _messages.add(ChatMessageModel.fromTextOutput(data));
        }
      }
    });

    _socket?.on('audioOutput', (data) {
      if (data is Map<String, dynamic>) {
        final content = data['content'] as String?;
        if (content != null) {
          final bytes = base64Decode(content);
          _audioOutput.add(bytes);
        }
      }
    });

    _socket?.on('contentEnd', (_) {});

    _socket?.on('streamComplete', (_) {
      _sessionStatus.add(SessionStatus.idle);
    });

    _socket?.on('sessionClosed', (_) {
      _sessionStatus.add(SessionStatus.idle);
    });

    _socket?.on('error', (data) {
      final message =
          data is Map ? data['message']?.toString() : data?.toString();
      _errors.add(message ?? 'Unknown error');
      _sessionStatus.add(SessionStatus.error);
    });
  }

  void disconnect() {
    _socket?.disconnect();
    _socket?.dispose();
    _socket = null;
  }

  void dispose() {
    disconnect();
    _connectionStatus.close();
    _sessionStatus.close();
    _messages.close();
    _audioOutput.close();
    _errors.close();
  }
}
