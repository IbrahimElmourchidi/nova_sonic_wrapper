import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
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
    debugPrint('[Socket] connect() → $url');
    final completer = Completer<bool>();

    _socket = io.io(
      url,
      io.OptionBuilder()
          .setTransports(['websocket'])
          .disableAutoConnect()
          .build(),
    );

    _socket!.onConnect((_) {
      debugPrint('[Socket] ✅ connected  id=${_socket?.id}');
      _connectionStatus.add(ConnectionStatus.connected);
      if (!completer.isCompleted) completer.complete(true);
    });

    _socket!.onConnectError((err) {
      debugPrint('[Socket] ❌ connect error: $err');
      _connectionStatus.add(ConnectionStatus.error);
      _errors.add('Connection error: $err');
      if (!completer.isCompleted) completer.complete(false);
    });

    _socket!.onDisconnect((_) {
      debugPrint('[Socket] 🔌 disconnected');
      _connectionStatus.add(ConnectionStatus.disconnected);
    });

    _socket!.on('connect_error', (err) {
      debugPrint('[Socket] connect_error event: $err');
    });

    _registerServerEvents();

    _connectionStatus.add(ConnectionStatus.connecting);
    debugPrint('[Socket] calling _socket.connect()');
    _socket!.connect();

    return completer.future.timeout(
      const Duration(seconds: 10),
      onTimeout: () {
        debugPrint('[Socket] ⏱ connect() timed out');
        _errors.add('Connection timeout');
        return false;
      },
    );
  }

  Future<bool> initializeConnection() async {
    debugPrint('[Socket] initializeConnection() → emitWithAck');
    final completer = Completer<bool>();
    _sessionStatus.add(SessionStatus.initializing);

    _socket?.emitWithAck(
      'initializeConnection',
      null,
      ack: (dynamic response) {
        debugPrint('[Socket] initializeConnection ack → $response');
        if (response is Map && response['success'] == true) {
          debugPrint('[Socket] ✅ session initialized');
          completer.complete(true);
        } else {
          debugPrint('[Socket] ❌ init failed: $response');
          _sessionStatus.add(SessionStatus.error);
          _errors.add(response?.toString() ?? 'Init failed');
          if (!completer.isCompleted) completer.complete(false);
        }
      },
    );

    return completer.future.timeout(
      const Duration(seconds: 10),
      onTimeout: () {
        debugPrint(
          '[Socket] ⏱ initializeConnection() ack timed out — ack never fired',
        );
        return false;
      },
    );
  }

  void emitStartNewChat() {
    debugPrint('[Socket] → emit startNewChat');
    _socket?.emit('startNewChat');
  }

  void emitPromptStart() {
    debugPrint('[Socket] → emit promptStart');
    _socket?.emit('promptStart');
  }

  void emitSystemPrompt(String content) {
    debugPrint('[Socket] → emit systemPrompt (${content.length} chars)');
    _socket?.emit('systemPrompt', {'content': content});
  }

  void emitAudioStart() {
    debugPrint('[Socket] → emit audioStart');
    _socket?.emit('audioStart', {});
  }

  void emitUserText(String content) {
    debugPrint('[Socket] → emit userText (${content.length} chars)');
    _socket?.emit('userText', {'content': content});
  }

  // FIX: Added missing debug print so we can confirm chunks are actually being sent.
  void emitAudioInput(String base64Pcm) {
    debugPrint('[Socket] → emit audioInput (${base64Pcm.length} b64 chars)');
    _socket?.emit('audioInput', base64Pcm);
  }

  void emitStopAudio() {
    debugPrint('[Socket] → emit stopAudio');
    _socket?.emit('stopAudio');
  }

  void _registerServerEvents() {
    _socket?.on('audioReady', (_) {
      debugPrint('[Socket] ← audioReady');
      _sessionStatus.add(SessionStatus.ready);
    });

    _socket?.on('contentStart', (data) {
      debugPrint('[Socket] ← contentStart: $data');
      if (data is Map<String, dynamic>) {
        final type = data['type']?.toString().toUpperCase();
        final role = data['role']?.toString().toUpperCase();
        if (type == 'AUDIO' && role == 'ASSISTANT') {
          _sessionStatus.add(SessionStatus.aiSpeaking);
        }
      }
    });

    _socket?.on('textOutput', (data) {
      debugPrint('[Socket] ← textOutput: $data');
      if (data is Map<String, dynamic>) {
        final content = data['content'] as String?;
        if (content != null && content.isNotEmpty) {
          _messages.add(ChatMessageModel.fromTextOutput(data));
        }
      }
    });

    _socket?.on('audioOutput', (data) {
      debugPrint('[Socket] ← audioOutput (chunk received)');
      if (data is Map<String, dynamic>) {
        final content = data['content'] as String?;
        if (content != null) {
          final bytes = base64Decode(content);
          _audioOutput.add(bytes);
        }
      }
    });

    _socket?.on('contentEnd', (data) {
      debugPrint('[Socket] ← contentEnd: $data');
    });

    _socket?.on('streamComplete', (_) {
      debugPrint('[Socket] ← streamComplete');
      _sessionStatus.add(SessionStatus.idle);
    });

    _socket?.on('turnComplete', (_) {
      debugPrint('[Socket] ← turnComplete');
      _sessionStatus.add(SessionStatus.turnComplete);
    });

    _socket?.on('sessionClosed', (_) {
      debugPrint('[Socket] ← sessionClosed');
      _sessionStatus.add(SessionStatus.closed);
    });

    _socket?.on('usageEvent', (data) {
      debugPrint('[Socket] ← usageEvent: $data');
    });

    _socket?.on('completionStart', (_) {
      debugPrint('[Socket] ← completionStart');
    });

    _socket?.on('error', (data) {
      debugPrint('[Socket] ← error: $data');
      final message = data is Map
          ? data['message']?.toString()
          : data?.toString();
      _errors.add(message ?? 'Unknown error');
      _sessionStatus.add(SessionStatus.error);
    });
  }

  void disconnect() {
    debugPrint('[Socket] disconnect()');
    _socket?.disconnect();
    _socket?.dispose();
    _socket = null;
  }

  void dispose() {
    debugPrint('[Socket] dispose()');
    disconnect();
    _connectionStatus.close();
    _sessionStatus.close();
    _messages.close();
    _audioOutput.close();
    _errors.close();
  }
}
