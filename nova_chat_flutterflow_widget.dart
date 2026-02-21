// ============================================================================
// NovaChatWidget — FlutterFlow Custom Widget
// ============================================================================
//
// Drop-in custom widget for FlutterFlow. All state is managed inside the
// widget lifecycle — no Cubit, no GetIt, no external providers needed.
//
// Parameters:
//   • voice  (String) — persona name, e.g. "greta" or "lennart"
//   • topic  (String) — instruction / system prompt content
//
// Required packages (add to FlutterFlow pubspec):
//   socket_io_client: ^3.1.4
//   record: ^6.2.0
//   just_audio: ^0.10.5
//   audio_session: ^0.1.25
//   permission_handler: ^12.0.1
//   path_provider: ^2.1.5
//
// ============================================================================

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:audio_session/audio_session.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:just_audio/just_audio.dart';
import 'package:record/record.dart';
import 'package:socket_io_client/socket_io_client.dart' as io;

// ── Constants ────────────────────────────────────────────────────────────────

const String _kServerUrl = 'http://13.61.2.209:8080';

// ── Enums ────────────────────────────────────────────────────────────────────

enum _ConnectionStatus { disconnected, connecting, connected, error }

enum _SessionStatus {
  idle,
  initializing,
  ready,
  recording,
  processing,
  aiSpeaking,
  turnComplete,
  waitingForPlayback,
  error,
  closed,
}

// ── Domain entities ──────────────────────────────────────────────────────────

enum MessageRole { user, assistant }

class ChatMessage {
  final String id;
  final String content;
  final MessageRole role;
  final DateTime timestamp;

  ChatMessage({
    required this.id,
    required this.content,
    required this.role,
    required this.timestamp,
  });
}

// ── Socket data source ───────────────────────────────────────────────────────

class _SocketDataSource {
  io.Socket? _socket;

  final _connectionStatus =
      StreamController<_ConnectionStatus>.broadcast();
  final _sessionStatus = StreamController<_SessionStatus>.broadcast();
  final _messages = StreamController<ChatMessage>.broadcast();
  final _audioOutput = StreamController<List<int>>.broadcast();
  final _errors = StreamController<String>.broadcast();

  Stream<_ConnectionStatus> get connectionStatusStream =>
      _connectionStatus.stream;
  Stream<_SessionStatus> get sessionStatusStream => _sessionStatus.stream;
  Stream<ChatMessage> get messageStream => _messages.stream;
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
      _connectionStatus.add(_ConnectionStatus.connected);
      if (!completer.isCompleted) completer.complete(true);
    });

    _socket!.onConnectError((err) {
      debugPrint('[Socket] ❌ connect error: $err');
      _connectionStatus.add(_ConnectionStatus.error);
      _errors.add('Connection error: $err');
      if (!completer.isCompleted) completer.complete(false);
    });

    _socket!.onDisconnect((_) {
      debugPrint('[Socket] 🔌 disconnected');
      _connectionStatus.add(_ConnectionStatus.disconnected);
    });

    _socket!.on('connect_error', (err) {
      debugPrint('[Socket] connect_error event: $err');
    });

    _registerServerEvents();

    _connectionStatus.add(_ConnectionStatus.connecting);
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
    _sessionStatus.add(_SessionStatus.initializing);

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
          _sessionStatus.add(_SessionStatus.error);
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
      _sessionStatus.add(_SessionStatus.ready);
    });

    _socket?.on('contentStart', (data) {
      debugPrint('[Socket] ← contentStart: $data');
      if (data is Map<String, dynamic>) {
        final type = data['type']?.toString().toUpperCase();
        final role = data['role']?.toString().toUpperCase();
        if (type == 'AUDIO' && role == 'ASSISTANT') {
          _sessionStatus.add(_SessionStatus.aiSpeaking);
        }
      }
    });

    _socket?.on('textOutput', (data) {
      debugPrint('[Socket] ← textOutput: $data');
      if (data is Map<String, dynamic>) {
        final content = data['content'] as String?;
        if (content != null && content.isNotEmpty) {
          final roleStr = (data['role'] as String? ?? '').toUpperCase();
          _messages.add(
            ChatMessage(
              id: DateTime.now().microsecondsSinceEpoch.toString(),
              content: content,
              role: roleStr == 'USER' ? MessageRole.user : MessageRole.assistant,
              timestamp: DateTime.now(),
            ),
          );
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
      if (data is Map<String, dynamic>) {
        final stopReason = data['stopReason']?.toString();
        final type = data['type']?.toString().toUpperCase();
        if (stopReason == 'END_TURN' && type == 'AUDIO') {
          debugPrint(
            '[Socket] ← contentEnd END_TURN on AUDIO → emitting turnComplete',
          );
          _sessionStatus.add(_SessionStatus.turnComplete);
        }
      }
    });

    _socket?.on('turnComplete', (_) {
      debugPrint('[Socket] ← turnComplete');
      _sessionStatus.add(_SessionStatus.turnComplete);
    });

    _socket?.on('streamComplete', (_) {
      debugPrint('[Socket] ← streamComplete');
      _sessionStatus.add(_SessionStatus.idle);
    });

    _socket?.on('sessionClosed', (_) {
      debugPrint('[Socket] ← sessionClosed');
      _sessionStatus.add(_SessionStatus.closed);
    });

    _socket?.on('usageEvent', (data) {
      debugPrint('[Socket] ← usageEvent: $data');
    });

    _socket?.on('completionStart', (_) {
      debugPrint('[Socket] ← completionStart');
    });

    _socket?.on('error', (data) {
      debugPrint('[Socket] ← error: $data');
      final message =
          data is Map ? data['message']?.toString() : data?.toString();
      _errors.add(message ?? 'Unknown error');
      _sessionStatus.add(_SessionStatus.error);
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

// ── Audio recorder service ───────────────────────────────────────────────────

class _AudioRecorderService {
  final AudioRecorder _recorder = AudioRecorder();
  StreamSubscription<List<int>>? _recordSubscription;
  final _chunkController = StreamController<String>.broadcast();
  final _silenceController = StreamController<void>.broadcast();

  static const int _speechThreshold = 300;
  static const int _silenceThreshold = 200;
  static const int _silenceChunksRequired = 25;
  static const int _minSpeechChunks = 5;

  bool _hasSpeech = false;
  int _silenceCount = 0;
  int _speechCount = 0;
  int _chunkCount = 0;
  bool _isHardwareActive = false;

  Stream<String> get audioChunkStream => _chunkController.stream;
  Stream<void> get silenceDetectedStream => _silenceController.stream;

  Future<bool> hasPermission() => _recorder.hasPermission();

  Future<void> startRecording() async {
    _resetVad();
    _chunkCount = 0;

    if (_isHardwareActive) {
      debugPrint(
        '[AudioRecorder] startRecording() — hardware already active, VAD reset only',
      );
      return;
    }

    try {
      debugPrint('[AudioRecorder] startRecording() — starting hardware mic');

      final stream = await _recorder.startStream(
        const RecordConfig(
          encoder: AudioEncoder.pcm16bits,
          sampleRate: 16000,
          numChannels: 1,
          autoGain: true,
          echoCancel: true,
          noiseSuppress: true,
        ),
      );

      _isHardwareActive = true;

      _recordSubscription = stream.listen(
        _onChunk,
        onError: (Object e, StackTrace st) {
          debugPrint('[AudioRecorder] stream error: $e\n$st');
        },
        onDone: () {
          debugPrint('[AudioRecorder] stream done  totalChunks=$_chunkCount');
          _isHardwareActive = false;
        },
        cancelOnError: false,
      );

      debugPrint('[AudioRecorder] stream active');
    } catch (e, st) {
      debugPrint('[AudioRecorder] failed to start: $e\n$st');
    }
  }

  Future<void> stopRecording() async {
    debugPrint('[AudioRecorder] stopRecording()  totalChunks=$_chunkCount');
    _isHardwareActive = false;
    await _recordSubscription?.cancel();
    _recordSubscription = null;
    await _recorder.stop();
    _resetVad();
  }

  void resetVad() => _resetVad();

  void dispose() {
    _recordSubscription?.cancel();
    _chunkController.close();
    _silenceController.close();
    _recorder.dispose();
  }

  void _resetVad() {
    _hasSpeech = false;
    _silenceCount = 0;
    _speechCount = 0;
  }

  void _onChunk(List<int> data) {
    if (_chunkController.isClosed) return;

    _chunkCount++;
    _chunkController.add(base64Encode(Uint8List.fromList(data)));

    final rms = _computeRms(data);

    final barLen = (rms / 200).clamp(0, 20).toInt();
    final bar = '${'█' * barLen}${'░' * (20 - barLen)}';

    final String vadState;
    if (!_hasSpeech) {
      vadState = 'WAITING  need rms>=$_speechThreshold to start';
    } else if (rms >= _speechThreshold) {
      vadState = 'SPEECH   silence reset  total=$_speechCount';
    } else if (rms < _silenceThreshold) {
      vadState = 'SILENCE  $_silenceCount/$_silenceChunksRequired';
    } else {
      vadState = 'HOLD     between thresholds';
    }

    debugPrint(
      '[VAD #${_chunkCount.toString().padLeft(4)}] '
      'rms=${rms.toString().padLeft(5)}  |$bar|  $vadState',
    );

    if (rms >= _speechThreshold) {
      _hasSpeech = true;
      _speechCount++;
      _silenceCount = 0;
    } else if (rms < _silenceThreshold && _hasSpeech) {
      _silenceCount++;

      if (_speechCount >= _minSpeechChunks &&
          _silenceCount >= _silenceChunksRequired) {
        debugPrint(
          '[VAD] ✅ END OF SPEECH  speechChunks=$_speechCount  '
          'silentChunks=$_silenceCount — firing silenceDetectedStream',
        );
        _resetVad();
        if (!_silenceController.isClosed) {
          _silenceController.add(null);
        }
      }
    }
  }

  int _computeRms(List<int> data) {
    if (data.length < 2) return 0;
    final bytes = Uint8List.fromList(data);
    final samples = bytes.buffer.asInt16List();
    double sum = 0;
    for (final sample in samples) {
      sum += sample * sample;
    }
    return math.sqrt(sum / samples.length).round();
  }
}

// ── Audio player service ─────────────────────────────────────────────────────

class _AudioPlayerService {
  final AudioPlayer _player = AudioPlayer(handleInterruptions: false);
  final Queue<Uint8List> _queue = Queue();
  bool _isPlaying = false;
  bool _playbackSessionActive = false;
  bool _expectingMore = false;

  final _playbackComplete = StreamController<void>.broadcast();
  Stream<void> get playbackCompleteStream => _playbackComplete.stream;

  void markStreamActive() {
    _expectingMore = true;
  }

  void markStreamDone() {
    _expectingMore = false;
    if (!_isPlaying && _queue.isEmpty) {
      _onAllPlaybackDone();
    }
  }

  Future<void> enqueueChunk(List<int> pcmBytes) async {
    _queue.add(Uint8List.fromList(pcmBytes));
    if (!_isPlaying) {
      _drainQueue();
    }
  }

  Future<void> _deactivatePlaybackSession() async {
    if (!_playbackSessionActive) return;
    _playbackSessionActive = false;
    final session = await AudioSession.instance;
    await session.setActive(false);
  }

  Future<void> _onAllPlaybackDone() async {
    await _deactivatePlaybackSession();
    _playbackComplete.add(null);
  }

  Future<void> _drainQueue() async {
    if (_isPlaying || _queue.isEmpty) return;
    _isPlaying = true;

    final allBytes = BytesBuilder();
    while (_queue.isNotEmpty) {
      allBytes.add(_queue.removeFirst());
    }

    final pcmData = allBytes.toBytes();
    final wavData = _wrapInWav(pcmData, 24000, 16, 1);

    try {
      final source = _WavAudioSource(wavData);
      await _player.setAudioSource(source);
      await _player.play();
      await _player.playerStateStream.firstWhere(
        (state) => state.processingState == ProcessingState.completed,
      );
    } catch (_) {
      // Silently handle playback errors
    }

    _isPlaying = false;

    if (_queue.isNotEmpty) {
      _drainQueue();
    } else if (!_expectingMore) {
      await _onAllPlaybackDone();
    }
  }

  Uint8List _wrapInWav(
    Uint8List pcmData,
    int sampleRate,
    int bitsPerSample,
    int channels,
  ) {
    final dataSize = pcmData.length;
    final header = ByteData(44);

    header.setUint8(0, 0x52);
    header.setUint8(1, 0x49);
    header.setUint8(2, 0x46);
    header.setUint8(3, 0x46);
    header.setUint32(4, 36 + dataSize, Endian.little);
    header.setUint8(8, 0x57);
    header.setUint8(9, 0x41);
    header.setUint8(10, 0x56);
    header.setUint8(11, 0x45);
    header.setUint8(12, 0x66);
    header.setUint8(13, 0x6D);
    header.setUint8(14, 0x74);
    header.setUint8(15, 0x20);
    header.setUint32(16, 16, Endian.little);
    header.setUint16(20, 1, Endian.little);
    header.setUint16(22, channels, Endian.little);
    header.setUint32(24, sampleRate, Endian.little);
    header.setUint32(
      28,
      sampleRate * channels * bitsPerSample ~/ 8,
      Endian.little,
    );
    header.setUint16(32, channels * bitsPerSample ~/ 8, Endian.little);
    header.setUint16(34, bitsPerSample, Endian.little);
    header.setUint8(36, 0x64);
    header.setUint8(37, 0x61);
    header.setUint8(38, 0x74);
    header.setUint8(39, 0x61);
    header.setUint32(40, dataSize, Endian.little);

    final result = Uint8List(44 + dataSize);
    result.setAll(0, header.buffer.asUint8List());
    result.setAll(44, pcmData);
    return result;
  }

  void dispose() {
    _playbackComplete.close();
    _player.dispose();
  }
}

class _WavAudioSource extends StreamAudioSource {
  final Uint8List _data;
  _WavAudioSource(this._data);

  @override
  Future<StreamAudioResponse> request([int? start, int? end]) async {
    start ??= 0;
    end ??= _data.length;
    return StreamAudioResponse(
      sourceLength: _data.length,
      contentLength: end - start,
      offset: start,
      stream: Stream.value(_data.sublist(start, end)),
      contentType: 'audio/wav',
    );
  }
}

// ── Message bubble widget ────────────────────────────────────────────────────

class _MessageBubble extends StatelessWidget {
  final ChatMessage message;
  const _MessageBubble({required this.message});

  @override
  Widget build(BuildContext context) {
    final isUser = message.role == MessageRole.user;
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4),
      child: Row(
        mainAxisAlignment:
            isUser ? MainAxisAlignment.end : MainAxisAlignment.start,
        children: [
          Flexible(
            child: Container(
              padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
              decoration: BoxDecoration(
                color: isUser
                    ? Theme.of(context).colorScheme.primary
                    : Colors.grey[300],
                borderRadius: BorderRadius.only(
                  topLeft: const Radius.circular(16),
                  topRight: const Radius.circular(16),
                  bottomLeft: Radius.circular(isUser ? 16 : 4),
                  bottomRight: Radius.circular(isUser ? 4 : 16),
                ),
              ),
              child: Text(
                message.content,
                style: TextStyle(
                  color: isUser ? Colors.white : Colors.black87,
                  fontSize: 15,
                ),
              ),
            ),
          ),
        ],
      ),
    );
  }
}

// ── Main widget ───────────────────────────────────────────────────────────────

class NovaChatWidget extends StatefulWidget {
  /// Persona name shown in the AppBar and included in the system prompt,
  /// e.g. "greta" or "lennart".
  final String voice;

  /// Instruction / system-prompt content for this conversation topic.
  final String topic;

  const NovaChatWidget({
    super.key,
    required this.voice,
    required this.topic,
  });

  @override
  State<NovaChatWidget> createState() => _NovaChatWidgetState();
}

class _NovaChatWidgetState extends State<NovaChatWidget> {
  // ── Services ──────────────────────────────────────────────────────────────
  late final _SocketDataSource _socket;
  late final _AudioRecorderService _recorder;
  late final _AudioPlayerService _player;

  // ── Stream subscriptions ──────────────────────────────────────────────────
  StreamSubscription? _connectionSub;
  StreamSubscription? _sessionSub;
  StreamSubscription? _messageSub;
  StreamSubscription? _audioOutputSub;
  StreamSubscription? _errorSub;
  StreamSubscription? _recorderSub;
  StreamSubscription? _playbackSub;
  StreamSubscription? _vadSub;

  // ── Local state ───────────────────────────────────────────────────────────
  _ConnectionStatus _connectionStatus = _ConnectionStatus.disconnected;
  _SessionStatus _sessionStatus = _SessionStatus.idle;
  List<ChatMessage> _messages = [];
  bool _isLoading = true;
  String? _errorMessage;

  // ── Internal flags (mirrors ChatCubit fields) ─────────────────────────────
  bool _greetingTriggered = false;
  bool _micStarting = false;
  bool _sessionStreamStarted = false;

  final ScrollController _scrollController = ScrollController();

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  @override
  void initState() {
    super.initState();
    _socket = _SocketDataSource();
    _recorder = _AudioRecorderService();
    _player = _AudioPlayerService();
    _subscribeToStreams();
    _autoConnect();
  }

  @override
  void dispose() {
    _scrollController.dispose();

    _connectionSub?.cancel();
    _sessionSub?.cancel();
    _messageSub?.cancel();
    _audioOutputSub?.cancel();
    _errorSub?.cancel();
    _recorderSub?.cancel();
    _playbackSub?.cancel();
    _vadSub?.cancel();

    _stopMic();

    // Send stopAudio only once — when the session truly ends.
    if (_sessionStreamStarted) {
      _socket.emitStopAudio();
    }

    _recorder.dispose();
    _player.dispose();
    _socket.dispose();

    AudioSession.instance.then((s) => s.setActive(false));

    super.dispose();
  }

  // ── Stream subscriptions ──────────────────────────────────────────────────

  void _subscribeToStreams() {
    _connectionSub = _socket.connectionStatusStream.listen((status) {
      debugPrint('[Widget] connectionStatus → $status');
      if (mounted) setState(() => _connectionStatus = status);
    });

    _sessionSub = _socket.sessionStatusStream.listen((status) {
      debugPrint(
        '[Widget] sessionStatus → $status  '
        '(greetingTriggered=$_greetingTriggered, micStarting=$_micStarting)',
      );
      if (!mounted) return;
      setState(() => _sessionStatus = status);

      if (status == _SessionStatus.aiSpeaking) {
        debugPrint(
          '[Widget] AI speaking → pausing mic send (hardware stays on for AEC)',
        );
        _player.markStreamActive();
        _pauseMicSend();
      }

      if (status == _SessionStatus.turnComplete && _greetingTriggered) {
        debugPrint('[Widget] turnComplete → waiting for playback to finish');
        _player.markStreamDone();
        if (mounted) {
          setState(() => _sessionStatus = _SessionStatus.waitingForPlayback);
        }
      }
    });

    _messageSub = _socket.messageStream.listen((message) {
      debugPrint(
        '[Widget] message received role=${message.role} '
        'content="${message.content.substring(0, message.content.length.clamp(0, 60))}"',
      );
      if (!mounted) return;
      setState(() {
        _messages = [..._messages, message];
        _isLoading = false;
      });
      _scrollToBottom();
    });

    _playbackSub = _player.playbackCompleteStream.listen((_) {
      debugPrint('[Widget] playbackComplete → resuming mic for next turn');
      if (mounted &&
          _greetingTriggered &&
          _sessionStatus == _SessionStatus.waitingForPlayback) {
        _resumeMicSend();
      }
    });

    _audioOutputSub = _socket.audioOutputStream.listen((pcmBytes) {
      debugPrint('[Widget] audioOutput chunk ${pcmBytes.length} bytes');
      _player.enqueueChunk(pcmBytes);
    });

    _errorSub = _socket.errorStream.listen((error) {
      debugPrint('[Widget] ❌ error: $error');
      if (mounted) setState(() => _errorMessage = error);
    });
  }

  // ── Connection ────────────────────────────────────────────────────────────

  Future<void> _autoConnect() async {
    debugPrint('[Widget] _autoConnect() voice=${widget.voice}');
    await _configureAudioSession();
    final connected = await _connect();
    if (connected && mounted) {
      _greetingTriggered = true;
      _startNextTurn();
    }
  }

  Future<void> _configureAudioSession() async {
    final session = await AudioSession.instance;
    await session.configure(
      AudioSessionConfiguration(
        avAudioSessionCategory: AVAudioSessionCategory.playAndRecord,
        avAudioSessionCategoryOptions:
            AVAudioSessionCategoryOptions.defaultToSpeaker |
            AVAudioSessionCategoryOptions.allowBluetooth,
        avAudioSessionMode: AVAudioSessionMode.videoChat,
        androidAudioAttributes: const AndroidAudioAttributes(
          contentType: AndroidAudioContentType.speech,
          usage: AndroidAudioUsage.voiceCommunication,
        ),
        androidAudioFocusGainType: AndroidAudioFocusGainType.gain,
        androidWillPauseWhenDucked: false,
      ),
    );
    await session.setActive(true);
  }

  Future<bool> _connect() async {
    debugPrint('[Widget] _connect() → $_kServerUrl');
    final connected = await _socket.connect(_kServerUrl);
    if (!connected) return false;
    return _socket.initializeConnection();
  }

  // ── Turn management (mirrors ChatCubit logic) ─────────────────────────────

  Future<void> _startNextTurn() async {
    debugPrint('[Widget] _startNextTurn() called  micStarting=$_micStarting');
    if (!mounted || _micStarting) return;
    _micStarting = true;

    try {
      if (!_sessionStreamStarted) {
        debugPrint('[Widget] _startNextTurn() first turn → startRecordingSession()');
        final systemPrompt =
            'You are ${widget.voice}, a helpful assistant. ${widget.topic}';
        await _sendPromptStart();
        await Future.delayed(const Duration(milliseconds: 100));
        _socket.emitSystemPrompt(systemPrompt);
        await Future.delayed(const Duration(milliseconds: 100));
        _socket.emitAudioStart();

        debugPrint('[Widget] _startNextTurn() → waiting for audioReady');
        final readyStatus = await _socket.sessionStatusStream
            .firstWhere((s) => s == _SessionStatus.ready)
            .timeout(
              const Duration(seconds: 8),
              onTimeout: () {
                debugPrint(
                  '[Widget] _startNextTurn() ⏱ timed out waiting for audioReady',
                );
                return _SessionStatus.error;
              },
            );
        if (readyStatus == _SessionStatus.error) return;

        _sessionStreamStarted = true;
        debugPrint('[Widget] _startNextTurn() ✅ audioReady received');
      } else {
        debugPrint(
          '[Widget] _startNextTurn() subsequent turn — stream already open, '
          'restarting mic only',
        );
      }

      if (!mounted) return;

      _vadSub?.cancel();
      _vadSub = _recorder.silenceDetectedStream.listen((_) {
        debugPrint('[Widget] VAD silence detected → auto-stopping mic');
        if (mounted && _sessionStatus == _SessionStatus.recording) {
          _pauseMicSend();
        }
      });

      debugPrint('[Widget] _startNextTurn() → recorder.startRecording()');
      await _recorder.startRecording();

      _recorderSub?.cancel();
      _recorderSub = _recorder.audioChunkStream.listen(
        (base64Chunk) {
          _socket.emitAudioInput(base64Chunk);
        },
        onError: (Object e) => debugPrint('[Widget] audioChunkStream error: $e'),
      );

      debugPrint('[Widget] _startNextTurn() ✅ mic is live — VAD will auto-stop');
      if (mounted) {
        setState(() => _sessionStatus = _SessionStatus.recording);
      }
    } catch (e, st) {
      debugPrint('[Widget] _startNextTurn() ❌ error: $e\n$st');
    } finally {
      _micStarting = false;
    }
  }

  Future<void> _sendPromptStart() async {
    _socket.emitPromptStart();
  }

  Future<void> _stopMic() async {
    debugPrint('[Widget] _stopMic() — full hardware stop');
    await _vadSub?.cancel();
    _vadSub = null;
    await _recorderSub?.cancel();
    _recorderSub = null;
    await _recorder.stopRecording();
  }

  Future<void> _pauseMicSend() async {
    debugPrint('[Widget] _pauseMicSend() — stop sending, hardware stays on');
    await _vadSub?.cancel();
    _vadSub = null;
    await _recorderSub?.cancel();
    _recorderSub = null;
    // DO NOT call _recorder.stopRecording() — hardware mic stays alive for AEC
  }

  Future<void> _resumeMicSend() async {
    debugPrint('[Widget] _resumeMicSend() — re-subscribing to mic stream');
    if (!mounted || _micStarting) return;

    _recorder.resetVad();

    _vadSub?.cancel();
    _vadSub = _recorder.silenceDetectedStream.listen((_) {
      debugPrint('[Widget] VAD silence → pausing mic send');
      if (mounted && _sessionStatus == _SessionStatus.recording) {
        _pauseMicSend();
      }
    });

    _recorderSub?.cancel();
    _recorderSub = _recorder.audioChunkStream.listen(
      (base64Chunk) {
        _socket.emitAudioInput(base64Chunk);
      },
      onError: (Object e) => debugPrint('[Widget] audioChunkStream error: $e'),
    );

    if (mounted) {
      setState(() => _sessionStatus = _SessionStatus.recording);
    }
  }

  // ── UI helpers ────────────────────────────────────────────────────────────

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          _scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 300),
          curve: Curves.easeOut,
        );
      }
    });
  }

  String _loadingLabel() {
    if (_connectionStatus == _ConnectionStatus.connecting) {
      return 'Connecting...';
    }
    if (_connectionStatus == _ConnectionStatus.error) {
      return _errorMessage ?? 'Connection error';
    }
    return switch (_sessionStatus) {
      _SessionStatus.initializing => 'Initializing session...',
      _SessionStatus.ready => 'Starting conversation...',
      _SessionStatus.aiSpeaking => 'AI is speaking...',
      _ => 'Please wait...',
    };
  }

  // ── Build ─────────────────────────────────────────────────────────────────

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        leading: IconButton(
          icon: const Icon(Icons.arrow_back),
          onPressed: () => Navigator.of(context).pop(),
        ),
        title: Text(widget.voice),
        centerTitle: true,
        actions: [
          Padding(
            padding: const EdgeInsets.only(right: 16),
            child: Icon(
              _statusIcon,
              size: 20,
              color: _statusColor,
            ),
          ),
        ],
      ),
      body: Stack(
        children: [
          // ── Chat messages ────────────────────────────────────────────────
          if (_messages.isEmpty)
            const SizedBox.shrink()
          else
            ListView.builder(
              controller: _scrollController,
              padding: const EdgeInsets.fromLTRB(16, 8, 16, 120),
              itemCount: _messages.length,
              itemBuilder: (context, index) =>
                  _MessageBubble(message: _messages[index]),
            ),

          // ── Loading overlay ──────────────────────────────────────────────
          if (_isLoading)
            Container(
              color: Theme.of(context).colorScheme.surface,
              child: Center(
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    const CircularProgressIndicator(),
                    const SizedBox(height: 24),
                    Text(
                      _loadingLabel(),
                      style: TextStyle(
                        color: Colors.grey[600],
                        fontSize: 15,
                      ),
                    ),
                  ],
                ),
              ),
            ),

          // ── Recording indicator ──────────────────────────────────────────
          if (_sessionStatus == _SessionStatus.recording)
            Positioned(
              bottom: 24,
              left: 24,
              right: 24,
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  Container(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 12,
                      vertical: 6,
                    ),
                    decoration: BoxDecoration(
                      color: Colors.black54,
                      borderRadius: BorderRadius.circular(20),
                    ),
                    child: const Text(
                      '🎙 Listening… will stop automatically when you pause',
                      style: TextStyle(color: Colors.white, fontSize: 12),
                      textAlign: TextAlign.center,
                    ),
                  ),
                ],
              ),
            ),
        ],
      ),
    );
  }

  IconData get _statusIcon => switch (_sessionStatus) {
        _SessionStatus.recording => Icons.mic,
        _SessionStatus.aiSpeaking => Icons.volume_up,
        _SessionStatus.processing => Icons.hourglass_top,
        _ => Icons.mic_off,
      };

  Color get _statusColor => switch (_sessionStatus) {
        _SessionStatus.recording => Colors.red,
        _SessionStatus.aiSpeaking => Colors.blue,
        _SessionStatus.processing => Colors.orange,
        _ => Colors.grey,
      };
}
