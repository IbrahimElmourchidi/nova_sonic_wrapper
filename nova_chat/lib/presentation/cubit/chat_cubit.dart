import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter_bloc/flutter_bloc.dart';

import '../../core/audio/audio_player_service.dart';
import '../../core/audio/audio_recorder_service.dart';
import '../../domain/entities/voice_option.dart';
import '../../domain/enums/session_status.dart';
import '../../domain/repositories/i_chat_repository.dart';
import '../../domain/usecases/connect_usecase.dart';
import '../../domain/usecases/send_audio_usecase.dart';
import 'chat_state.dart';

class ChatCubit extends Cubit<ChatState> {
  final ConnectUseCase _connectUseCase;
  final SendAudioUseCase _sendAudioUseCase;
  final IChatRepository _repository;
  final AudioRecorderService _recorder;
  final AudioPlayerService _player;

  StreamSubscription? _connectionSub;
  StreamSubscription? _sessionSub;
  StreamSubscription? _messageSub;
  StreamSubscription? _audioOutputSub;
  StreamSubscription? _errorSub;
  StreamSubscription? _recorderSub;
  StreamSubscription? _playbackSub;
  StreamSubscription? _vadSub; // NEW: VAD silence events

  bool _greetingTriggered = false;
  bool _micStarting = false;
  String _systemPrompt = '';

  ChatCubit({
    required ConnectUseCase connectUseCase,
    required SendAudioUseCase sendAudioUseCase,
    required IChatRepository repository,
    required AudioRecorderService recorder,
    required AudioPlayerService player,
  }) : _connectUseCase = connectUseCase,
       _sendAudioUseCase = sendAudioUseCase,
       _repository = repository,
       _recorder = recorder,
       _player = player,
       super(const ChatState()) {
    _subscribeToStreams();
  }

  void _subscribeToStreams() {
    _connectionSub = _repository.connectionStatusStream.listen((status) {
      debugPrint('[Cubit] connectionStatus → $status');
      if (!isClosed) emit(state.copyWith(connectionStatus: status));
    });

    _sessionSub = _repository.sessionStatusStream.listen((status) {
      debugPrint(
        '[Cubit] sessionStatus → $status  '
        '(greetingTriggered=$_greetingTriggered, micStarting=$_micStarting, '
        'recorderSub=${_recorderSub != null})',
      );
      if (isClosed) return;
      emit(state.copyWith(sessionStatus: status));

      if (status == SessionStatus.aiSpeaking) {
        debugPrint('[Cubit] AI speaking → _stopMic()');
        _player.markStreamActive();
        _stopMic();
      }

      if (status == SessionStatus.turnComplete && _greetingTriggered) {
        debugPrint('[Cubit] turnComplete → waiting for playback to finish');
        _player.markStreamDone();
        emit(state.copyWith(sessionStatus: SessionStatus.waitingForPlayback));
      }
    });

    _messageSub = _repository.messageStream.listen((message) {
      debugPrint(
        '[Cubit] message received role=${message.role} '
        'content="${message.content.substring(0, message.content.length.clamp(0, 60))}"',
      );
      if (isClosed) return;
      emit(
        state.copyWith(
          messages: [...state.messages, message],
          isLoading: false,
        ),
      );
    });

    _playbackSub = _player.playbackCompleteStream.listen((_) {
      debugPrint('[Cubit] playbackComplete → _startNextTurn()');
      if (!isClosed &&
          _greetingTriggered &&
          state.sessionStatus == SessionStatus.waitingForPlayback) {
        _startNextTurn();
      }
    });

    _audioOutputSub = _repository.audioOutputStream.listen((pcmBytes) {
      debugPrint('[Cubit] audioOutput chunk ${pcmBytes.length} bytes');
      _player.enqueueChunk(pcmBytes);
    });

    _errorSub = _repository.errorStream.listen((error) {
      debugPrint('[Cubit] ❌ error: $error');
      if (!isClosed) emit(state.copyWith(errorMessage: error));
    });
  }

  Future<void> autoConnect(VoiceOption voice) async {
    debugPrint('[Cubit] autoConnect() voice=${voice.name}');
    _systemPrompt = voice.systemPrompt;
    final result = await _connectUseCase.execute('http://10.0.2.2:3000');
    debugPrint('[Cubit] autoConnect() ConnectUseCase returned $result');
    if (result && !isClosed) {
      _greetingTriggered = true;
      _startNextTurn();
    }
  }

  /// Starts a new prompt within the existing session and begins recording.
  /// VAD will automatically stop the turn when silence is detected.
  Future<void> _startNextTurn() async {
    debugPrint('[Cubit] _startNextTurn() called  micStarting=$_micStarting');
    if (isClosed || _micStarting) return;
    _micStarting = true;

    try {
      debugPrint('[Cubit] _startNextTurn() → sendAudioUseCase.startNextTurn()');
      final ok = await _sendAudioUseCase.startNextTurn(_systemPrompt);
      if (!ok) {
        debugPrint(
          '[Cubit] _startNextTurn() ❌ startNextTurn failed — aborting',
        );
        return;
      }

      debugPrint('[Cubit] _startNextTurn() → waiting for audioReady');
      final readyStatus = await _repository.sessionStatusStream
          .firstWhere((s) => s == SessionStatus.ready)
          .timeout(
            const Duration(seconds: 8),
            onTimeout: () {
              debugPrint(
                '[Cubit] _startNextTurn() ⏱ timed out waiting for audioReady',
              );
              return SessionStatus.error;
            },
          );
      if (readyStatus == SessionStatus.error) return;

      if (isClosed) return;

      // Subscribe to VAD silence events BEFORE starting the recorder so we
      // never miss the event in case the stream fires on the first tick.
      _vadSub?.cancel();
      _vadSub = _recorder.silenceDetectedStream.listen((_) {
        debugPrint('[Cubit] VAD silence detected → auto-stopping turn');
        // Guard: only act if still recording; ignore stale events.
        if (!isClosed && state.sessionStatus == SessionStatus.recording) {
          _stopMic();
        }
      });

      // Start physical recorder — VAD runs inside the recorder service.
      debugPrint('[Cubit] _startNextTurn() → recorder.startRecording()');
      await _recorder.startRecording();

      // Subscribe to audio chunks and forward to socket.
      _recorderSub?.cancel();
      _recorderSub = _recorder.audioChunkStream.listen(
        (base64Chunk) {
          _sendAudioUseCase.sendChunk(base64Chunk).catchError((Object e) {
            debugPrint('[Cubit] sendChunk error: $e');
          });
        },
        onError: (Object e) => debugPrint('[Cubit] audioChunkStream error: $e'),
      );

      debugPrint('[Cubit] _startNextTurn() ✅ mic is live — VAD will auto-stop');
      if (!isClosed) {
        emit(state.copyWith(sessionStatus: SessionStatus.recording));
      }
    } catch (e, st) {
      debugPrint('[Cubit] _startNextTurn() ❌ error: $e\n$st');
    } finally {
      _micStarting = false;
    }
  }

  /// Called by the UI manual stop button (fallback in case VAD misses the
  /// end of speech — e.g. very quiet environments or background noise).
  Future<void> stopSpeaking() async {
    debugPrint('[Cubit] stopSpeaking() called — ending user turn manually');
    if (_recorderSub == null) {
      debugPrint('[Cubit] stopSpeaking() — no active recording, ignoring');
      return;
    }
    await _stopMic();
  }

  Future<void> _stopMic() async {
    debugPrint('[Cubit] _stopMic()  recorderSub=${_recorderSub != null}');
    if (_recorderSub == null) return;

    // Cancel VAD subscription first to prevent double-fire.
    await _vadSub?.cancel();
    _vadSub = null;

    await _recorderSub?.cancel();
    _recorderSub = null;
    await _recorder.stopRecording();
    await _sendAudioUseCase.stopRecording();
    debugPrint('[Cubit] _stopMic() done');
  }

  @override
  Future<void> close() async {
    debugPrint('[Cubit] close()');
    _connectionSub?.cancel();
    _sessionSub?.cancel();
    _messageSub?.cancel();
    _audioOutputSub?.cancel();
    _errorSub?.cancel();
    _recorderSub?.cancel();
    _playbackSub?.cancel();
    _vadSub?.cancel();
    await _recorder.stopRecording();
    _recorder.dispose();
    _player.dispose();
    _repository.dispose();
    return super.close();
  }
}
