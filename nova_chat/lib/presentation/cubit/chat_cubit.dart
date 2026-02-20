// File: lib/presentation/cubit/chat_cubit.dart
import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter_bloc/flutter_bloc.dart';

import '../../core/audio/audio_player_service.dart';
import '../../core/audio/audio_recorder_service.dart';
import '../../core/constants.dart';
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
  StreamSubscription? _vadSub;

  bool _greetingTriggered = false;
  bool _micStarting = false;
  String _systemPrompt = '';

  // FIX: Track whether we have already opened the Nova Sonic stream with
  // promptStart / systemPrompt / audioStart.  After the first turn the stream
  // stays open for the entire session, so we must NEVER re-send those events.
  bool _sessionStreamStarted = false;

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
        debugPrint(
          '[Cubit] AI speaking → pausing mic send (hardware stays on for AEC)',
        );
        _player.markStreamActive();
        _pauseMicSend(); // ← renamed, see below
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
      debugPrint('[Cubit] playbackComplete → resuming mic for next turn');
      if (!isClosed &&
          _greetingTriggered &&
          state.sessionStatus == SessionStatus.waitingForPlayback) {
        _resumeMicSend();
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
    final result = await _connectUseCase.execute(AppConstants.serverUrl);
    debugPrint('[Cubit] autoConnect() ConnectUseCase returned $result');
    if (result && !isClosed) {
      _greetingTriggered = true;
      _startNextTurn();
    }
  }

  Future<void> _startNextTurn() async {
    debugPrint('[Cubit] _startNextTurn() called  micStarting=$_micStarting');
    if (isClosed || _micStarting) return;
    _micStarting = true;

    try {
      // ── FIRST TURN ONLY: open the Nova Sonic stream ──────────────────────
      //
      // FIX: We only send promptStart / systemPrompt / audioStart ONCE for
      // the entire conversation.  After that the stream stays alive on the
      // server.  Sending these events again on a live stream is what caused
      // the server to ignore them (they arrive on an already-open stream),
      // which in turn caused audioReady to never fire → timeout.
      //
      if (!_sessionStreamStarted) {
        debugPrint(
          '[Cubit] _startNextTurn() first turn → startRecordingSession()',
        );
        final ok = await _sendAudioUseCase.startRecordingSession(_systemPrompt);
        if (!ok) {
          debugPrint(
            '[Cubit] _startNextTurn() ❌ startRecordingSession failed — aborting',
          );
          return;
        }

        // Wait for server to confirm the audio stream is ready.
        // This only happens once — on the first turn.
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

        _sessionStreamStarted = true;
        debugPrint('[Cubit] _startNextTurn() ✅ audioReady received');
      } else {
        // Subsequent turns: stream is already open, just restart the mic.
        debugPrint(
          '[Cubit] _startNextTurn() subsequent turn — stream already open, '
          'restarting mic only',
        );
      }

      if (isClosed) return;

      // ── Start local recording (every turn) ───────────────────────────────

      // Subscribe to VAD silence events BEFORE starting the recorder.
      _vadSub?.cancel();
      _vadSub = _recorder.silenceDetectedStream.listen((_) {
        debugPrint('[Cubit] VAD silence detected → auto-stopping mic');
        if (!isClosed && state.sessionStatus == SessionStatus.recording) {
          _pauseMicSend();
        }
      });

      debugPrint('[Cubit] _startNextTurn() → recorder.startRecording()');
      await _recorder.startRecording();

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

  /// Manual stop button fallback (in case VAD misses end of speech).
  Future<void> stopSpeaking() async {
    debugPrint('[Cubit] stopSpeaking() called — ending user turn manually');
    if (_recorderSub == null) {
      debugPrint('[Cubit] stopSpeaking() — no active recording, ignoring');
      return;
    }
    await _stopMic();
  }

  /// Called at session end only — kills the hardware mic.
  Future<void> _stopMic() async {
    debugPrint('[Cubit] _stopMic() — full hardware stop');
    await _vadSub?.cancel();
    _vadSub = null;
    await _recorderSub?.cancel();
    _recorderSub = null;
    await _recorder.stopRecording();
  }

  /// Called between turns — stops sending audio to server but keeps hardware on for AEC.
  Future<void> _pauseMicSend() async {
    debugPrint('[Cubit] _pauseMicSend() — stop sending, hardware stays on');
    await _vadSub?.cancel();
    _vadSub = null;
    await _recorderSub?.cancel();
    _recorderSub = null;
    // DO NOT call _recorder.stopRecording() — hardware mic stays alive
  }

  /// Called after AI playback ends — re-subscribes to mic stream for next turn.
  Future<void> _resumeMicSend() async {
    debugPrint('[Cubit] _resumeMicSend() — re-subscribing to mic stream');
    if (isClosed || _micStarting) return;

    _recorder.resetVad(); // fresh VAD state for new turn

    // Re-attach VAD silence listener
    _vadSub?.cancel();
    _vadSub = _recorder.silenceDetectedStream.listen((_) {
      debugPrint('[Cubit] VAD silence → pausing mic send');
      if (!isClosed && state.sessionStatus == SessionStatus.recording) {
        _pauseMicSend();
      }
    });

    // Re-attach chunk sender
    _recorderSub?.cancel();
    _recorderSub = _recorder.audioChunkStream.listen((base64Chunk) {
      _sendAudioUseCase.sendChunk(base64Chunk).catchError((Object e) {
        debugPrint('[Cubit] sendChunk error: $e');
      });
    }, onError: (Object e) => debugPrint('[Cubit] audioChunkStream error: $e'));

    if (!isClosed) {
      emit(state.copyWith(sessionStatus: SessionStatus.recording));
    }
  }

  @override
  Future<void> close() async {
    debugPrint('[Cubit] close()');

    // Cancel all subscriptions first.
    _connectionSub?.cancel();
    _sessionSub?.cancel();
    _messageSub?.cancel();
    _audioOutputSub?.cancel();
    _errorSub?.cancel();
    _recorderSub?.cancel();
    _playbackSub?.cancel();
    _vadSub?.cancel();
    _stopMic();

    // Stop local recording.
    await _recorder.stopRecording();

    // FIX: Send stopAudio HERE — once, when the user truly ends the session.
    // This is the correct and only place where the server stream should be closed.
    if (_sessionStreamStarted) {
      await _sendAudioUseCase.endSession();
    }

    _recorder.dispose();
    _player.dispose();
    _repository.dispose();
    return super.close();
  }
}
