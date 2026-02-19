import 'dart:async';
import 'dart:convert';

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
    _greetingTriggered = false;
    _systemPrompt = voice.systemPrompt;
    final result = await _connectUseCase.execute(
      'http://10.0.2.2:3000',
      _systemPrompt,
    );
    debugPrint('[Cubit] autoConnect() ConnectUseCase returned $result');
    if (result && !isClosed) {
      await _triggerGreeting();
    }
  }

  Future<void> _triggerGreeting() async {
    debugPrint(
      '[Cubit] _triggerGreeting() → sending user text turn to trigger greeting',
    );
    _greetingTriggered = true;
    // Nova Sonic requires audio content in every prompt.
    // Open an audio block, send 1 silence chunk (100ms), then a text "Hello" turn.
    // The backend auto-closes the audio block before the text, giving correct ordering:
    //   contentEnd(AUDIO) → contentStart(TEXT) → textInput → contentEnd(TEXT) → promptEnd
    await _repository.sendAudioStart();

    debugPrint('[Cubit] _triggerGreeting() → waiting for audioReady');
    await _repository.sessionStatusStream
        .firstWhere((s) => s == SessionStatus.ready)
        .timeout(
          const Duration(seconds: 8),
          onTimeout: () {
            debugPrint(
              '[Cubit] _triggerGreeting() ⏱ timed out waiting for audioReady',
            );
            return SessionStatus.error;
          },
        );

    // One 100ms silence chunk to satisfy "content must have data" requirement.
    await _repository.sendAudioChunk(base64Encode(Uint8List(3200)));
    await _repository.sendUserText('Hello');
    await _repository.sendStopAudio();
    debugPrint('[Cubit] _triggerGreeting() done — waiting for AI response');
  }

  /// Starts a new prompt within the existing session and begins recording.
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

      debugPrint('[Cubit] _startNextTurn() → subscribing to audioChunkStream');
      _recorderSub = _recorder.audioChunkStream.listen((base64Chunk) {
        _sendAudioUseCase.sendChunk(base64Chunk);
      });

      debugPrint('[Cubit] _startNextTurn() → recorder.startRecording()');
      await _recorder.startRecording();
      debugPrint('[Cubit] _startNextTurn() ✅ mic is live');
      if (!isClosed)
        emit(state.copyWith(sessionStatus: SessionStatus.recording));
    } catch (e) {
      debugPrint('[Cubit] _startNextTurn() ❌ error: $e');
    } finally {
      _micStarting = false;
    }
  }

  Future<void> _stopMic() async {
    debugPrint('[Cubit] _stopMic()  recorderSub=${_recorderSub != null}');
    if (_recorderSub == null) return;
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
    await _recorder.stopRecording();
    _recorder.dispose();
    _player.dispose();
    _repository.dispose();
    return super.close();
  }
}
