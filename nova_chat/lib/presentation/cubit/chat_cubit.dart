import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:flutter_bloc/flutter_bloc.dart';

import '../../core/audio/audio_player_service.dart';
import '../../core/audio/audio_recorder_service.dart';
import '../../core/constants.dart';
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

  bool _greetingTriggered = false;

  ChatCubit({
    required ConnectUseCase connectUseCase,
    required SendAudioUseCase sendAudioUseCase,
    required IChatRepository repository,
    required AudioRecorderService recorder,
    required AudioPlayerService player,
  })  : _connectUseCase = connectUseCase,
        _sendAudioUseCase = sendAudioUseCase,
        _repository = repository,
        _recorder = recorder,
        _player = player,
        super(const ChatState()) {
    _subscribeToStreams();
  }

  void _subscribeToStreams() {
    _connectionSub = _repository.connectionStatusStream.listen((status) {
      emit(state.copyWith(connectionStatus: status));
    });

    _sessionSub = _repository.sessionStatusStream.listen((status) {
      emit(state.copyWith(sessionStatus: status));

      if (status == SessionStatus.ready && !_greetingTriggered) {
        _triggerGreeting();
      }
    });

    _messageSub = _repository.messageStream.listen((message) {
      emit(state.copyWith(messages: [...state.messages, message]));
    });

    _audioOutputSub = _repository.audioOutputStream.listen((pcmBytes) {
      _player.enqueueChunk(pcmBytes);
    });

    _errorSub = _repository.errorStream.listen((error) {
      emit(state.copyWith(errorMessage: error));
    });
  }

  Future<void> autoConnect() async {
    _greetingTriggered = false;
    await _connectUseCase.execute(
      AppConstants.serverUrl,
      AppConstants.systemPrompt,
    );
  }

  Future<void> _triggerGreeting() async {
    _greetingTriggered = true;
    // Send 100ms of silence at 16kHz 16-bit mono = 3200 bytes of zeros
    final silence = Uint8List(3200);
    final base64Silence = base64Encode(silence);
    await _repository.sendAudioChunk(base64Silence);
    await Future.delayed(const Duration(milliseconds: 50));
    await _repository.sendStopAudio();
  }

  Future<void> startRecording() async {
    if (state.isRecording) return;

    final hasPermission = await _recorder.hasPermission();
    if (!hasPermission) {
      emit(state.copyWith(errorMessage: 'Microphone permission denied'));
      return;
    }

    emit(state.copyWith(isRecording: true, sessionStatus: SessionStatus.recording));

    // Re-initialize session since stopAudio destroys it
    await _sendAudioUseCase.startRecordingSession(AppConstants.systemPrompt);

    // Wait for audioReady
    await Future.delayed(const Duration(milliseconds: 300));

    _recorderSub = _recorder.audioChunkStream.listen((base64Chunk) {
      _sendAudioUseCase.sendChunk(base64Chunk);
    });

    await _recorder.startRecording();
  }

  Future<void> stopRecording() async {
    if (!state.isRecording) return;

    await _recorder.stopRecording();
    await _recorderSub?.cancel();
    _recorderSub = null;

    await _sendAudioUseCase.stopRecording();
    emit(state.copyWith(
      isRecording: false,
      sessionStatus: SessionStatus.processing,
    ));
  }

  Future<void> toggleRecording() async {
    if (state.isRecording) {
      await stopRecording();
    } else {
      await startRecording();
    }
  }

  @override
  Future<void> close() {
    _connectionSub?.cancel();
    _sessionSub?.cancel();
    _messageSub?.cancel();
    _audioOutputSub?.cancel();
    _errorSub?.cancel();
    _recorderSub?.cancel();
    _recorder.dispose();
    _player.dispose();
    _repository.dispose();
    return super.close();
  }
}
