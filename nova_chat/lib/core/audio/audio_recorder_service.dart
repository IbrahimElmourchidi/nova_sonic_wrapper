import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:flutter/foundation.dart';
import 'package:record/record.dart';

/// Simple energy-based Voice Activity Detector.
///
/// State machine:
///   WAITING_FOR_SPEECH  →  (RMS > speechThreshold)  →  IN_SPEECH
///   IN_SPEECH           →  (RMS < silenceThreshold for silenceChunks) → SILENCE_DETECTED
///
/// Once SILENCE_DETECTED fires, [silenceDetectedStream] emits one event
/// and the VAD resets so it can be reused on the next turn.
class AudioRecorderService {
  final AudioRecorder _recorder = AudioRecorder();
  StreamSubscription<List<int>>? _recordSubscription;
  final _chunkController = StreamController<String>.broadcast();
  final _silenceController = StreamController<void>.broadcast();

  // ── VAD tunables ──────────────────────────────────────────────────────────

  /// RMS value (0–32768) above which audio is considered speech.
  static const int _speechThreshold = 300;

  /// RMS value below which audio is considered silence.
  /// Slightly lower than speech threshold to create hysteresis.
  static const int _silenceThreshold = 200;

  /// How many consecutive silent chunks must follow speech before we declare
  /// the turn over.  Each chunk ≈ 60 ms at 16 kHz/16-bit/mono (960 samples).
  /// 25 chunks × 60 ms = 1 500 ms of trailing silence.
  static const int _silenceChunksRequired = 25;

  /// Minimum number of speech chunks required before silence detection can
  /// trigger. Prevents firing on a quiet environment before the user speaks.
  /// 5 chunks × 60 ms = 300 ms of speech required.
  static const int _minSpeechChunks = 5;

  // ── VAD state ─────────────────────────────────────────────────────────────
  bool _hasSpeech = false;
  int _silenceCount = 0;
  int _speechCount = 0;
  int _chunkCount = 0;

  // ── Public streams ────────────────────────────────────────────────────────

  /// Base64-encoded PCM chunks as they arrive from the microphone.
  Stream<String> get audioChunkStream => _chunkController.stream;

  /// Fires once when VAD determines the user has finished speaking.
  Stream<void> get silenceDetectedStream => _silenceController.stream;

  // ── Public API ────────────────────────────────────────────────────────────

  Future<bool> hasPermission() => _recorder.hasPermission();

  Future<void> startRecording() async {
    _resetVad();
    _chunkCount = 0;

    try {
      debugPrint('[AudioRecorder] startRecording()');
      debugPrint(
        '[VAD] thresholds: speech>=$_speechThreshold  silence<$_silenceThreshold  '
        'need $_silenceChunksRequired silent chunks after $_minSpeechChunks speech chunks',
      );

      final stream = await _recorder.startStream(
        const RecordConfig(
          encoder: AudioEncoder.pcm16bits,
          sampleRate: 16000,
          numChannels: 1,
          autoGain: false,
          echoCancel: false,
          noiseSuppress: false,
        ),
      );

      _recordSubscription = stream.listen(
        _onChunk,
        onError: (Object e, StackTrace st) {
          debugPrint('[AudioRecorder] stream error: $e\n$st');
        },
        onDone: () {
          debugPrint('[AudioRecorder] stream done  totalChunks=$_chunkCount');
        },
        cancelOnError: false,
      );

      debugPrint('[AudioRecorder] stream active — watching RMS levels:');
    } catch (e, st) {
      debugPrint('[AudioRecorder] failed to start: $e\n$st');
    }
  }

  Future<void> stopRecording() async {
    debugPrint('[AudioRecorder] stopRecording()  totalChunks=$_chunkCount');
    await _recordSubscription?.cancel();
    _recordSubscription = null;
    await _recorder.stop();
    _resetVad();
  }

  void dispose() {
    _recordSubscription?.cancel();
    _chunkController.close();
    _silenceController.close();
    _recorder.dispose();
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  void _resetVad() {
    _hasSpeech = false;
    _silenceCount = 0;
    _speechCount = 0;
  }

  void _onChunk(List<int> data) {
    if (_chunkController.isClosed) return;

    _chunkCount++;

    // Forward raw audio to the socket stream immediately.
    _chunkController.add(base64Encode(Uint8List.fromList(data)));

    // ── VAD logic ─────────────────────────────────────────────────────────
    final rms = _computeRms(data);

    // ASCII level bar: each filled block = 200 RMS units, 20 chars wide.
    // At RMS=300 (speech threshold) you will see 1-2 filled blocks.
    // Normal speech is typically RMS 500-3000 (3-15 filled blocks).
    final barLen = (rms / 200).clamp(0, 20).toInt();
    final bar = ('${'█' * barLen}${'░' * (20 - barLen)}');

    // Human-readable VAD state label for this chunk.
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

    // Print every single chunk so you can see real-time levels.
    debugPrint(
      '[VAD #${_chunkCount.toString().padLeft(4)}] '
      'rms=${rms.toString().padLeft(5)}  |$bar|  $vadState',
    );

    // ── State transitions ──────────────────────────────────────────────────
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
    // RMS between thresholds while _hasSpeech → hysteresis, no state change.
  }

  /// Compute the Root Mean Square energy of a 16-bit little-endian PCM buffer.
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
