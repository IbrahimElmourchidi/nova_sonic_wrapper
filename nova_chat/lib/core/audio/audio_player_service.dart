import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';

import 'package:audio_session/audio_session.dart';
import 'package:just_audio/just_audio.dart';

class AudioPlayerService {
  final AudioPlayer _player = AudioPlayer(handleInterruptions: false);
  final Queue<Uint8List> _queue = Queue();
  bool _isPlaying = false;
  bool _playbackSessionActive = false;

  final _playbackComplete = StreamController<void>.broadcast();
  Stream<void> get playbackCompleteStream => _playbackComplete.stream;

  /// Whether more audio chunks are expected from the AI stream.
  bool _expectingMore = false;

  /// Call when AI audio stream starts (contentStart AUDIO ASSISTANT).
  void markStreamActive() {
    _expectingMore = true;
  }

  /// Call when AI audio stream is done (turnComplete received).
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

  /// Acquire transient audio focus before playback begins.

  /// Release audio focus so the recorder can capture mic input.
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

    // Collect all available chunks into one buffer
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

    // RIFF header
    header.setUint8(0, 0x52); // R
    header.setUint8(1, 0x49); // I
    header.setUint8(2, 0x46); // F
    header.setUint8(3, 0x46); // F
    header.setUint32(4, 36 + dataSize, Endian.little);
    header.setUint8(8, 0x57); // W
    header.setUint8(9, 0x41); // A
    header.setUint8(10, 0x56); // V
    header.setUint8(11, 0x45); // E

    // fmt chunk
    header.setUint8(12, 0x66); // f
    header.setUint8(13, 0x6D); // m
    header.setUint8(14, 0x74); // t
    header.setUint8(15, 0x20); // (space)
    header.setUint32(16, 16, Endian.little); // chunk size
    header.setUint16(20, 1, Endian.little); // PCM format
    header.setUint16(22, channels, Endian.little);
    header.setUint32(24, sampleRate, Endian.little);
    header.setUint32(
      28,
      sampleRate * channels * bitsPerSample ~/ 8,
      Endian.little,
    );
    header.setUint16(32, channels * bitsPerSample ~/ 8, Endian.little);
    header.setUint16(34, bitsPerSample, Endian.little);

    // data chunk
    header.setUint8(36, 0x64); // d
    header.setUint8(37, 0x61); // a
    header.setUint8(38, 0x74); // t
    header.setUint8(39, 0x61); // a
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
