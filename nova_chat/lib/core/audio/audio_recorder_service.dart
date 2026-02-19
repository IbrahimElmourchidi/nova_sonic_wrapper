import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:record/record.dart';

class AudioRecorderService {
  final AudioRecorder _recorder = AudioRecorder();
  StreamSubscription<List<int>>? _recordSubscription;
  final _chunkController = StreamController<String>.broadcast();

  Stream<String> get audioChunkStream => _chunkController.stream;

  Future<bool> hasPermission() async {
    return await _recorder.hasPermission();
  }

  Future<void> startRecording() async {
    try {
      debugPrint('[AudioRecorder] startRecording()');
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

      _recordSubscription = stream.listen(
        (data) {
          final base64Chunk = base64Encode(Uint8List.fromList(data));
          _chunkController.add(base64Chunk);
        },
        onError: (e) => debugPrint('[AudioRecorder] stream error: $e'),
      );
      debugPrint('[AudioRecorder] stream active');
    } catch (e) {
      debugPrint('[AudioRecorder] failed to start: $e');
    }
  }

  Future<void> stopRecording() async {
    await _recordSubscription?.cancel();
    _recordSubscription = null;
    await _recorder.stop();
  }

  void dispose() {
    _recordSubscription?.cancel();
    _chunkController.close();
    _recorder.dispose();
  }
}
