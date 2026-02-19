import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

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

    _recordSubscription = stream.listen((data) {
      final base64Chunk = base64Encode(Uint8List.fromList(data));
      _chunkController.add(base64Chunk);
    });
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
