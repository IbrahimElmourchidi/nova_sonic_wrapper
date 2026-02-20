class AppConstants {
  AppConstants._();
  static const String serverUrl = 'http://10.0.2.2:3000';
  // static const String serverUrl =
  //     'https://nova-sonic-server-673314104404.europe-west3.run.app';

  static const int inputSampleRate = 16000;
  static const int outputSampleRate = 24000;
  static const int sampleSizeBits = 16;
  static const int channelCount = 1;
  static const int chunkDurationMs = 100;

  static const String systemPrompt =
      'Du bist ein freundlicher Deutschlehrer. '
      'Begrüße den Benutzer auf Deutsch und hilf ihm, Deutsch zu lernen. '
      'Führe ein natürliches Gespräch auf Deutsch. '
      'Halte deine Antworten kurz, normalerweise zwei oder drei Sätze.';

  static const String appTitle = 'General Health';
}
