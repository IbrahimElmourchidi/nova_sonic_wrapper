class VoiceOption {
  final String name;
  final String voiceId;
  final String systemPrompt;

  const VoiceOption({
    required this.name,
    required this.voiceId,
    required this.systemPrompt,
  });
}

class VoiceOptions {
  VoiceOptions._();

  static const List<VoiceOption> all = [
    VoiceOption(
      name: 'Greta',
      voiceId: 'greta',
      systemPrompt:
          'You are Greta, a friendly German language teacher. '
          'The conversation is just starting. '
          'Greet the user warmly in German right now and ask how you can help them learn German today. '
          'Keep responses short, two or three sentences maximum.',
    ),
    VoiceOption(
      name: 'Lennart',
      voiceId: 'lennart',
      systemPrompt:
          'You are Lennart, a friendly German language teacher. '
          'The conversation is just starting. '
          'Greet the user warmly in German right now and ask how you can help them learn German today. '
          'Keep responses short, two or three sentences maximum.',
    ),
  ];
}
