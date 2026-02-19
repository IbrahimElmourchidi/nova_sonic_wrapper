enum MessageRole { user, assistant }

class ChatMessage {
  final String id;
  final String content;
  final MessageRole role;
  final DateTime timestamp;

  const ChatMessage({
    required this.id,
    required this.content,
    required this.role,
    required this.timestamp,
  });
}
