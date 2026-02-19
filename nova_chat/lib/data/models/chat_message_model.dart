import '../../domain/entities/chat_message.dart';

class ChatMessageModel extends ChatMessage {
  const ChatMessageModel({
    required super.id,
    required super.content,
    required super.role,
    required super.timestamp,
  });

  factory ChatMessageModel.fromTextOutput(Map<String, dynamic> data) {
    final roleStr = (data['role'] as String? ?? '').toUpperCase();
    return ChatMessageModel(
      id: DateTime.now().microsecondsSinceEpoch.toString(),
      content: data['content'] as String? ?? '',
      role: roleStr == 'USER' ? MessageRole.user : MessageRole.assistant,
      timestamp: DateTime.now(),
    );
  }
}
