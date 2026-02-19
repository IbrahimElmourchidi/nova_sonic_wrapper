import 'package:equatable/equatable.dart';

import '../../domain/entities/chat_message.dart';
import '../../domain/enums/connection_status.dart';
import '../../domain/enums/session_status.dart';

class ChatState extends Equatable {
  final ConnectionStatus connectionStatus;
  final SessionStatus sessionStatus;
  final List<ChatMessage> messages;
  final bool isRecording;
  final String? errorMessage;

  const ChatState({
    this.connectionStatus = ConnectionStatus.disconnected,
    this.sessionStatus = SessionStatus.idle,
    this.messages = const [],
    this.isRecording = false,
    this.errorMessage,
  });

  ChatState copyWith({
    ConnectionStatus? connectionStatus,
    SessionStatus? sessionStatus,
    List<ChatMessage>? messages,
    bool? isRecording,
    String? errorMessage,
  }) {
    return ChatState(
      connectionStatus: connectionStatus ?? this.connectionStatus,
      sessionStatus: sessionStatus ?? this.sessionStatus,
      messages: messages ?? this.messages,
      isRecording: isRecording ?? this.isRecording,
      errorMessage: errorMessage,
    );
  }

  @override
  List<Object?> get props => [
        connectionStatus,
        sessionStatus,
        messages,
        isRecording,
        errorMessage,
      ];
}
