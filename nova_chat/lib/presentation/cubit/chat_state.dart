import 'package:equatable/equatable.dart';

import '../../domain/entities/chat_message.dart';
import '../../domain/enums/connection_status.dart';
import '../../domain/enums/session_status.dart';

class ChatState extends Equatable {
  final ConnectionStatus connectionStatus;
  final SessionStatus sessionStatus;
  final List<ChatMessage> messages;
  // true while connecting/requesting permissions before the first AI message
  final bool isLoading;
  final String? errorMessage;

  const ChatState({
    this.connectionStatus = ConnectionStatus.disconnected,
    this.sessionStatus = SessionStatus.idle,
    this.messages = const [],
    this.isLoading = true,
    this.errorMessage,
  });

  ChatState copyWith({
    ConnectionStatus? connectionStatus,
    SessionStatus? sessionStatus,
    List<ChatMessage>? messages,
    bool? isLoading,
    String? errorMessage,
  }) {
    return ChatState(
      connectionStatus: connectionStatus ?? this.connectionStatus,
      sessionStatus: sessionStatus ?? this.sessionStatus,
      messages: messages ?? this.messages,
      isLoading: isLoading ?? this.isLoading,
      errorMessage: errorMessage,
    );
  }

  @override
  List<Object?> get props => [
        connectionStatus,
        sessionStatus,
        messages,
        isLoading,
        errorMessage,
      ];
}
