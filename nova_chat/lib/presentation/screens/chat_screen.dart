import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';

import '../../core/constants.dart';
import '../../domain/enums/connection_status.dart';
import '../../domain/enums/session_status.dart';
import '../cubit/chat_cubit.dart';
import '../cubit/chat_state.dart';
import '../widgets/message_bubble.dart';
import '../widgets/recording_indicator.dart';

class ChatScreen extends StatefulWidget {
  const ChatScreen({super.key});

  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  final ScrollController _scrollController = ScrollController();

  @override
  void initState() {
    super.initState();
    context.read<ChatCubit>().autoConnect();
  }

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          _scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 300),
          curve: Curves.easeOut,
        );
      }
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        leading: IconButton(
          icon: const Icon(Icons.arrow_back),
          onPressed: () {},
        ),
        title: const Text(AppConstants.appTitle),
        centerTitle: true,
        actions: [
          BlocBuilder<ChatCubit, ChatState>(
            buildWhen: (prev, curr) =>
                prev.connectionStatus != curr.connectionStatus,
            builder: (context, state) {
              final color = switch (state.connectionStatus) {
                ConnectionStatus.connected => Colors.green,
                ConnectionStatus.connecting => Colors.orange,
                ConnectionStatus.error => Colors.red,
                ConnectionStatus.disconnected => Colors.grey,
              };
              return Padding(
                padding: const EdgeInsets.only(right: 16),
                child: Icon(Icons.circle, size: 12, color: color),
              );
            },
          ),
        ],
      ),
      body: Column(
        children: [
          Expanded(
            child: BlocConsumer<ChatCubit, ChatState>(
              listenWhen: (prev, curr) =>
                  prev.messages.length != curr.messages.length,
              listener: (context, state) => _scrollToBottom(),
              buildWhen: (prev, curr) => prev.messages != curr.messages ||
                  prev.sessionStatus != curr.sessionStatus,
              builder: (context, state) {
                if (state.messages.isEmpty) {
                  return Center(
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        if (state.sessionStatus == SessionStatus.idle ||
                            state.sessionStatus == SessionStatus.initializing ||
                            state.sessionStatus == SessionStatus.ready)
                          const CircularProgressIndicator(),
                        const SizedBox(height: 16),
                        Text(
                          _statusText(state),
                          style: const TextStyle(color: Colors.grey),
                        ),
                      ],
                    ),
                  );
                }
                return ListView.builder(
                  controller: _scrollController,
                  padding:
                      const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
                  itemCount: state.messages.length,
                  itemBuilder: (context, index) {
                    return MessageBubble(message: state.messages[index]);
                  },
                );
              },
            ),
          ),
          _buildBottomBar(),
        ],
      ),
    );
  }

  String _statusText(ChatState state) {
    return switch (state.connectionStatus) {
      ConnectionStatus.disconnected => 'Disconnected',
      ConnectionStatus.connecting => 'Connecting...',
      ConnectionStatus.error => state.errorMessage ?? 'Connection error',
      ConnectionStatus.connected => switch (state.sessionStatus) {
          SessionStatus.initializing => 'Initializing session...',
          SessionStatus.ready => 'Waiting for AI...',
          SessionStatus.aiSpeaking => 'AI is speaking...',
          SessionStatus.processing => 'Processing...',
          _ => 'Connected',
        },
    };
  }

  Widget _buildBottomBar() {
    return BlocBuilder<ChatCubit, ChatState>(
      builder: (context, state) {
        final canRecord = state.connectionStatus == ConnectionStatus.connected &&
            !_isBusy(state);

        return Container(
          padding: const EdgeInsets.symmetric(vertical: 16, horizontal: 24),
          decoration: BoxDecoration(
            color: Theme.of(context).colorScheme.surface,
            boxShadow: [
              BoxShadow(
                color: Colors.black.withValues(alpha: 0.1),
                blurRadius: 8,
                offset: const Offset(0, -2),
              ),
            ],
          ),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              if (state.isRecording) const RecordingIndicator(),
              if (state.isRecording) const SizedBox(height: 8),
              GestureDetector(
                onTap: canRecord || state.isRecording
                    ? () => context.read<ChatCubit>().toggleRecording()
                    : null,
                child: Container(
                  width: 64,
                  height: 64,
                  decoration: BoxDecoration(
                    shape: BoxShape.circle,
                    color: state.isRecording
                        ? Colors.red
                        : canRecord
                            ? Theme.of(context).colorScheme.primary
                            : Colors.grey,
                    boxShadow: state.isRecording
                        ? [
                            BoxShadow(
                              color: Colors.red.withValues(alpha: 0.4),
                              blurRadius: 16,
                              spreadRadius: 2,
                            ),
                          ]
                        : null,
                  ),
                  child: Icon(
                    state.isRecording ? Icons.stop : Icons.mic,
                    color: Colors.white,
                    size: 32,
                  ),
                ),
              ),
              const SizedBox(height: 4),
              Text(
                state.isRecording
                    ? 'Tap to stop'
                    : canRecord
                        ? 'Tap to speak'
                        : _statusText(state),
                style: TextStyle(
                  fontSize: 12,
                  color: Colors.grey[600],
                ),
              ),
            ],
          ),
        );
      },
    );
  }

  bool _isBusy(ChatState state) {
    return state.sessionStatus == SessionStatus.initializing ||
        state.sessionStatus == SessionStatus.processing ||
        state.sessionStatus == SessionStatus.aiSpeaking;
  }

  @override
  void dispose() {
    _scrollController.dispose();
    super.dispose();
  }
}
