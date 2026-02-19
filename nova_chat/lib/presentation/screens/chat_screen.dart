import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';

import '../../domain/entities/voice_option.dart';
import '../../domain/enums/connection_status.dart';
import '../../domain/enums/session_status.dart';
import '../cubit/chat_cubit.dart';
import '../cubit/chat_state.dart';
import '../widgets/message_bubble.dart';

class ChatScreen extends StatefulWidget {
  final VoiceOption voice;

  const ChatScreen({super.key, required this.voice});

  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  final ScrollController _scrollController = ScrollController();

  @override
  void initState() {
    super.initState();
    context.read<ChatCubit>().autoConnect(widget.voice);
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
    return PopScope(
      // Allow back navigation; cubit.close() is called automatically when the
      // BlocProvider created in HomeScreen is removed from the widget tree.
      canPop: true,
      child: Scaffold(
        appBar: AppBar(
          leading: IconButton(
            icon: const Icon(Icons.arrow_back),
            onPressed: () => Navigator.of(context).pop(),
          ),
          title: Text(widget.voice.name),
          centerTitle: true,
          actions: [
            BlocBuilder<ChatCubit, ChatState>(
              buildWhen: (p, c) => p.sessionStatus != c.sessionStatus,
              builder: (context, state) {
                final (icon, color) = switch (state.sessionStatus) {
                  SessionStatus.recording => (Icons.mic, Colors.red),
                  SessionStatus.aiSpeaking => (Icons.volume_up, Colors.blue),
                  SessionStatus.processing => (Icons.hourglass_top, Colors.orange),
                  _ => (Icons.mic_off, Colors.grey),
                };
                return Padding(
                  padding: const EdgeInsets.only(right: 16),
                  child: Icon(icon, size: 20, color: color),
                );
              },
            ),
          ],
        ),
        body: Stack(
          children: [
            // ── Chat messages ────────────────────────────────────────────
            BlocConsumer<ChatCubit, ChatState>(
              listenWhen: (p, c) => p.messages.length != c.messages.length,
              listener: (_, _) => _scrollToBottom(),
              buildWhen: (p, c) => p.messages != c.messages,
              builder: (context, state) {
                if (state.messages.isEmpty) {
                  return const SizedBox.shrink();
                }
                return ListView.builder(
                  controller: _scrollController,
                  padding: const EdgeInsets.fromLTRB(16, 8, 16, 24),
                  itemCount: state.messages.length,
                  itemBuilder: (context, index) =>
                      MessageBubble(message: state.messages[index]),
                );
              },
            ),

            // ── Loading overlay (shown until first message arrives) ──────
            BlocBuilder<ChatCubit, ChatState>(
              buildWhen: (p, c) =>
                  p.isLoading != c.isLoading ||
                  p.connectionStatus != c.connectionStatus ||
                  p.sessionStatus != c.sessionStatus,
              builder: (context, state) {
                if (!state.isLoading) return const SizedBox.shrink();

                return Container(
                  color: Theme.of(context).colorScheme.surface,
                  child: Center(
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        const CircularProgressIndicator(),
                        const SizedBox(height: 24),
                        Text(
                          _loadingLabel(state),
                          style: TextStyle(
                            color: Colors.grey[600],
                            fontSize: 15,
                          ),
                        ),
                      ],
                    ),
                  ),
                );
              },
            ),
          ],
        ),
      ),
    );
  }

  String _loadingLabel(ChatState state) {
    if (state.connectionStatus == ConnectionStatus.connecting) {
      return 'Connecting...';
    }
    if (state.connectionStatus == ConnectionStatus.error) {
      return state.errorMessage ?? 'Connection error';
    }
    return switch (state.sessionStatus) {
      SessionStatus.initializing => 'Initializing session...',
      SessionStatus.ready => 'Starting conversation...',
      SessionStatus.aiSpeaking => 'AI is speaking...',
      _ => 'Please wait...',
    };
  }

  @override
  void dispose() {
    _scrollController.dispose();
    super.dispose();
  }
}
