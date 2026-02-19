import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';

import 'injection_container.dart';
import 'presentation/cubit/chat_cubit.dart';
import 'presentation/screens/chat_screen.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  initDependencies();
  runApp(const NovaChatApp());
}

class NovaChatApp extends StatelessWidget {
  const NovaChatApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Nova Chat',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(
          seedColor: const Color(0xFF5C6BC0),
          brightness: Brightness.light,
        ),
        useMaterial3: true,
      ),
      home: BlocProvider(
        create: (_) => sl<ChatCubit>(),
        child: const ChatScreen(),
      ),
    );
  }
}
