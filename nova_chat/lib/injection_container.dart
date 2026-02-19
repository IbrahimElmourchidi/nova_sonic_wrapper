import 'package:get_it/get_it.dart';

import 'core/audio/audio_player_service.dart';
import 'core/audio/audio_recorder_service.dart';
import 'data/datasources/socket_data_source.dart';
import 'data/repositories/chat_repository_impl.dart';
import 'domain/usecases/connect_usecase.dart';
import 'domain/usecases/disconnect_usecase.dart';
import 'domain/usecases/send_audio_usecase.dart';
import 'presentation/cubit/chat_cubit.dart';

final sl = GetIt.instance;

void initDependencies() {
  sl.registerFactory(() => DisconnectUseCase(ChatRepositoryImpl(SocketDataSource())));

  // Each ChatCubit gets one shared SocketDataSource → one ChatRepositoryImpl,
  // passed into every use-case so they all talk to the same socket.
  sl.registerFactory(() {
    final dataSource = SocketDataSource();
    final repository = ChatRepositoryImpl(dataSource);
    return ChatCubit(
      connectUseCase: ConnectUseCase(repository),
      sendAudioUseCase: SendAudioUseCase(repository),
      repository: repository,
      recorder: AudioRecorderService(),
      player: AudioPlayerService(),
    );
  });
}
