import 'package:get_it/get_it.dart';

import 'core/audio/audio_player_service.dart';
import 'core/audio/audio_recorder_service.dart';
import 'data/datasources/socket_data_source.dart';
import 'data/repositories/chat_repository_impl.dart';
import 'domain/repositories/i_chat_repository.dart';
import 'domain/usecases/connect_usecase.dart';
import 'domain/usecases/disconnect_usecase.dart';
import 'domain/usecases/send_audio_usecase.dart';
import 'presentation/cubit/chat_cubit.dart';

final sl = GetIt.instance;

void initDependencies() {
  // Data sources
  sl.registerLazySingleton(() => SocketDataSource());

  // Repository
  sl.registerLazySingleton<IChatRepository>(
    () => ChatRepositoryImpl(sl<SocketDataSource>()),
  );

  // Use cases
  sl.registerFactory(() => ConnectUseCase(sl()));
  sl.registerFactory(() => SendAudioUseCase(sl()));
  sl.registerFactory(() => DisconnectUseCase(sl()));

  // Audio services
  sl.registerLazySingleton(() => AudioRecorderService());
  sl.registerLazySingleton(() => AudioPlayerService());

  // Cubit
  sl.registerFactory(() => ChatCubit(
        connectUseCase: sl(),
        sendAudioUseCase: sl(),
        repository: sl(),
        recorder: sl(),
        player: sl(),
      ));
}
