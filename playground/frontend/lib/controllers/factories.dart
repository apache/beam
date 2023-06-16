/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'dart:async';

import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../constants/params.dart';
import '../modules/messages/handlers/messages_debouncer.dart';
import '../modules/messages/handlers/messages_handler.dart';
import '../modules/messages/listeners/messages_listener.dart';

PlaygroundController createPlaygroundController(
  ExamplesLoadingDescriptor descriptor,
) {
  final exampleCache = ExampleCache(
    exampleRepository: GetIt.instance.get<ExampleRepository>(),
  );

  final controller = PlaygroundController(
    examplesLoader: ExamplesLoader(),
    exampleCache: exampleCache,
    codeClient: GetIt.instance.get<CodeClient>(),
  );

  unawaited(_loadExamples(controller, descriptor));

  final handler = MessagesDebouncer(
    handler: MessagesHandler(playgroundController: controller),
  );
  MessagesListener(handler: handler);

  return controller;
}

Future<void> _loadExamples(
  PlaygroundController controller,
  ExamplesLoadingDescriptor descriptor,
) async {
  try {
    await controller.examplesLoader.loadIfNew(descriptor);
  } on Exception catch (ex) {
    PlaygroundComponents.toastNotifier.addException(ex);

    controller.setEmptyIfNoSdk(
      descriptor.initialSdk ?? defaultSdk,
    );
  }
}
