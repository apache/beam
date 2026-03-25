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

import 'package:app_state/app_state.dart';
import 'package:flutter/foundation.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../controllers/factories.dart';
import '../../modules/examples/models/example_loading_descriptors/no_url_example_loading_descriptor.dart';
import '../../services/analytics/events/loaded.dart';
import '../enum.dart';
import 'path.dart';

const _cutUrlDescriptors = {
  ContentExampleLoadingDescriptor,
};

/// The main state object behind EmbeddedPlaygroundScreen.
class EmbeddedPlaygroundNotifier extends ChangeNotifier
    with PageStateMixin<void> {
  final PlaygroundController playgroundController;
  final windowCloseNotifier = GetIt.instance.get<WindowCloseNotifier>();
  final bool isEditable;

  EmbeddedPlaygroundNotifier({
    required ExamplesLoadingDescriptor initialDescriptor,
    required this.isEditable,
  }) : playgroundController = createPlaygroundController(initialDescriptor) {
    playgroundController.addListener(_onPlaygroundControllerChanged);
    windowCloseNotifier.addListener(dispose);

    // This adds the layout to all events sent from now on.
    // Ideally we want to set/unset that when this page becomes topmost
    // or loses that, but we have no API for that so far.
    // See https://github.com/alexeyinkin/flutter-app-state/issues/23
    // Anyway we do not switch between embedded/standalone at runtime.
    PlaygroundComponents.analyticsService.defaultEventParameters = {
      EventParams.app: PagesEnum.embeddedPlayground.name,
    };

    PlaygroundComponents.analyticsService.sendUnawaited(
      LoadedAnalyticsEvent(
        sdk: initialDescriptor.initialSnippetSdk,
        snippet: initialDescriptor.initialSnippetToken,
      ),
    );
  }

  void _onPlaygroundControllerChanged() {
    emitPathChanged();
  }

  @override
  PagePath get path {
    return EmbeddedPlaygroundSingleFirstPath(
      descriptor: _getExampleLoadingDescriptor(),
      isEditable: isEditable,
      multipleDescriptor: playgroundController.getLoadingDescriptor(),
    );
  }

  ExampleLoadingDescriptor _getExampleLoadingDescriptor() {
    final snippetController = playgroundController.snippetEditingController;
    if (snippetController == null) {
      return const NoUrlExampleLoadingDescriptor();
    }

    final descriptor = snippetController.getLoadingDescriptor();

    return _cutUrlDescriptors.contains(descriptor.runtimeType)
        ? const NoUrlExampleLoadingDescriptor()
        : descriptor;
  }

  /// Cancels a possible run and frees other resources.
  @override
  Future<void> dispose() async {
    playgroundController.codeRunner.cancelRun();
    playgroundController.dispose();
    windowCloseNotifier.removeListener(dispose);
    super.dispose();
  }
}
