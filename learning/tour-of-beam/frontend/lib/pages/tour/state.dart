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
import 'package:flutter/widgets.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../cache/unit_content.dart';
import '../../config.dart';
import '../../models/unit.dart';
import '../../models/unit_content.dart';
import 'controllers/content_tree.dart';
import 'path.dart';

class TourNotifier extends ChangeNotifier with PageStateMixin<void> {
  final ContentTreeController contentTreeController;
  final PlaygroundController playgroundController;
  final _unitContentCache = GetIt.instance.get<UnitContentCache>();
  UnitContentModel? _currentUnitContent;

  TourNotifier({
    required String initialSdkId,
    List<String> initialTreeIds = const [],
  })  : contentTreeController = ContentTreeController(
          initialSdkId: initialSdkId,
          initialTreeIds: initialTreeIds,
        ),
        playgroundController = _createPlaygroundController(initialSdkId) {
    contentTreeController.addListener(_onChanged);
    _unitContentCache.addListener(_onChanged);
    _onChanged();
  }

  @override
  PagePath get path => TourPath(
        sdkId: contentTreeController.sdkId,
        treeIds: contentTreeController.treeIds,
      );

  void _onChanged() {
    emitPathChanged();
    final currentNode = contentTreeController.currentNode;
    if (currentNode is UnitModel) {
      final content = _unitContentCache.getUnitContent(
        contentTreeController.sdkId,
        currentNode.id,
      );

      _setCurrentUnitContent(content);
    } else {
      _emptyPlayground();
    }

    notifyListeners();
  }

  UnitContentModel? get currentUnitContent => _currentUnitContent;

  void _setCurrentUnitContent(UnitContentModel? content) {
    if (content == _currentUnitContent) {
      return;
    }

    _currentUnitContent = content;

    if (content == null) {
      return;
    }

    final taskSnippetId = content.taskSnippetId;
    if (taskSnippetId == null) {
      _emptyPlayground();
      return;
    }

    playgroundController.examplesLoader.load(
      ExamplesLoadingDescriptor(
        descriptors: [
          UserSharedExampleLoadingDescriptor(snippetId: taskSnippetId),
        ],
      ),
    );
  }

  // TODO(alexeyinkin): Hide the entire right pane instead.
  void _emptyPlayground() {
    playgroundController.examplesLoader.load(
      ExamplesLoadingDescriptor(
        descriptors: [
          EmptyExampleLoadingDescriptor(sdk: contentTreeController.sdk),
        ],
      ),
    );
  }

  static PlaygroundController _createPlaygroundController(String initialSdkId) {
    final exampleRepository = ExampleRepository(
      client: GrpcExampleClient(url: kApiClientURL),
    );

    final codeRepository = CodeRepository(
      client: GrpcCodeClient(
        url: kApiClientURL,
        runnerUrlsById: {
          Sdk.java.id: kApiJavaClientURL,
          Sdk.go.id: kApiGoClientURL,
          Sdk.python.id: kApiPythonClientURL,
          Sdk.scio.id: kApiScioClientURL,
        },
      ),
    );

    final exampleCache = ExampleCache(
      exampleRepository: exampleRepository,
      hasCatalog: false,
    );

    final playgroundController = PlaygroundController(
      codeRepository: codeRepository,
      exampleCache: exampleCache,
      examplesLoader: ExamplesLoader(),
    );

    playgroundController.examplesLoader.load(
      ExamplesLoadingDescriptor(
        descriptors: [
          EmptyExampleLoadingDescriptor(sdk: Sdk.parseOrCreate(initialSdkId)),
        ],
      ),
    );

    return playgroundController;
  }

  @override
  void dispose() {
    _unitContentCache.removeListener(_onChanged);
    contentTreeController.removeListener(_onChanged);
    super.dispose();
  }
}
