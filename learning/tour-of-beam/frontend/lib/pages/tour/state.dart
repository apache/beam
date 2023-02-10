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

import 'package:app_state/app_state.dart';
import 'package:flutter/widgets.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../auth/notifier.dart';
import '../../cache/unit_content.dart';
import '../../cache/unit_progress.dart';
import '../../config.dart';
import '../../models/unit.dart';
import '../../models/unit_content.dart';
import '../../state.dart';
import 'controllers/content_tree.dart';
import 'controllers/unit.dart';
import 'path.dart';

class TourNotifier extends ChangeNotifier with PageStateMixin<void> {
  final ContentTreeController contentTreeController;
  final PlaygroundController playgroundController;
  UnitController? currentUnitController;
  final _appNotifier = GetIt.instance.get<AppNotifier>();
  final _authNotifier = GetIt.instance.get<AuthNotifier>();
  final _unitContentCache = GetIt.instance.get<UnitContentCache>();
  final _unitProgressCache = GetIt.instance.get<UnitProgressCache>();
  UnitContentModel? _currentUnitContent;

  TourNotifier({
    required String initialSdkId,
    List<String> initialTreeIds = const [],
  })  : contentTreeController = ContentTreeController(
          initialSdkId: initialSdkId,
          initialTreeIds: initialTreeIds,
        ),
        playgroundController = _createPlaygroundController(initialSdkId) {
    contentTreeController.addListener(_onUnitChanged);
    _unitContentCache.addListener(_onUnitChanged);
    _appNotifier.addListener(_onAppNotifierChanged);
    _authNotifier.addListener(_onUnitProgressChanged);
    _onUnitChanged();
  }

  @override
  PagePath get path => TourPath(
        sdkId: contentTreeController.sdkId,
        treeIds: contentTreeController.treeIds,
      );

  String? get currentUnitId => currentUnitController?.unitId;
  UnitContentModel? get currentUnitContent => _currentUnitContent;
  bool get doesCurrentUnitHaveSolution =>
      currentUnitContent?.solutionSnippetId != null;
  bool _isShowingSolution = false;
  bool get isShowingSolution => _isShowingSolution;

  void toggleShowingSolution() {
    if (doesCurrentUnitHaveSolution) {
      _isShowingSolution = !_isShowingSolution;

      final snippetId = _isShowingSolution
          ? _currentUnitContent?.solutionSnippetId
          : _currentUnitContent?.taskSnippetId;
      if (snippetId != null) {
        // TODO(nausharipov): store/recover
        unawaited(_setPlaygroundSnippet(snippetId));
      }

      notifyListeners();
    }
  }

  void _createCurrentUnitController(String sdkId, String unitId) {
    currentUnitController = UnitController(
      unitId: unitId,
      sdkId: sdkId,
    );
  }

  Future<void> _onUnitProgressChanged() async {
    await _unitProgressCache.updateCompletedUnits();
  }

  void _onAppNotifierChanged() {
    final sdkId = _appNotifier.sdkId;
    if (sdkId != null) {
      playgroundController.setSdk(Sdk.parseOrCreate(sdkId));
      contentTreeController.sdkId = sdkId;
      _onUnitProgressChanged();
    }
  }

  void _onUnitChanged() {
    emitPathChanged();
    final currentNode = contentTreeController.currentNode;
    if (currentNode is UnitModel) {
      final sdk = contentTreeController.sdk;
      final content = _unitContentCache.getUnitContent(
        sdk.id,
        currentNode.id,
      );
      _createCurrentUnitController(contentTreeController.sdkId, currentNode.id);
      _setCurrentUnitContent(content);
    } else {
      _emptyPlayground();
    }

    notifyListeners();
  }

  Future<void> _setCurrentUnitContent(UnitContentModel? content) async {
    if (content == _currentUnitContent) {
      return;
    }

    _currentUnitContent = content;

    if (content == null) {
      return;
    }
    final taskSnippetId = content.taskSnippetId;
    await _setPlaygroundSnippet(taskSnippetId);
    _isShowingSolution = false;
  }

  Future<void> _setPlaygroundSnippet(String? snippetId) async {
    if (snippetId == null) {
      await _emptyPlayground();
      return;
    }

    final selectedSdk = _appNotifier.sdk;
    if (selectedSdk != null) {
      await playgroundController.examplesLoader.load(
        ExamplesLoadingDescriptor(
          descriptors: [
            UserSharedExampleLoadingDescriptor(
              sdk: selectedSdk,
              snippetId: snippetId,
            ),
          ],
        ),
      );
    }
  }

  // TODO(alexeyinkin): Hide the entire right pane instead.
  Future<void> _emptyPlayground() async {
    await playgroundController.examplesLoader.load(
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
    );

    final playgroundController = PlaygroundController(
      codeRepository: codeRepository,
      exampleCache: exampleCache,
      examplesLoader: ExamplesLoader(),
    );

    unawaited(
      playgroundController.examplesLoader.load(
        ExamplesLoadingDescriptor(
          descriptors: [
            EmptyExampleLoadingDescriptor(sdk: Sdk.parseOrCreate(initialSdkId)),
          ],
        ),
      ),
    );

    return playgroundController;
  }

  @override
  void dispose() {
    _unitContentCache.removeListener(_onUnitChanged);
    contentTreeController.removeListener(_onUnitChanged);
    _appNotifier.removeListener(_onAppNotifierChanged);
    _authNotifier.removeListener(_onUnitProgressChanged);
    super.dispose();
  }
}
