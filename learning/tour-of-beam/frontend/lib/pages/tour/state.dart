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
import 'package:rate_limiter/rate_limiter.dart';

import '../../auth/notifier.dart';
import '../../cache/unit_content.dart';
import '../../cache/unit_progress.dart';
import '../../config.dart';
import '../../enums/save_code_status.dart';
import '../../enums/snippet_type.dart';
import '../../models/unit.dart';
import '../../models/unit_content.dart';
import '../../repositories/client/client.dart';
import '../../state.dart';
import 'controllers/content_tree.dart';
import 'controllers/unit.dart';
import 'path.dart';

class TourNotifier extends ChangeNotifier with PageStateMixin<void> {
  static const _saveUserCodeDebounceDuration = Duration(seconds: 2);
  Debounce? _saveCodeDebounced;

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
    _appNotifier.sdkId ??= initialSdkId;
    contentTreeController.addListener(_onUnitChanged);
    _unitContentCache.addListener(_onUnitChanged);
    _appNotifier.addListener(_onAppNotifierChanged);
    _authNotifier.addListener(_onAuthChanged);
    _saveCodeDebounced = _saveUserCode.debounced(
      _saveUserCodeDebounceDuration,
    );
    // setSdk creates snippetEditingController if it doesn't exist.
    playgroundController.setSdk(_appNotifier.sdk!);
    _listenToCurrentSnippetEditingController();
    _onUnitChanged();
  }

  @override
  void setStateMap(Map<String, dynamic> state) {
    super.setStateMap(state);
    _appNotifier.sdkId = state['sdkId'];
  }

  @override
  PagePath get path => TourPath(
        sdkId: contentTreeController.sdkId,
        treeIds: contentTreeController.treeIds,
      );

  String? get currentUnitId => currentUnitController?.unitId;
  UnitContentModel? get currentUnitContent => _currentUnitContent;

  bool get hasSolution => currentUnitContent?.solutionSnippetId != null;
  bool get hasSavedSnippet =>
      _unitProgressCache.getUnitSavedSnippetId(currentUnitId) != null;

  SnippetType _snippetType = SnippetType.original;
  SnippetType get snippetType => _snippetType;

  SaveCodeStatus _saveCodeStatus = SaveCodeStatus.saved;
  SaveCodeStatus get saveCodeStatus => _saveCodeStatus;
  set saveCodeStatus(SaveCodeStatus saveCodeStatus) {
    _saveCodeStatus = saveCodeStatus;
    notifyListeners();
  }

  Future<void> _onAuthChanged() async {
    await _updateUnitProgressAndSnippet();
    notifyListeners();
  }

  Future<void> _updateUnitProgressAndSnippet() async {
    await _unitProgressCache.updateUnitProgress();
    await _setCurrentSnippetFromType();
  }

  Future<void> _onAppNotifierChanged() async {
    final sdk = _appNotifier.sdk;
    if (sdk != null) {
      playgroundController.setSdk(sdk);
      _listenToCurrentSnippetEditingController();
      contentTreeController.sdkId = sdk.id;
      await _updateUnitProgressAndSnippet();
      notifyListeners();
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
  }

  Future<void> _setCurrentUnitContent(UnitContentModel? content) async {
    if (content == _currentUnitContent) {
      return;
    }

    _currentUnitContent = content;

    if (content == null) {
      return;
    }

    await _updateHasSavedSnippet();
    _setSnippetTypeSavedIfCan();
    await _setCurrentSnippetFromType();
    notifyListeners();
  }

  // Save user code.

  void _listenToCurrentSnippetEditingController() {
    playgroundController.snippetEditingController?.addListener(
      _onActiveFileControllerChanged,
    );
  }

  void _onActiveFileControllerChanged() {
    playgroundController
        .snippetEditingController?.activeFileController?.codeController
        .addListener(_onCodeChanged);
  }

  void _onCodeChanged() {
    final snippetEditingController =
        playgroundController.snippetEditingController!;
    final isCodeChanged =
        snippetEditingController.activeFileController?.isChanged ?? false;
    final snippetFiles = snippetEditingController.getFiles();

    final doSave = _authNotifier.isAuthenticated &&
        isCodeChanged &&
        currentUnitController != null &&
        snippetFiles.isNotEmpty;

    if (doSave) {
      _saveCodeDebounced?.call([], {
        const Symbol('sdkId'): currentUnitController!.sdkId,
        const Symbol('unitId'): currentUnitController!.unitId,
        const Symbol('snippetFiles'): snippetFiles,
      });
    }
  }

  Future<void> _saveUserCode({
    required String sdkId,
    required List<SnippetFile> snippetFiles,
    required String unitId,
  }) async {
    saveCodeStatus = SaveCodeStatus.saving;
    try {
      final client = GetIt.instance.get<TobClient>();
      await client.postUserCode(
        sdkId: sdkId,
        snippetFiles: snippetFiles,
        unitId: unitId,
      );
      await _updateHasSavedSnippet();
      saveCodeStatus = SaveCodeStatus.saved;
    } on Exception catch (e) {
      print(['Could not save code: ', e]);
      _saveCodeStatus = SaveCodeStatus.error;
    }
  }

  Future<void> showSnippetByType(SnippetType snippetType) async {
    if (snippetType == _snippetType) {
      return;
    }
    _snippetType = snippetType;
    // Get fresh saved snippet IDs.
    await _unitProgressCache.updateUnitProgress();
    await _setCurrentSnippetFromType();
    notifyListeners();
  }

  void _createCurrentUnitController(String sdkId, String unitId) {
    currentUnitController = UnitController(
      unitId: unitId,
      sdkId: sdkId,
    );
  }

  Future<void> _updateHasSavedSnippet() async {
    if (!hasSavedSnippet) {
      await _unitProgressCache.updateUnitProgress();
    }
  }

  void _setSnippetTypeSavedIfCan() {
    if (hasSavedSnippet) {
      _snippetType = SnippetType.saved;
    }
  }

  Future<void> _setCurrentSnippetFromType() async {
    final unit = _currentUnitContent;
    if (unit == null) {
      return;
    }

    final String? snippetId;
    switch (_snippetType) {
      case SnippetType.original:
        snippetId = unit.taskSnippetId;
        break;
      case SnippetType.saved:
        snippetId = _unitProgressCache.getUnitSavedSnippetId(currentUnitId) ??
            unit.taskSnippetId;
        break;
      case SnippetType.solution:
        snippetId = unit.solutionSnippetId;
        break;
    }
    await _setPlaygroundSnippet(snippetId);
  }

  // Playground controller.

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
    await playgroundController.examplesLoader.loadIfNew(
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
      playgroundController.examplesLoader.loadIfNew(
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
  Future<void> dispose() async {
    _unitContentCache.removeListener(_onUnitChanged);
    contentTreeController.removeListener(_onUnitChanged);
    _appNotifier.removeListener(_onAppNotifierChanged);
    _authNotifier.removeListener(_onAuthChanged);
    playgroundController.snippetEditingController
        ?.removeListener(_onActiveFileControllerChanged);
    // TODO(nausharipov): Use stream events https://github.com/apache/beam/issues/25185
    playgroundController
        .snippetEditingController?.activeFileController?.codeController
        .removeListener(_onCodeChanged);
    await super.dispose();
  }
}
