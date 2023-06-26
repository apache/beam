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
import '../../enums/save_code_status.dart';
import '../../enums/snippet_type.dart';
import '../../models/event_context.dart';
import '../../models/node.dart';
import '../../models/unit.dart';
import '../../models/unit_content.dart';
import '../../services/analytics/events/unit_closed.dart';
import '../../services/analytics/events/unit_opened.dart';
import '../../state.dart';
import 'controllers/content_tree.dart';
import 'path.dart';

class TourNotifier extends ChangeNotifier with PageStateMixin<void> {
  static const _saveUserCodeDebounceDuration = Duration(seconds: 2);
  Debounce? _saveCodeDebounced;

  final ContentTreeController contentTreeController;
  final PlaygroundController playgroundController;
  final _appNotifier = GetIt.instance.get<AppNotifier>();
  final _authNotifier = GetIt.instance.get<AuthNotifier>();
  final _unitContentCache = GetIt.instance.get<UnitContentCache>();
  final _unitProgressCache = GetIt.instance.get<UnitProgressCache>();
  UnitContentModel? _currentUnitContent;
  bool _isPlaygroundShown = false;
  DateTime? _currentUnitOpenedAt;

  TobEventContext _tobEventContext = TobEventContext.empty;
  TobEventContext get tobEventContext => _tobEventContext;

  TourNotifier({
    List<String> initialBreadcrumbIds = const [],
  })  : contentTreeController = ContentTreeController(
          initialBreadcrumbIds: initialBreadcrumbIds,
        ),
        playgroundController = _createPlaygroundController() {
    contentTreeController.addListener(_onUnitChanged);
    _appNotifier.addListener(_onAppNotifierChanged);
    _authNotifier.addListener(_onAuthChanged);
    _saveCodeDebounced = _saveCode.debounced(
      _saveUserCodeDebounceDuration,
    );
    // setSdk creates snippetEditingController if it doesn't exist.
    playgroundController.setSdk(currentSdk);
    _listenToCurrentSnippetEditingController();
    unawaited(_onUnitChanged());
  }

  @override
  void setStateMap(Map<String, dynamic> state) {
    super.setStateMap(state);
    _appNotifier.sdk = Sdk.parseOrCreate(state['sdkId']);
  }

  @override
  PagePath get path => TourPath(
        sdkId: GetIt.instance.get<AppNotifier>().sdk.id,
        breadcrumbIds: contentTreeController.breadcrumbIds,
      );

  bool get isAuthenticated => _authNotifier.isAuthenticated;

  Sdk get currentSdk => _appNotifier.sdk;
  String? get currentUnitId => _currentUnitContent?.id;
  UnitContentModel? get currentUnitContent => _currentUnitContent;

  bool get hasSolution => _currentUnitContent?.solutionSnippetId != null;
  bool get isCodeSaved => _unitProgressCache.hasSavedSnippet(currentUnitId);

  SnippetType _snippetType = SnippetType.original;
  SnippetType get snippetType => _snippetType;

  bool _isLoadingSnippet = false;

  SaveCodeStatus _saveCodeStatus = SaveCodeStatus.saved;
  SaveCodeStatus get saveCodeStatus => _saveCodeStatus;
  set saveCodeStatus(SaveCodeStatus saveCodeStatus) {
    _saveCodeStatus = saveCodeStatus;
    notifyListeners();
  }

  bool get isPlaygroundShown => _isPlaygroundShown;
  bool get isSnippetLoading => _isLoadingSnippet;

  Future<void> _onAuthChanged() async {
    await _unitProgressCache.loadUnitProgress(currentSdk);
    // The local changes are preserved if the user signs in.
    if (_snippetType != SnippetType.saved || !isAuthenticated) {
      _trySetSnippetType(SnippetType.saved);
      await _loadSnippetByType();
    }
    notifyListeners();
  }

  Future<void> _onAppNotifierChanged() async {
    playgroundController.setSdk(currentSdk);
    _listenToCurrentSnippetEditingController();
    final currentNode = contentTreeController.currentNode;
    if (currentNode != null) {
      await _loadUnit(currentNode);
    }
  }

  Future<void> _onUnitChanged() async {
    emitPathChanged();
    final currentNode = contentTreeController.currentNode;
    if (currentNode is! UnitModel) {
      await _emptyPlayground();
    } else {
      await _loadUnit(currentNode);
    }
    notifyListeners();
  }

  Future<void> _loadUnit(NodeModel node) async {
    _setUnitContent(null);
    notifyListeners();

    final content = await _unitContentCache.getUnitContent(
      currentSdk.id,
      node.id,
    );

    _setUnitContent(content);
    await _unitProgressCache.loadUnitProgress(currentSdk);

    if (content != _currentUnitContent) {
      return; // Changed while waiting.
    }

    _trySetSnippetType(SnippetType.saved);
    await _loadSnippetByType();
  }

  void _setUnitContent(UnitContentModel? unitContent) {
    if (unitContent == _currentUnitContent) {
      return;
    }

    if (_currentUnitOpenedAt != null && _currentUnitContent != null) {
      _trackUnitClosed();
    }

    if (_currentUnitContent != null) {
      _isPlaygroundShown = _currentUnitContent!.taskSnippetId != null;
    }
    _currentUnitContent = unitContent;

    if (_currentUnitContent != null) {
      _trackUnitOpened(_currentUnitContent!.id);
    }
  }

  void _trackUnitClosed() {
    PlaygroundComponents.analyticsService.sendUnawaited(
      UnitClosedTobAnalyticsEvent(
        tobContext: _tobEventContext,
        timeSpent: DateTime.now().difference(_currentUnitOpenedAt!),
      ),
    );
  }

  void _trackUnitOpened(String unitId) {
    _currentUnitOpenedAt = DateTime.now();
    _tobEventContext = TobEventContext(
      sdkId: currentSdk.id,
      unitId: unitId,
    );
    playgroundController
        .requireSnippetEditingController()
        .setDefaultEventParams(_tobEventContext.toJson());
    PlaygroundComponents.analyticsService.sendUnawaited(
      UnitOpenedTobAnalyticsEvent(
        tobContext: _tobEventContext,
      ),
    );
  }

  // Save user code.

  Future<void> showSnippetByType(SnippetType snippetType) async {
    _trySetSnippetType(snippetType);
    await _loadSnippetByType();
    notifyListeners();
  }

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

    final doSave = _isSnippetTypeSavable() &&
        isCodeChanged &&
        _currentUnitContent != null &&
        snippetFiles.isNotEmpty;

    if (doSave) {
      // Snapshot of sdk and unitId at the moment of editing.
      final sdk = currentSdk;
      final unitId = currentUnitId;
      _saveCodeDebounced?.call([], {
        const Symbol('sdk'): sdk,
        const Symbol('snippetFiles'): snippetFiles,
        const Symbol('unitId'): unitId,
      });
    }
  }

  bool _isSnippetTypeSavable() {
    return snippetType != SnippetType.solution;
  }

  Future<void> _saveCode({
    required Sdk sdk,
    required List<SnippetFile> snippetFiles,
    required String unitId,
  }) async {
    saveCodeStatus = SaveCodeStatus.saving;
    try {
      await _unitProgressCache.saveSnippet(
        sdk: sdk,
        snippetFiles: snippetFiles,
        snippetType: _snippetType,
        unitId: unitId,
      );
      saveCodeStatus = SaveCodeStatus.saved;
      await _unitProgressCache.loadUnitProgress(currentSdk);
      _trySetSnippetType(SnippetType.saved);
    } on Exception catch (e) {
      print(['Could not save code: ', e]);
      _saveCodeStatus = SaveCodeStatus.error;
    }
  }

  void _trySetSnippetType(SnippetType snippetType) {
    if (snippetType == SnippetType.saved && !isCodeSaved) {
      _snippetType = SnippetType.original;
    } else {
      _snippetType = snippetType;
    }
    notifyListeners();
  }

  Future<void> _loadSnippetByType() async {
    final ExampleLoadingDescriptor descriptor;
    switch (_snippetType) {
      case SnippetType.original:
        descriptor = _getStandardOrEmptyDescriptor(
          currentSdk,
          _currentUnitContent!.taskSnippetId,
        );
        break;
      case SnippetType.saved:
        descriptor = await _unitProgressCache.getSavedDescriptor(
          sdk: currentSdk,
          unitId: _currentUnitContent!.id,
        );
        break;
      case SnippetType.solution:
        descriptor = _getStandardOrEmptyDescriptor(
          currentSdk,
          _currentUnitContent!.solutionSnippetId,
        );
        break;
    }

    try {
      _isLoadingSnippet = true;
      notifyListeners();

      await playgroundController.examplesLoader.load(
        ExamplesLoadingDescriptor(
          descriptors: [
            descriptor,
          ],
        ),
      );
    } finally {
      _isLoadingSnippet = false;
      notifyListeners();
    }

    _fillFeedbackController();
  }

  void _fillFeedbackController() {
    final controller = GetIt.instance.get<FeedbackController>();
    controller.eventSnippetContext = playgroundController.eventSnippetContext;
    controller.additionalParams = _tobEventContext.toJson();
  }

  ExampleLoadingDescriptor _getStandardOrEmptyDescriptor(
    Sdk sdk,
    String? snippetId,
  ) {
    if (snippetId == null) {
      return EmptyExampleLoadingDescriptor(
        sdk: currentSdk,
      );
    }
    return StandardExampleLoadingDescriptor(
      path: snippetId,
      sdk: sdk,
    );
  }

  // TODO(alexeyinkin): Hide the entire right pane instead.
  Future<void> _emptyPlayground() async {
    await playgroundController.examplesLoader.loadIfNew(
      ExamplesLoadingDescriptor(
        descriptors: [
          EmptyExampleLoadingDescriptor(sdk: currentSdk),
        ],
      ),
    );
  }

  // Playground controller.

  static PlaygroundController _createPlaygroundController() {
    final playgroundController = PlaygroundController(
      codeClient: GetIt.instance.get<CodeClient>(),
      exampleCache: ExampleCache(
        exampleRepository: GetIt.instance.get<ExampleRepository>(),
      ),
      examplesLoader: ExamplesLoader(),
    );

    unawaited(
      playgroundController.examplesLoader.loadIfNew(
        ExamplesLoadingDescriptor(
          descriptors: [
            EmptyExampleLoadingDescriptor(
              sdk: GetIt.instance.get<AppNotifier>().sdk,
            ),
          ],
        ),
      ),
    );

    return playgroundController;
  }

  @override
  Future<void> dispose() async {
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
