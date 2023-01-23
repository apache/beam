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
import '../../cache/units_progress.dart';
import '../../config.dart';
import '../../models/unit.dart';
import '../../models/unit_content.dart';
import '../../repositories/client/client.dart';
import '../../state.dart';
import 'controllers/content_tree.dart';
import 'controllers/unit.dart';
import 'path.dart';

class TourNotifier extends ChangeNotifier with PageStateMixin<void> {
  static const _saveUserCodeDebounceDuration = Duration(seconds: 2);

  final ContentTreeController contentTreeController;
  final PlaygroundController playgroundController;
  UnitController? currentUnitController;
  final _appNotifier = GetIt.instance.get<AppNotifier>();
  final _authNotifier = GetIt.instance.get<AuthNotifier>();
  final _unitContentCache = GetIt.instance.get<UnitContentCache>();
  final _unitsProgressCache = GetIt.instance.get<UnitsProgressCache>();
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
    _authNotifier.addListener(_onAuthChanged);
    _setSaveCodeListener();
    _onUnitChanged();
  }

  void _setSaveCodeListener() {
    final saveCodeDebounced =
        _saveUserCode.debounced(_saveUserCodeDebounceDuration);

    // TODO(nausharipov): review
    // couldn't remove anonymous functions and dispose listeners
    // because the functions would require saveCodeDebounced as an argument.

    // Creates snippetEditingController
    playgroundController.setSdk(_appNotifier.sdk!);
    // Notifies when activeFileController is created
    playgroundController.snippetEditingController?.addListener(
      () {
        // Notifies when code is changed
        playgroundController
            .snippetEditingController?.activeFileController?.codeController
            .addListener(
          () {
            final currentUnit = currentUnitController;
            final isCodeChanged = playgroundController.snippetEditingController
                    ?.activeFileController?.isChanged ??
                false;
            final code = playgroundController.snippetEditingController
                ?.activeFileController?.codeController.fullText;
            final doSave = _authNotifier.isAuthenticated &&
                isCodeChanged &&
                currentUnit != null &&
                code != null;
            if (isCodeChanged) {
              _setResetSnippetFalse();
            }

            if (doSave) {
              saveCodeDebounced.call([
                currentUnit.sdkId,
                currentUnit.unitId,
                code,
              ]);
            }
          },
        );
      },
    );
  }

  Future<void> _saveUserCode(String sdkId, String unitId, String code) async {
    try {
      final client = GetIt.instance.get<TobClient>();
      await client.postUserCode(sdkId, unitId, code);
      if (!isCurrentUnitCodeSaved) {
        await _unitsProgressCache.updateUnitsProgress();
        notifyListeners();
      }
    } on Exception catch (e) {
      // TODO(nausharipov): how to handle?
      print(['Could not save code: ', e]);
    }
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
  bool get isCurrentUnitCodeSaved =>
      _unitsProgressCache.getUnitSnippets()[currentUnitId] != null;
  bool _resetSnippet = false;
  bool get resetSnippet => _resetSnippet;

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

  void _setResetSnippetFalse() {
    _resetSnippet = false;
    notifyListeners();
  }

  Future<void> toggleReset() async {
    _isShowingSolution = false;
    _resetSnippet = !_resetSnippet;
    await _setCurrentSnippet();
    notifyListeners();
  }

  Future<void> _onAuthChanged() async {
    await _unitsProgressCache.updateUnitsProgress();
    await _setCurrentSnippet();
  }

  void _onAppNotifierChanged() {
    final sdkId = _appNotifier.sdkId;
    if (sdkId != null) {
      playgroundController.setSdk(Sdk.parseOrCreate(sdkId));
      contentTreeController.sdkId = sdkId;
      _onAuthChanged();
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
    await _setCurrentSnippet();
  }

  Future<void> _setCurrentSnippet() async {
    final unit = _currentUnitContent;
    if (unit != null) {
      final String? snippetId;
      if (_resetSnippet) {
        snippetId = unit.taskSnippetId;
      } else {
        await _unitsProgressCache.updateUnitsProgress();
        snippetId = _unitsProgressCache.getUnitSnippets()[unit.id] ??
            unit.taskSnippetId;
      }
      await _setPlaygroundSnippet(snippetId);
      _isShowingSolution = false;
    }
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
        doCheckDescriptor: false,
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
    _authNotifier.removeListener(_onAuthChanged);
    super.dispose();
  }
}
