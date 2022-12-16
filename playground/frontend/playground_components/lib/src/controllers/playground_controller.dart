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
import 'dart:math';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:get_it/get_it.dart';

import '../../playground_components.dart';
import '../repositories/models/shared_file.dart';
import '../services/symbols/loaders/map.dart';
import '../services/symbols/symbols_notifier.dart';
import 'code_runner.dart';
import 'snippet_editing_controller.dart';

const kTitleLength = 15;
const kExecutionTimeUpdate = 100;
const kPrecompiledDelay = Duration(seconds: 1);
const kTitle = 'Catalog';
const kExecutionCancelledText = '\nPipeline cancelled';
const kPipelineOptionsParseError =
    'Failed to parse pipeline options, please check the format (example: --key1 value1 --key2 value2), only alphanumeric and ",*,/,-,:,;,\',. symbols are allowed';
const kCachedResultsLog =
    'The results of this example are taken from the Apache Beam Playground cache.\n';

class PlaygroundController with ChangeNotifier {
  final ExampleCache exampleCache;
  final ExamplesLoader examplesLoader;

  late final CodeRunner codeRunner;
  final CodeRepository? _codeRepository;

  final _snippetEditingControllers = <Sdk, SnippetEditingController>{};

  Sdk? _sdk;

  // TODO(alexeyinkin): Extract along with run status, https://github.com/apache/beam/issues/23248

  PlaygroundController({
    required this.exampleCache,
    required this.examplesLoader,
    CodeRepository? codeRepository,
  }) : _codeRepository = codeRepository {
    examplesLoader.setPlaygroundController(this);

    codeRunner = CodeRunner(
      codeRepository: _codeRepository,
      snippetEditingController: () => snippetEditingController!,
    )..addListener(notifyListeners);
  }

  SnippetEditingController _getOrCreateSnippetEditingController(
    Sdk sdk, {
    required bool loadDefaultIfNot,
  }) {
    final existing = _snippetEditingControllers[sdk];
    if (existing != null) {
      return existing;
    }

    final result = SnippetEditingController(sdk: sdk);
    _snippetEditingControllers[sdk] = result;

    if (loadDefaultIfNot) {
      examplesLoader.loadDefaultIfAny(sdk);
    }

    return result;
  }

  // TODO(alexeyinkin): Return full, then shorten, https://github.com/apache/beam/issues/23250
  String get examplesTitle {
    final name = snippetEditingController?.selectedExample?.name ?? kTitle;
    return name.substring(0, min(kTitleLength, name.length));
  }

  Example? get selectedExample => snippetEditingController?.selectedExample;

  Sdk? get sdk => _sdk;

  SnippetEditingController? get snippetEditingController =>
      _snippetEditingControllers[_sdk];

  SnippetEditingController requireSnippetEditingController() {
    final controller = snippetEditingController;

    if (controller == null) {
      throw Exception('SDK is not set.');
    }

    return controller;
  }

  String? get source => snippetEditingController?.codeController.fullText;

  bool get isExampleChanged {
    return snippetEditingController?.isChanged ?? false;
  }

  // TODO(alexeyinkin): Single source of truth for whether graph is supported, https://github.com/apache/beam/issues/23251
  bool get graphAvailable =>
      selectedExample?.type != ExampleType.test &&
      [Sdk.java, Sdk.python].contains(sdk);

  void setExample(
    Example example, {
    required bool setCurrentSdk,
  }) {
    if (setCurrentSdk) {
      _sdk = example.sdk;
      final controller = _getOrCreateSnippetEditingController(
        example.sdk,
        loadDefaultIfNot: false,
      );

      controller.selectedExample = example;
      _ensureSymbolsInitialized();
    } else {
      final controller = _getOrCreateSnippetEditingController(
        example.sdk,
        loadDefaultIfNot: false,
      );
      controller.selectedExample = example;
    }

    codeRunner.clearResult();
    codeRunner.setOutputResult('');
    notifyListeners();
  }

  void setSdk(
    Sdk sdk, {
    bool notify = true,
  }) {
    _sdk = sdk;
    _getOrCreateSnippetEditingController(
      sdk,
      loadDefaultIfNot: true,
    );
    _ensureSymbolsInitialized();

    if (notify) {
      notifyListeners();
    }
  }

  void _ensureSymbolsInitialized() {
    final mode = _sdk?.highlightMode;
    final loader = symbolLoadersByMode[mode];

    if (mode == null || loader == null) {
      return;
    }

    GetIt.instance.get<SymbolsNotifier>().addLoaderIfNot(mode, loader);
  }

  // TODO(alexeyinkin): Remove, used only in tests, refactor them.
  void setSource(String source) {
    final controller = requireSnippetEditingController();
    controller.setSource(source);
  }

  void setPipelineOptions(String options) {
    final controller = requireSnippetEditingController();
    controller.pipelineOptions = options;
    notifyListeners();
  }

  StreamController<int> _createExecutionTimeStream() {
    StreamController<int>? streamController;
    Timer? timer;
    Duration timerInterval = const Duration(milliseconds: kExecutionTimeUpdate);
    int ms = 0;

    void stopTimer() {
      timer?.cancel();
      streamController?.close();
    }

    void tick(_) {
      ms += kExecutionTimeUpdate;
      streamController?.add(ms);
    }

    void startTimer() {
      timer = Timer.periodic(timerInterval, tick);
    }

    streamController = StreamController<int>.broadcast(
      onListen: startTimer,
      onCancel: stopTimer,
    );

    return streamController;
  }

  Future<String> getSnippetId() {
    final controller = requireSnippetEditingController();

    return exampleCache.getSnippetId(
      files: [
        SharedFile(code: controller.codeController.fullText, isMain: true),
      ],
      sdk: controller.sdk,
      pipelineOptions: controller.pipelineOptions,
    );
  }

  /// Creates an [ExamplesLoadingDescriptor] that can recover
  /// the current content.
  ExamplesLoadingDescriptor getLoadingDescriptor() {
    return ExamplesLoadingDescriptor(
      descriptors: _snippetEditingControllers.values
          .map(
            (controller) => controller.getLoadingDescriptor(),
          )
          .toList(growable: false),
    );
  }

  late BeamShortcut runShortcut = BeamShortcut(
    shortcuts: LogicalKeySet(
      LogicalKeyboardKey.meta,
      LogicalKeyboardKey.enter,
    ),
    actionIntent: const RunIntent(),
    createAction: (BuildContext context) => CallbackAction(
      onInvoke: (_) => codeRunner.runCode(),
    ),
  );

  late BeamShortcut resetShortcut = BeamShortcut(
    shortcuts: LogicalKeySet(
      LogicalKeyboardKey.meta,
      LogicalKeyboardKey.shift,
      LogicalKeyboardKey.keyE,
    ),
    actionIntent: const ResetIntent(),
    createAction: (BuildContext context) => CallbackAction(
      onInvoke: (_) => codeRunner.reset(),
    ),
  );

  List<BeamShortcut> get shortcuts => [
        runShortcut,
        resetShortcut,
      ];

  @override
  void dispose() {
    super.dispose();
    codeRunner.removeListener(notifyListeners);
  }
}
