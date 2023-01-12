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

import '../cache/example_cache.dart';
import '../models/example.dart';
import '../models/example_base.dart';
import '../models/example_loading_descriptors/empty_example_loading_descriptor.dart';
import '../models/example_loading_descriptors/example_loading_descriptor.dart';
import '../models/example_loading_descriptors/examples_loading_descriptor.dart';
import '../models/example_loading_descriptors/standard_example_loading_descriptor.dart';
import '../models/example_loading_descriptors/user_shared_example_loading_descriptor.dart';
import '../models/intents.dart';
import '../models/outputs.dart';
import '../models/sdk.dart';
import '../models/shortcut.dart';
import '../repositories/code_repository.dart';
import '../repositories/models/run_code_request.dart';
import '../repositories/models/run_code_result.dart';
import '../services/symbols/loaders/map.dart';
import '../services/symbols/symbols_notifier.dart';
import '../util/pipeline_options.dart';
import 'example_loaders/examples_loader.dart';
import 'snippet_editing_controller.dart';

const kTitleLength = 25;
const kExecutionTimeUpdate = 100;
const kPrecompiledDelay = Duration(seconds: 1);
const kTitle = 'Catalog';
const kExecutionCancelledText = '\nPipeline cancelled';
const kPipelineOptionsParseError =
    'Failed to parse pipeline options, please check the format (example: --key1 value1 --key2 value2), only alphanumeric and ",*,/,-,:,;,\',. symbols are allowed';
const kCachedResultsLog =
    'The results of this example are taken from the Apache Beam Playground cache.\n';

/// The main state object for the code and its running.
class PlaygroundController with ChangeNotifier {
  final ExampleCache exampleCache;
  final ExamplesLoader examplesLoader;

  final _snippetEditingControllers = <Sdk, SnippetEditingController>{};

  Sdk? _sdk;
  final CodeRepository? _codeRepository;

  RunCodeResult? _result;
  StreamSubscription<RunCodeResult>? _runSubscription;
  StreamController<int>? _executionTime;

  // TODO(alexeyinkin): Extract along with run status, https://github.com/apache/beam/issues/23248
  OutputType selectedOutputFilterType = OutputType.all;
  String outputResult = '';

  PlaygroundController({
    required this.exampleCache,
    required this.examplesLoader,
    CodeRepository? codeRepository,
  }) : _codeRepository = codeRepository {
    examplesLoader.setPlaygroundController(this);
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
    result.addListener(notifyListeners);

    if (loadDefaultIfNot) {
      // TODO(alexeyinkin): Show loading indicator if loading.
      examplesLoader.loadDefaultIfAny(sdk);
    }

    return result;
  }

  // TODO(alexeyinkin): Return full, then shorten, https://github.com/apache/beam/issues/23250
  String get examplesTitle {
    final name = snippetEditingController?.example?.name ?? kTitle;
    return name.substring(0, min(kTitleLength, name.length));
  }

  Example? get selectedExample => snippetEditingController?.example;

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

  String? get source =>
      snippetEditingController?.activeFileController?.codeController.fullText;

  bool get isCodeRunning => !(result?.isFinished ?? true);

  RunCodeResult? get result => _result;

  String? get pipelineOptions => snippetEditingController?.pipelineOptions;

  Stream<int>? get executionTime => _executionTime?.stream;

  bool get isExampleChanged {
    return snippetEditingController?.isChanged ?? false;
  }

  // TODO(alexeyinkin): Single source of truth for whether graph is supported, https://github.com/apache/beam/issues/23251
  bool get graphAvailable =>
      selectedExample?.type != ExampleType.test &&
      [Sdk.java, Sdk.python].contains(sdk);

  /// If no SDK is selected, sets it to [sdk] and creates an empty state for it.
  void setEmptyIfNoSdk(Sdk sdk) {
    if (_sdk != null) {
      return;
    }

    setExample(
      Example.empty(sdk),
      descriptor: EmptyExampleLoadingDescriptor(sdk: sdk),
      setCurrentSdk: true,
    );
  }

  /// If the state for [sdk] does not exists, creates an empty state for it.
  void setEmptyIfNotExists(
    Sdk sdk, {
    required bool setCurrentSdk,
  }) {
    if (_snippetEditingControllers.containsKey(sdk)) {
      return;
    }

    setExample(
      Example.empty(sdk),
      descriptor: EmptyExampleLoadingDescriptor(sdk: sdk),
      setCurrentSdk: setCurrentSdk,
    );
  }

  Future<void> setExampleBase(ExampleBase exampleBase) async {
    final snippetEditingController = _getOrCreateSnippetEditingController(
      exampleBase.sdk,
      loadDefaultIfNot: false,
    );

    if (!snippetEditingController.lockExampleLoading()) {
      return;
    }

    notifyListeners();

    try {
      final example = await exampleCache.loadExampleInfo(exampleBase);
      // TODO(alexeyinkin): setCurrentSdk = false when we do
      //  per-SDK output and run status.
      //  Now using true to reset the output and run status.
      //  https://github.com/apache/beam/issues/23248
      final descriptor = StandardExampleLoadingDescriptor(
        sdk: example.sdk,
        path: example.path,
      );

      setExample(
        example,
        descriptor: descriptor,
        setCurrentSdk: true,
      );

      // ignore: avoid_catches_without_on_clauses
    } catch (ex) {
      snippetEditingController.releaseExampleLoading();
      notifyListeners();
      rethrow;
    }
  }

  void setExample(
    Example example, {
    required ExampleLoadingDescriptor descriptor,
    required bool setCurrentSdk,
  }) {
    if (setCurrentSdk) {
      _sdk = example.sdk;
      final controller = _getOrCreateSnippetEditingController(
        example.sdk,
        loadDefaultIfNot: false,
      );

      controller.setExample(example, descriptor: descriptor);
      _ensureSymbolsInitialized();
    } else {
      final controller = _getOrCreateSnippetEditingController(
        example.sdk,
        loadDefaultIfNot: false,
      );
      controller.setExample(example, descriptor: descriptor);
    }

    _result = null;
    _executionTime = null;
    setOutputResult('');
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

  void setSelectedOutputFilterType(OutputType type) {
    selectedOutputFilterType = type;
    notifyListeners();
  }

  void setOutputResult(String outputs) {
    outputResult = outputs;
    notifyListeners();
  }

  void clearOutput() {
    _result = null;
    notifyListeners();
  }

  void reset() {
    snippetEditingController?.reset();
    _executionTime = null;
    outputResult = '';
    notifyListeners();
  }

  void resetError() {
    if (result == null) {
      return;
    }
    _result = RunCodeResult(status: result!.status, output: result!.output);
    notifyListeners();
  }

  void setPipelineOptions(String options) {
    final controller = requireSnippetEditingController();
    controller.pipelineOptions = options;
    notifyListeners();
  }

  void runCode({void Function()? onFinish}) {
    final controller = requireSnippetEditingController();
    final parsedPipelineOptions =
        parsePipelineOptions(controller.pipelineOptions);
    if (parsedPipelineOptions == null) {
      _result = const RunCodeResult(
        status: RunCodeStatus.compileError,
        errorMessage: kPipelineOptionsParseError,
      );
      notifyListeners();
      return;
    }
    _executionTime?.close();
    _executionTime = _createExecutionTimeStream();
    if (!isExampleChanged && controller.example?.outputs != null) {
      _showPrecompiledResult(controller);
    } else {
      final request = RunCodeRequest(
        datasets: selectedExample?.datasets ?? [],
        files: controller.getFiles(),
        pipelineOptions: parsedPipelineOptions,
        sdk: controller.sdk,
      );
      _runSubscription = _codeRepository?.runCode(request).listen((event) {
        _result = event;
        filterOutput(selectedOutputFilterType);

        if (event.isFinished && onFinish != null) {
          onFinish();
          _executionTime?.close();
        }
        notifyListeners();
      });
      notifyListeners();
    }
  }

  Future<void> cancelRun() async {
    await _runSubscription?.cancel();
    final pipelineUuid = result?.pipelineUuid ?? '';

    if (pipelineUuid.isNotEmpty) {
      await _codeRepository?.cancelExecution(pipelineUuid);
    }

    _result = RunCodeResult(
      status: RunCodeStatus.finished,
      output: _result?.output,
      log: (_result?.log ?? '') + kExecutionCancelledText,
      graph: _result?.graph,
    );

    final log = _result?.log ?? '';
    final output = _result?.output ?? '';
    setOutputResult(log + output);
    await _executionTime?.close();
    notifyListeners();
  }

  Future<void> _showPrecompiledResult(
    SnippetEditingController snippetEditingController,
  ) async {
    _result = const RunCodeResult(
      status: RunCodeStatus.preparation,
    );
    final selectedExample = snippetEditingController.example!;

    notifyListeners();
    // add a little delay to improve user experience
    await Future.delayed(kPrecompiledDelay);

    String logs = selectedExample.logs ?? '';
    _result = RunCodeResult(
      status: RunCodeStatus.finished,
      output: selectedExample.outputs,
      log: kCachedResultsLog + logs,
      graph: selectedExample.graph,
    );

    filterOutput(selectedOutputFilterType);
    await _executionTime?.close();
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

  void filterOutput(OutputType type) {
    final output = result?.output ?? '';
    final log = result?.log ?? '';

    switch (type) {
      case OutputType.all:
      case OutputType.graph:
        setOutputResult(log + output);
        break;
      case OutputType.log:
        setOutputResult(log);
        break;
      case OutputType.output:
        setOutputResult(output);
        break;
    }
  }

  Future<UserSharedExampleLoadingDescriptor> saveSnippet() async {
    final snippetController = requireSnippetEditingController();
    final files = snippetController.getFiles();

    final snippetId = await exampleCache.saveSnippet(
      files: files,
      pipelineOptions: snippetController.pipelineOptions,
      sdk: snippetController.sdk,
    );

    final sharedExample = Example(
      datasets: snippetController.example?.datasets ?? [],
      files: files,
      name: files.first.name,
      path: snippetId,
      sdk: snippetController.sdk,
      type: ExampleType.example,
    );

    final descriptor = UserSharedExampleLoadingDescriptor(
      sdk: sharedExample.sdk,
      snippetId: snippetId,
    );

    snippetController.setExample(sharedExample, descriptor: descriptor);

    return descriptor;
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
      initialSdk: _sdk,
    );
  }

  late BeamShortcut runShortcut = BeamShortcut(
    shortcuts: LogicalKeySet(
      LogicalKeyboardKey.meta,
      LogicalKeyboardKey.enter,
    ),
    actionIntent: const RunIntent(),
    createAction: (BuildContext context) => CallbackAction(
      onInvoke: (_) => runCode(),
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
      onInvoke: (_) => reset(),
    ),
  );

  List<BeamShortcut> get shortcuts => [
        runShortcut,
        resetShortcut,
      ];
}
