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

import '../cache/example_cache.dart';
import '../models/example.dart';
import '../models/example_base.dart';
import '../models/example_loading_descriptors/examples_loading_descriptor.dart';
import '../models/intents.dart';
import '../models/outputs.dart';
import '../models/sdk.dart';
import '../models/shortcut.dart';
import '../repositories/code_repository.dart';
import '../repositories/models/run_code_request.dart';
import '../repositories/models/run_code_result.dart';
import '../repositories/models/shared_file.dart';
import '../util/pipeline_options.dart';
import 'example_loaders/examples_loader.dart';
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

  String? get source => snippetEditingController?.codeController.text;

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
    } else {
      final controller = _getOrCreateSnippetEditingController(
        example.sdk,
        loadDefaultIfNot: false,
      );
      controller.selectedExample = example;
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

    if (notify) {
      notifyListeners();
    }
  }

  void setSource(String source) {
    final controller = requireSnippetEditingController();
    controller.codeController.text = source;
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
    if (!isExampleChanged && controller.selectedExample?.outputs != null) {
      _showPrecompiledResult(controller);
    } else {
      final request = RunCodeRequest(
        code: controller.codeController.text,
        sdk: controller.sdk,
        pipelineOptions: parsedPipelineOptions,
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
    final selectedExample = snippetEditingController.selectedExample!;

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
    var output = result?.output ?? '';
    var log = result?.log ?? '';

    switch (type) {
      case OutputType.all:
        setOutputResult(log + output);
        break;
      case OutputType.log:
        setOutputResult(log);
        break;
      case OutputType.output:
        setOutputResult(output);
        break;
      default:
        setOutputResult(log + output);
        break;
    }
  }

  Future<String> getSnippetId() {
    final controller = requireSnippetEditingController();

    return exampleCache.getSnippetId(
      files: [SharedFile(code: controller.codeController.text, isMain: true)],
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
