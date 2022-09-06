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

import 'package:collection/collection.dart';
import 'package:flutter/material.dart';
import 'package:playground/modules/editor/controllers/snippet_editing_controller.dart';
import 'package:playground/modules/editor/parsers/run_options_parser.dart';
import 'package:playground/modules/editor/repository/code_repository/code_repository.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_request.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_result.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/examples/models/outputs_model.dart';
import 'package:playground/modules/examples/repositories/models/shared_file_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/states/example_loaders/examples_loader.dart';
import 'package:playground/pages/playground/states/examples_state.dart';

const kTitleLength = 15;
const kExecutionTimeUpdate = 100;
const kPrecompiledDelay = Duration(seconds: 1);
const kTitle = 'Catalog';
const kExecutionCancelledText = '\nPipeline cancelled';
const kPipelineOptionsParseError =
    'Failed to parse pipeline options, please check the format (example: --key1 value1 --key2 value2), only alphanumeric and ",*,/,-,:,;,\',. symbols are allowed';
const kCachedResultsLog =
    'The results of this example are taken from the Apache Beam Playground cache.\n';

class PlaygroundState with ChangeNotifier {
  final ExampleState exampleState;
  final ExamplesLoader examplesLoader;
  final ExamplesLoadingDescriptor examplesLoadingDescriptor;

  final _snippetEditingControllers = <SDK, SnippetEditingController>{};

  SDK? _sdk;
  CodeRepository? _codeRepository;
  RunCodeResult? _result;
  StreamSubscription<RunCodeResult>? _runSubscription;
  StreamController<int>? _executionTime;
  OutputType? selectedOutputFilterType;
  String outputResult = '';

  PlaygroundState({
    required this.exampleState,
    required this.examplesLoader,
    CodeRepository? codeRepository,
  }) : examplesLoadingDescriptor =
            ExamplesLoadingDescriptorFactory.fromUriParts(
          path: Uri.base.path,
          params: Uri.base.queryParameters,
        ) {
    examplesLoader.setPlaygroundState(this);
    examplesLoader.load(examplesLoadingDescriptor);

    _codeRepository = codeRepository;
    selectedOutputFilterType = OutputType.all;
    outputResult = '';
  }

  SnippetEditingController _getOrCreateSnippetEditingController(
    SDK sdk, {
    required bool loadDefaultIfNot,
  }) {
    final existing = _snippetEditingControllers[sdk];
    if (existing != null) {
      return existing;
    }

    final result = SnippetEditingController(sdk: sdk);
    _snippetEditingControllers[sdk] = result;

    if (loadDefaultIfNot) {
      final descriptor =
          examplesLoadingDescriptor.lazyLoadDescriptors[sdk]?.firstOrNull;

      if (descriptor != null) {
        examplesLoader.loadOne(
          group: examplesLoadingDescriptor,
          one: descriptor,
        );
      }
    }

    return result;
  }

  // TODO: Return full, then shorten.
  String get examplesTitle {
    final name = snippetEditingController?.selectedExample?.name ?? kTitle;
    return name.substring(0, min(kTitleLength, name.length));
  }

  ExampleModel? get selectedExample =>
      snippetEditingController?.selectedExample;

  SDK? get sdk => _sdk;

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

  bool get graphAvailable =>
      selectedExample?.type != ExampleType.test &&
      [SDK.java, SDK.python].contains(sdk);

  void setExample(
    ExampleModel example, {
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
    SDK sdk, {
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
      _result = RunCodeResult(
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
      final request = RunCodeRequestWrapper(
        code: controller.codeController.text,
        sdk: controller.sdk,
        pipelineOptions: parsedPipelineOptions,
      );
      _runSubscription = _codeRepository?.runCode(request).listen((event) {
        _result = event;
        filterOutput(selectedOutputFilterType ?? OutputType.all);

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
    _runSubscription?.cancel();
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
    String log = _result?.log ?? '';
    String output = _result?.output ?? '';
    setOutputResult(log + output);
    _executionTime?.close();
    notifyListeners();
  }

  Future<void> _showPrecompiledResult(
    SnippetEditingController snippetEditingController,
  ) async {
    _result = RunCodeResult(
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

    filterOutput(selectedOutputFilterType ?? OutputType.all);
    _executionTime?.close();
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

    return exampleState.getSnippetId(
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
}
