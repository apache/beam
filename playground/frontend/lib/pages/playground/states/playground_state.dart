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
import 'package:playground/modules/editor/parsers/run_options_parser.dart';
import 'package:playground/modules/editor/repository/code_repository/code_repository.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_request.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_result.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/examples/models/outputs_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

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
  late SDK _sdk;
  CodeRepository? _codeRepository;
  ExampleModel? _selectedExample;
  String _source = '';
  RunCodeResult? _result;
  StreamSubscription<RunCodeResult>? _runSubscription;
  String _pipelineOptions = '';
  DateTime? resetKey;
  StreamController<int>? _executionTime;
  OutputType? selectedOutputFilterType;
  String? outputResult;

  PlaygroundState({
    SDK sdk = SDK.java,
    ExampleModel? selectedExample,
    CodeRepository? codeRepository,
  }) {
    _selectedExample = selectedExample;
    _pipelineOptions = selectedExample?.pipelineOptions ?? '';
    _sdk = sdk;
    _source = _selectedExample?.source ?? '';
    _codeRepository = codeRepository;
    selectedOutputFilterType = OutputType.all;
    outputResult = '';
  }

  String get examplesTitle {
    final name = _selectedExample?.name ?? kTitle;
    return name.substring(0, min(kTitleLength, name.length));
  }

  ExampleModel? get selectedExample => _selectedExample;

  SDK get sdk => _sdk;

  String get source => _source;

  bool get isCodeRunning => !(result?.isFinished ?? true);

  RunCodeResult? get result => _result;

  String get pipelineOptions => _pipelineOptions;

  Stream<int>? get executionTime => _executionTime?.stream;

  bool get isExampleChanged {
    return selectedExample?.source != source || _arePipelineOptionsChanges;
  }

  bool get _arePipelineOptionsChanges {
    return pipelineOptions != (_selectedExample?.pipelineOptions ?? '');
  }

  bool get graphAvailable =>
      selectedExample?.type != ExampleType.test &&
      [SDK.java, SDK.python].contains(sdk);

  setExample(ExampleModel example) {
    _selectedExample = example;
    _pipelineOptions = example.pipelineOptions ?? '';
    _source = example.source ?? '';
    _result = null;
    _executionTime = null;
    setOutputResult('');
    notifyListeners();
  }

  setSdk(SDK sdk) {
    _sdk = sdk;
    notifyListeners();
  }

  setSource(String source) {
    _source = source;
  }

  setSelectedOutputFilterType(OutputType type) {
    selectedOutputFilterType = type;
    notifyListeners();
  }

  setOutputResult(String outputs) {
    outputResult = outputs;
    notifyListeners();
  }

  clearOutput() {
    _result = null;
    notifyListeners();
  }

  reset() {
    _source = _selectedExample?.source ?? '';
    _pipelineOptions = selectedExample?.pipelineOptions ?? '';
    resetKey = DateTime.now();
    _executionTime = null;
    setOutputResult('');
    notifyListeners();
  }

  resetError() {
    if (result == null) {
      return;
    }
    _result = RunCodeResult(status: result!.status, output: result!.output);
    notifyListeners();
  }

  setPipelineOptions(String options) {
    _pipelineOptions = options;
    notifyListeners();
  }

  void runCode({void Function()? onFinish}) {
    final parsedPipelineOptions = parsePipelineOptions(pipelineOptions);
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
    if (!isExampleChanged && _selectedExample?.outputs != null) {
      _showPrecompiledResult();
    } else {
      final request = RunCodeRequestWrapper(
        code: source,
        sdk: sdk,
        pipelineOptions: parsedPipelineOptions,
      );
      _runSubscription = _codeRepository?.runCode(request).listen((event) {
        _result = event;
        String log = event.log ?? '';
        String output = event.output ?? '';
        setOutputResult(log + output);

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

  _showPrecompiledResult() async {
    _result = RunCodeResult(
      status: RunCodeStatus.preparation,
    );
    notifyListeners();
    // add a little delay to improve user experience
    await Future.delayed(kPrecompiledDelay);
    String logs = _selectedExample!.logs ?? '';
    _result = RunCodeResult(
      status: RunCodeStatus.finished,
      output: _selectedExample!.outputs,
      log: kCachedResultsLog + logs,
      graph: _selectedExample!.graph,
    );
    setOutputResult(_result!.log! + _result!.output!);
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

  filterOutput(OutputType type) {
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
}
