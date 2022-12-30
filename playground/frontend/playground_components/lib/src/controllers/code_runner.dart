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

import 'package:flutter/material.dart';

import '../../playground_components.dart';
import '../repositories/models/run_code_request.dart';
import '../repositories/models/run_code_result.dart';
import 'snippet_editing_controller.dart';

class CodeRunner extends ChangeNotifier {
  final CodeRepository? _codeRepository;
  final ValueGetter<SnippetEditingController> _snippetEditingControllerGetter;
  SnippetEditingController? snippetEditingController;

  CodeRunner({
    required ValueGetter<SnippetEditingController> snippetEditingController,
    CodeRepository? codeRepository,
  })  : _codeRepository = codeRepository,
        _snippetEditingControllerGetter = snippetEditingController;

  RunCodeResult? _result;
  StreamSubscription<RunCodeResult>? _runSubscription;
  DateTime? _runStartDate;
  DateTime? _runStopDate;

  String? get pipelineOptions => snippetEditingController?.pipelineOptions;
  RunCodeResult? get result => _result;
  DateTime? get runStartDate => _runStartDate;
  DateTime? get runStopDate => _runStopDate;
  bool get isCodeRunning => !(_result?.isFinished ?? true);

  String get resultLog => _result?.log ?? '';
  String get resultOutput => _result?.output ?? '';
  String get resultLogOutput => resultLog + resultOutput;

  void clearResult() {
    _result = null;
    notifyListeners();
  }

  void runCode({void Function()? onFinish}) {
    _runStartDate = DateTime.now();
    _runStopDate = null;
    notifyListeners();
    snippetEditingController = _snippetEditingControllerGetter();

    final parsedPipelineOptions =
        parsePipelineOptions(snippetEditingController!.pipelineOptions);
    if (parsedPipelineOptions == null) {
      _result = const RunCodeResult(
        status: RunCodeStatus.compileError,
        errorMessage: kPipelineOptionsParseError,
      );
      _runStopDate = DateTime.now();
      notifyListeners();
      return;
    }

    if (!snippetEditingController!.isChanged &&
        snippetEditingController!.selectedExample?.outputs != null) {
      unawaited(_showPrecompiledResult());
    } else {
      final request = RunCodeRequest(
        code: snippetEditingController!.codeController.fullText,
        sdk: snippetEditingController!.sdk,
        pipelineOptions: parsedPipelineOptions,
      );
      _runSubscription = _codeRepository?.runCode(request).listen((event) {
        _result = event;
        notifyListeners();

        if (event.isFinished) {
          if (onFinish != null) {
            onFinish();
          }
          snippetEditingController = null;
          _runStopDate = DateTime.now();
        }
      });
      notifyListeners();
    }
  }

  void clearOutput() {
    _result = null;
    notifyListeners();
  }

  void setResultAfterResetError() {
    if (_result == null) {
      return;
    }
    _result = RunCodeResult(
      status: _result!.status,
      output: _result!.output,
    );
    notifyListeners();
  }

  Future<void> cancelRun() async {
    snippetEditingController = null;
    await _runSubscription?.cancel();
    final pipelineUuid = _result?.pipelineUuid ?? '';

    if (pipelineUuid.isNotEmpty) {
      await _codeRepository?.cancelExecution(pipelineUuid);
    }

    _result = RunCodeResult(
      status: RunCodeStatus.finished,
      output: _result?.output,
      log: (_result?.log ?? '') + kExecutionCancelledText,
      graph: _result?.graph,
    );

    _runStopDate = DateTime.now();
    notifyListeners();
  }

  Future<void> _showPrecompiledResult() async {
    _result = const RunCodeResult(
      status: RunCodeStatus.preparation,
    );
    final selectedExample = snippetEditingController!.selectedExample!;

    notifyListeners();
    // add a little delay to improve user experience
    await Future.delayed(kPrecompiledDelay);

    final String logs = selectedExample.logs ?? '';
    _result = RunCodeResult(
      status: RunCodeStatus.finished,
      output: selectedExample.outputs,
      log: kCachedResultsLog + logs,
      graph: selectedExample.graph,
    );

    _runStopDate = DateTime.now();
    notifyListeners();
  }
}
