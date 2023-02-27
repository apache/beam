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

import 'package:clock/clock.dart';
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
    required ValueGetter<SnippetEditingController>
        snippetEditingControllerGetter,
    CodeRepository? codeRepository,
  })  : _codeRepository = codeRepository,
        _snippetEditingControllerGetter = snippetEditingControllerGetter;

  RunCodeResult? _result;
  StreamSubscription<RunCodeResult>? _runSubscription;
  DateTime? _runStartDate;
  DateTime? _runStopDate;

  /// [Duration] from the last execution start to finish or to present time.
  Duration? get elapsed => _runStartDate == null
      ? null
      : (_runStopDate ?? clock.now()).difference(_runStartDate!);

  /// The [EventSnippetContext] at the time when execution started.
  EventSnippetContext? _eventSnippetContext;

  /// [EventSnippetContext] for which the execution last started.
  EventSnippetContext? get eventSnippetContext => _eventSnippetContext;

  String? get pipelineOptions =>
      _snippetEditingControllerGetter().pipelineOptions;

  RunCodeResult? get result => _result;
  DateTime? get runStartDate => _runStartDate;
  DateTime? get runStopDate => _runStopDate;
  bool get isCodeRunning => !(_result?.isFinished ?? true);

  String get resultLog => _result?.log ?? '';
  String get resultOutput => _result?.output ?? '';
  String get resultLogOutput => resultLog + resultOutput;

  bool get isExampleChanged {
    return _snippetEditingControllerGetter().isChanged;
  }

  // Snapshot of additional analytics data at the time when execution started.
  Map<String, dynamic> _analyticsData = const {};
  Map<String, dynamic> get analyticsData => _analyticsData;

  bool get canRun => snippetEditingController != null;

  void clearResult() {
    _eventSnippetContext = null;
    _result = null;
    notifyListeners();
  }

  void runCode({
    void Function()? onFinish,
    Map<String, dynamic> analyticsData = const {},
  }) {
    _analyticsData = analyticsData;
    _runStartDate = DateTime.now();
    _runStopDate = null;
    notifyListeners();
    snippetEditingController = _snippetEditingControllerGetter();
    _eventSnippetContext = snippetEditingController!.eventSnippetContext;

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

    if (!isExampleChanged &&
        snippetEditingController!.example?.outputs != null) {
      unawaited(_showPrecompiledResult());
    } else {
      final request = RunCodeRequest(
        datasets: snippetEditingController?.example?.datasets ?? [],
        files: snippetEditingController!.getFiles(),
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

  /// Resets the error message text so that on the next rebuild
  /// of `CodeTextAreaWrapper` it is not picked up and not shown as a toast.
  // TODO: Listen to this object outside of widgets,
  //  emit toasts from notifications, then remove this method.
  void resetErrorMessageText() {
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
    final selectedExample = snippetEditingController!.example!;

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
