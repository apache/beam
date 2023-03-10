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
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';

import '../../playground_components.dart';
import '../enums/unread_entry.dart';
import '../repositories/models/run_code_request.dart';
import '../repositories/models/run_code_result.dart';
import '../util/connectivity_result.dart';
import 'snippet_editing_controller.dart';
import 'unread_controller.dart';

class CodeRunner extends ChangeNotifier {
  final CodeRepository? _codeRepository;
  final ValueGetter<SnippetEditingController?> _snippetEditingControllerGetter;
  SnippetEditingController? snippetEditingController;
  final unreadController = UnreadController();

  CodeRunner({
    required ValueGetter<SnippetEditingController?>
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
      _snippetEditingControllerGetter()?.pipelineOptions;

  RunCodeResult? get result => _result;
  DateTime? get runStartDate => _runStartDate;
  DateTime? get runStopDate => _runStopDate;
  bool get isCodeRunning => !(_result?.isFinished ?? true);

  String get resultLog => _result?.log ?? '';
  String get resultOutput => _result?.output ?? '';
  String get resultLogOutput => resultLog + resultOutput;

  bool get isExampleChanged {
    return _snippetEditingControllerGetter()?.isChanged ?? false;
  }

  // Snapshot of additional analytics data at the time when execution started.
  Map<String, dynamic> _analyticsData = const {};
  Map<String, dynamic> get analyticsData => _analyticsData;

  bool get canRun => _snippetEditingControllerGetter() != null;

  void clearResult() {
    _eventSnippetContext = null;
    _setResult(null);
    notifyListeners();
  }

  Future<void> reset() async {
    if (isCodeRunning) {
      await cancelRun();
    }
    _runStartDate = null;
    _runStopDate = null;
    _eventSnippetContext = null;
    _setResult(null);
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
    final sdk = snippetEditingController!.sdk;

    final parsedPipelineOptions =
        parsePipelineOptions(snippetEditingController!.pipelineOptions);
    if (parsedPipelineOptions == null) {
      _setResult(
        RunCodeResult(
          errorMessage: 'errors.failedParseOptions'.tr(),
          sdk: sdk,
          status: RunCodeStatus.compileError,
        ),
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
        _setResult(event);
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

    _setResult(
      RunCodeResult(
        output: _result!.output,
        sdk: _result!.sdk,
        status: _result!.status,
      ),
    );

    notifyListeners();
  }

  Future<void> cancelRun() async {
    final sdk = _result?.sdk;
    if (sdk == null) {
      return;
    }

    final hasInternet = (await Connectivity().checkConnectivity()).isConnected;
    if (!hasInternet) {
      _setResult(
        RunCodeResult(
          errorMessage: 'errors.internetUnavailable'.tr(),
          graph: _result?.graph,
          log: _result?.log ?? '',
          output: _result?.output,
          sdk: sdk,
          status: _result?.status ?? RunCodeStatus.unspecified,
        ),
      );
      notifyListeners();
      return;
    }

    snippetEditingController = null;
    // Awaited cancelling subscription here blocks further method execution.
    // TODO: Figure out the reason: https://github.com/apache/beam/issues/25509
    unawaited(_runSubscription?.cancel());
    final pipelineUuid = _result?.pipelineUuid ?? '';

    if (pipelineUuid.isNotEmpty) {
      await _codeRepository?.cancelExecution(pipelineUuid);
    }

    _setResult(
      RunCodeResult(
        graph: _result?.graph,
        // ignore: prefer_interpolation_to_compose_strings
        log: (_result?.log ?? '') +
            '\n' +
            'widgets.output.messages.pipelineCancelled'.tr(),
        output: _result?.output,
        sdk: sdk,
        status: RunCodeStatus.finished,
      ),
    );

    _runStopDate = DateTime.now();
    notifyListeners();
  }

  Future<void> _showPrecompiledResult() async {
    final selectedExample = snippetEditingController!.example!;

    _setResult(
      RunCodeResult(
        sdk: selectedExample.sdk,
        status: RunCodeStatus.preparation,
      ),
    );

    notifyListeners();
    // add a little delay to improve user experience
    await Future.delayed(kPrecompiledDelay);

    if (_result?.status != RunCodeStatus.preparation) {
      return;
    }

    final String logs = selectedExample.logs ?? '';
    _setResult(
      RunCodeResult(
        graph: selectedExample.graph,
        // ignore: prefer_interpolation_to_compose_strings
        log: kCachedResultsLog + logs,
        output: selectedExample.outputs,
        sdk: selectedExample.sdk,
        status: RunCodeStatus.finished,
      ),
    );

    _runStopDate = DateTime.now();
    notifyListeners();
  }

  void _setResult(RunCodeResult? newValue) {
    _result = newValue;

    if (newValue == null) {
      unreadController.markAllRead();
    } else {
      unreadController.setValue(
        UnreadEntryEnum.result,
        (newValue.output ?? '') + (newValue.log ?? ''),
      );
      unreadController.setValue(
        UnreadEntryEnum.graph,
        newValue.graph ?? '',
      );
    }
  }
}
