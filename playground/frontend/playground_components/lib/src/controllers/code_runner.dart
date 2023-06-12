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

// ignore_for_file: prefer_interpolation_to_compose_strings

import 'dart:async';

import 'package:clock/clock.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';

import '../../playground_components.dart';
import '../enums/unread_entry.dart';
import '../repositories/models/output_response.dart';
import '../repositories/models/run_code_error.dart';
import '../repositories/models/run_code_request.dart';
import '../repositories/models/run_code_response.dart';
import '../repositories/models/run_code_result.dart';
import '../util/connectivity_result.dart';
import 'snippet_editing_controller.dart';
import 'unread_controller.dart';

const kTimeoutErrorText =
    'Pipeline exceeded Playground execution timeout and was terminated. '
    'We recommend installing Apache Beam '
    'https://beam.apache.org/get-started/downloads/ '
    'to try examples without timeout limitation.';
const kUnknownErrorText =
    'Something went wrong. Please try again later or create a GitHub issue';
const kProcessingStartedText = 'The processing has been started\n';
const kProcessingStartedOptionsText =
    'The processing has been started with the pipeline options: ';

class CodeRunner extends ChangeNotifier {
  final CodeClient? codeClient;
  final ValueGetter<SnippetEditingController?> _snippetEditingControllerGetter;
  SnippetEditingController? snippetEditingController;
  final unreadController = UnreadController();

  CodeRunner({
    required ValueGetter<SnippetEditingController?>
        snippetEditingControllerGetter,
    this.codeClient,
  }) : _snippetEditingControllerGetter = snippetEditingControllerGetter;

  RunCodeResult? _result;
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

  static const _attempts = 6;
  static const _attemptInterval = Duration(seconds: 5);
  static const _statusCheckInterval = Duration(seconds: 1);

  void clearResult() {
    _eventSnippetContext = null;
    _setResult(null);
  }

  Future<void> reset() async {
    if (isCodeRunning) {
      await cancelRun();
    }
    _runStartDate = null;
    _runStopDate = null;
    _eventSnippetContext = null;
    _setResult(null);
  }

  Future<void> runCode({
    Map<String, dynamic> analyticsData = const {},
  }) async {
    _analyticsData = analyticsData;
    _runStartDate = DateTime.now();
    _runStopDate = null;
    snippetEditingController = _snippetEditingControllerGetter();
    _eventSnippetContext = snippetEditingController!.eventSnippetContext;

    if (!isExampleChanged &&
        snippetEditingController!.example?.outputs != null) {
      await _showPrecompiledResult();
      return;
    }

    await _runReal();
  }

  Future<void> _runReal() async {
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
      return;
    }

    final log = parsedPipelineOptions.isEmpty
        ? kProcessingStartedText
        : kProcessingStartedOptionsText +
            parsedPipelineOptions.entries
                .map((e) => '--${e.key} ${e.value}')
                .join(' ') +
            '\n';

    _setResult(
      RunCodeResult(
        log: log,
        sdk: sdk,
        status: RunCodeStatus.preparation,
      ),
    );

    final request = RunCodeRequest(
      datasets: snippetEditingController?.example?.datasets ?? [],
      files: snippetEditingController!.getFiles(),
      sdk: snippetEditingController!.sdk,
      pipelineOptions: parsedPipelineOptions,
    );

    try {
      final runResponse = await _startExecution(request);

      if (runResponse == null || _result!.isFinished) {
        // Cancelled while trying to start.
        final pipelineUuid = runResponse?.pipelineUuid;
        if (pipelineUuid != null) {
          await codeClient?.cancelExecution(pipelineUuid);
        }
        return;
      }

      await Future.delayed(_statusCheckInterval);

      while (!_result!.isFinished) {
        final statusResponse =
            await codeClient!.checkStatus(runResponse.pipelineUuid);

        final result = await _getPipelineResult(
          runResponse.pipelineUuid,
          statusResponse.status,
          _result!,
        );

        _setResultIfNotFinished(result);

        await Future.delayed(_statusCheckInterval);
      }
    } on RunCodeError catch (ex) {
      _setResult(
        RunCodeResult(
          errorMessage: ex.message ?? kUnknownErrorText,
          log: _result!.log,
          output: (_result!.output ?? '') +
              '\n' +
              (ex.message ?? kUnknownErrorText),
          sdk: request.sdk,
          status: RunCodeStatus.unknownError,
        ),
      );
    } on Exception catch (ex) {
      print(ex); // ignore: avoid_print
      _setResult(
        RunCodeResult(
          errorMessage: kUnknownErrorText,
          log: _result!.log,
          output: (_result!.output ?? '') + '\n' + kUnknownErrorText,
          sdk: request.sdk,
          status: RunCodeStatus.unknownError,
        ),
      );
    } finally {
      snippetEditingController = null;
    }
  }

  Future<RunCodeResponse?> _startExecution(RunCodeRequest request) async {
    Exception? lastException;

    // Attempts to place the job for execution.
    // This fails if the backend is overloaded and has not yet scaled up.
    for (int attemptsLeft = _attempts; --attemptsLeft >= 0;) {
      if (_result!.isFinished) {
        return null; // Cancelled while retrying.
      }

      try {
        return await codeClient!.runCode(request);
      } on RunCodeResourceExhaustedError catch (ex) {
        lastException = ex;
      }

      // ignore: avoid_print
      print(
        'Got RunCodeResourceExhaustedError, attempts left: $attemptsLeft.',
      );
      if (attemptsLeft > 0) {
        // ignore: avoid_print
        print('Waiting for $_attemptInterval before retrying.');
        await Future.delayed(_attemptInterval);
      }
    }

    throw lastException ?? Exception('lastException must be filled above.');
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
        graph: _result!.graph,
        log: _result!.log,
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
    final pipelineUuid = _result?.pipelineUuid ?? '';

    if (pipelineUuid.isNotEmpty) {
      await codeClient?.cancelExecution(pipelineUuid);
    }

    _setResult(
      RunCodeResult(
        graph: _result?.graph,
        log: (_result?.log ?? '') +
            '\n' +
            'widgets.output.messages.pipelineCancelled'.tr(),
        output: _result?.output,
        sdk: sdk,
        status: RunCodeStatus.cancelled,
      ),
    );
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
        log: kCachedResultsLog + logs,
        output: selectedExample.outputs,
        sdk: selectedExample.sdk,
        status: RunCodeStatus.finished,
      ),
    );
  }

  void _setResultIfNotFinished(RunCodeResult newValue) {
    if (_result?.isFinished ?? true) {
      return;
    }
    _setResult(newValue);
  }

  void _setResult(RunCodeResult? newValue) {
    // ignore: use_if_null_to_convert_nulls_to_bools
    if (_result?.isFinished == false && newValue?.isFinished == true) {
      _runStopDate = clock.now();
    }

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

    notifyListeners();
  }

  Future<RunCodeResult> _getPipelineResult(
    String pipelineUuid,
    RunCodeStatus status,
    RunCodeResult prevResult,
  ) async {
    final prevOutput = prevResult.output ?? '';
    final prevLog = prevResult.log ?? '';
    final prevGraph = prevResult.graph ?? '';

    switch (status) {
      case RunCodeStatus.compileError:
        final compileOutput = await codeClient!.getCompileOutput(pipelineUuid);
        return RunCodeResult(
          graph: prevGraph,
          log: prevLog,
          output: prevOutput + compileOutput.output,
          pipelineUuid: pipelineUuid,
          sdk: prevResult.sdk,
          status: status,
        );

      case RunCodeStatus.timeout:
        return RunCodeResult(
          errorMessage: kTimeoutErrorText,
          graph: prevGraph,
          log: prevLog,
          output: prevOutput + kTimeoutErrorText,
          pipelineUuid: pipelineUuid,
          sdk: prevResult.sdk,
          status: status,
        );

      case RunCodeStatus.runError:
        final output = await codeClient!.getRunErrorOutput(pipelineUuid);
        return RunCodeResult(
          graph: prevGraph,
          log: prevLog,
          output: prevOutput + output.output,
          pipelineUuid: pipelineUuid,
          sdk: prevResult.sdk,
          status: status,
        );

      case RunCodeStatus.validationError:
        final output = await codeClient!.getValidationErrorOutput(pipelineUuid);
        return RunCodeResult(
          graph: prevGraph,
          log: prevLog,
          output: prevOutput + output.output,
          sdk: prevResult.sdk,
          status: status,
        );

      case RunCodeStatus.preparationError:
        final output =
            await codeClient!.getPreparationErrorOutput(pipelineUuid);
        return RunCodeResult(
          graph: prevGraph,
          log: prevLog,
          output: prevOutput + output.output,
          sdk: prevResult.sdk,
          status: status,
        );

      case RunCodeStatus.unknownError:
        return RunCodeResult(
          errorMessage: kUnknownErrorText,
          graph: prevGraph,
          log: prevLog,
          output: prevOutput + kUnknownErrorText,
          pipelineUuid: pipelineUuid,
          sdk: prevResult.sdk,
          status: status,
        );

      case RunCodeStatus.executing:
        final responses = await Future.wait([
          codeClient!.getRunOutput(pipelineUuid),
          codeClient!.getLogOutput(pipelineUuid),
          prevGraph.isEmpty
              ? codeClient!.getGraphOutput(pipelineUuid)
              : Future.value(OutputResponse(output: prevGraph)),
        ]);
        final output = responses[0];
        final log = responses[1];
        final graph = responses[2];
        return RunCodeResult(
          graph: graph.output,
          log: prevLog + log.output,
          output: prevOutput + output.output,
          pipelineUuid: pipelineUuid,
          sdk: prevResult.sdk,
          status: status,
        );

      case RunCodeStatus.cancelled:
      case RunCodeStatus.finished:
        final responses = await Future.wait([
          codeClient!.getRunOutput(pipelineUuid),
          codeClient!.getLogOutput(pipelineUuid),
          codeClient!.getRunErrorOutput(pipelineUuid),
          prevGraph.isEmpty
              ? codeClient!.getGraphOutput(pipelineUuid)
              : Future.value(OutputResponse(output: prevGraph)),
        ]);
        final output = responses[0];
        final log = responses[1];
        final error = responses[2];
        final graph = responses[3];
        return RunCodeResult(
          graph: graph.output,
          log: prevLog + log.output,
          output: prevOutput + output.output + error.output,
          pipelineUuid: pipelineUuid,
          sdk: prevResult.sdk,
          status: status,
        );

      case RunCodeStatus.unspecified:
      case RunCodeStatus.preparation:
      case RunCodeStatus.compiling:
        return RunCodeResult(
          graph: prevGraph,
          log: prevLog,
          output: prevOutput,
          pipelineUuid: pipelineUuid,
          sdk: prevResult.sdk,
          status: status,
        );
    }
  }
}
