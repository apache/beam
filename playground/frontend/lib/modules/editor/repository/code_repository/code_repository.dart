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

import 'package:playground/modules/editor/repository/code_repository/code_client/code_client.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_error.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_request.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_result.dart';
import 'package:playground/utils/run_with_retry.dart';

const kPipelineCheckDelay = Duration(seconds: 1);
const kTimeoutErrorText =
    'Pipeline exceeded Playground execution timeout and was terminated. '
    'We recommend installing Apache Beam '
    'https://beam.apache.org/get-started/downloads/ '
    'to try examples without timeout limitation.';
const kUnknownErrorText =
    'Something went wrong. Please try again later or create a jira ticket';
const kProcessingStartedText = 'The processing has started';

class CodeRepository {
  late final CodeClient _client;

  CodeRepository(CodeClient client) {
    _client = client;
  }

  Stream<RunCodeResult> runCode(RunCodeRequestWrapper request) async* {
    try {
      final initResult = RunCodeResult(
        status: RunCodeStatus.preparation,
        log: kProcessingStartedText,
      );
      yield initResult;
      var runCodeResponse = await _client.runCode(request);
      final pipelineUuid = runCodeResponse.pipelineUuid;
      yield* _checkPipelineExecution(
        pipelineUuid,
        request,
        prevResult: initResult,
      );
    } on RunCodeError catch (error) {
      yield RunCodeResult(
        status: RunCodeStatus.unknownError,
        errorMessage: error.message ?? kUnknownErrorText,
        output: error.message ?? kUnknownErrorText,
      );
    }
  }

  Future<void> cancelExecution(String pipelineUuid) {
    return _client.cancelExecution(pipelineUuid);
  }

  Stream<RunCodeResult> _checkPipelineExecution(
    String pipelineUuid,
    RunCodeRequestWrapper request, {
    RunCodeResult? prevResult,
  }) async* {
    try {
      final statusResponse = await runWithRetry(
        () => _client.checkStatus(pipelineUuid, request),
      );
      final result = await _getPipelineResult(
        pipelineUuid,
        statusResponse.status,
        prevResult,
        request,
      );
      yield result;
      if (!result.isFinished) {
        await Future.delayed(kPipelineCheckDelay);
        yield* _checkPipelineExecution(
          pipelineUuid,
          request,
          prevResult: result,
        );
      }
    } on RunCodeError catch (error) {
      yield RunCodeResult(
        pipelineUuid: prevResult?.pipelineUuid,
        status: RunCodeStatus.unknownError,
        errorMessage: error.message ?? kUnknownErrorText,
        output: error.message ?? kUnknownErrorText,
      );
    }
  }

  Future<RunCodeResult> _getPipelineResult(
    String pipelineUuid,
    RunCodeStatus status,
    RunCodeResult? prevResult,
    RunCodeRequestWrapper request,
  ) async {
    final prevOutput = prevResult?.output ?? '';
    final prevLog = prevResult?.log ?? '';
    switch (status) {
      case RunCodeStatus.compileError:
        final compileOutput = await _client.getCompileOutput(
          pipelineUuid,
          request,
        );
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          output: compileOutput.output,
          log: prevLog,
        );
      case RunCodeStatus.timeout:
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          errorMessage: kTimeoutErrorText,
          output: kTimeoutErrorText,
        );
      case RunCodeStatus.runError:
        final output = await _client.getRunErrorOutput(pipelineUuid, request);
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          output: output.output,
          log: prevLog,
        );
      case RunCodeStatus.unknownError:
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          errorMessage: kUnknownErrorText,
          output: kUnknownErrorText,
          log: prevLog,
        );
      case RunCodeStatus.executing:
      case RunCodeStatus.finished:
        final responses = await Future.wait([
          _client.getRunOutput(pipelineUuid, request),
          _client.getLogOutput(pipelineUuid, request)
        ]);
        final output = responses[0];
        final log = responses[1];
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          output: prevOutput + output.output,
          log: prevLog + log.output,
        );
      default:
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
        );
    }
  }
}
