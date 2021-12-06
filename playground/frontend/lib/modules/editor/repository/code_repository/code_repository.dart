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

const kPipelineCheckDelay = Duration(seconds: 1);
const kTimeoutErrorText = 'Code execution exceeded timeout';
const kUnknownErrorText =
    'Something went wrong. Please try again later or create a jira ticket';

class CodeRepository {
  late final CodeClient _client;

  CodeRepository(CodeClient client) {
    _client = client;
  }

  Stream<RunCodeResult> runCode(RunCodeRequestWrapper request) async* {
    try {
      yield RunCodeResult(status: RunCodeStatus.preparation);
      var runCodeResponse = await _client.runCode(request);
      final pipelineUuid = runCodeResponse.pipelineUuid;
      yield* _checkPipelineExecution(pipelineUuid, request);
    } on RunCodeError catch (error) {
      yield RunCodeResult(
        status: RunCodeStatus.unknownError,
        errorMessage: error.message ?? kUnknownErrorText,
      );
    }
  }

  Stream<RunCodeResult> _checkPipelineExecution(
    String pipelineUuid,
    RunCodeRequestWrapper request, {
    RunCodeResult? prevResult,
  }) async* {
    final statusResponse = await _client.checkStatus(pipelineUuid, request);
    final result = await _getPipelineResult(
      pipelineUuid,
      statusResponse.status,
      prevResult,
      request,
    );
    yield result;
    if (!result.isFinished) {
      await Future.delayed(kPipelineCheckDelay);
      yield* _checkPipelineExecution(pipelineUuid, request, prevResult: result);
    }
  }

  Future<RunCodeResult> _getPipelineResult(
    String pipelineUuid,
    RunCodeStatus status,
    RunCodeResult? prevResult,
    RunCodeRequestWrapper request,
  ) async {
    final prevOutput = prevResult?.output ?? '';
    switch (status) {
      case RunCodeStatus.compileError:
        final compileOutput = await _client.getCompileOutput(
          pipelineUuid,
          request,
        );
        return RunCodeResult(status: status, output: compileOutput.output);
      case RunCodeStatus.timeout:
        return RunCodeResult(status: status, errorMessage: kTimeoutErrorText);
      case RunCodeStatus.runError:
        final output = await _client.getRunErrorOutput(pipelineUuid, request);
        return RunCodeResult(status: status, output: output.output);
      case RunCodeStatus.unknownError:
        return RunCodeResult(status: status, errorMessage: kUnknownErrorText);
      case RunCodeStatus.executing:
        final output = await _client.getRunOutput(pipelineUuid, request);
        return RunCodeResult(
          status: status,
          output: prevOutput + output.output,
        );
      case RunCodeStatus.finished:
        final output = await _client.getRunOutput(pipelineUuid, request);
        return RunCodeResult(
            status: status, output: prevOutput + output.output);
      default:
        return RunCodeResult(status: status);
    }
  }
}
