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

class CodeRepository {
  late final CodeClient _client;

  CodeRepository(CodeClient client) {
    _client = client;
  }

  Stream<RunCodeResult> runCode(RunCodeRequestWrapper request) async* {
    try {
      yield RunCodeResult(status: RunCodeStatus.executing);
      var runCodeResponse = await _client.runCode(request);
      final pipelineUuid = runCodeResponse.pipelineUuid;
      final resultStatus = await _waitPipelineExecution(pipelineUuid);
      final result = await _getPipelineResult(pipelineUuid, resultStatus);
      yield result;
    } on RunCodeError catch (error) {
      yield RunCodeResult(
        status: RunCodeStatus.error,
        errorMessage: error.message,
      );
    }
  }

  Future<RunCodeStatus> _waitPipelineExecution(String pipelineUuid) async {
    final statusResponse = await _client.checkStatus(pipelineUuid);
    final isFinished = statusResponse.status == RunCodeStatus.finished ||
        statusResponse.status == RunCodeStatus.error;
    if (isFinished) {
      return statusResponse.status;
    }

    return Future.delayed(
      kPipelineCheckDelay,
      () => _waitPipelineExecution(pipelineUuid),
    );
  }

  Future<RunCodeResult> _getPipelineResult(
    String pipelineUuid,
    RunCodeStatus status,
  ) async {
    final output = await _getPipelineOutput(pipelineUuid, status);
    return RunCodeResult(status: status, output: output);
  }

  Future<String> _getPipelineOutput(
    String pipelineUuid,
    RunCodeStatus status,
  ) async {
    if (status == RunCodeStatus.error) {
      final compileOutput = await _client.getCompileOutput(pipelineUuid);
      if (compileOutput.output.isNotEmpty) {
        return compileOutput.output;
      }
    }
    final runOutput = await _client.getRunOutput(pipelineUuid);
    return runOutput.output;
  }
}
