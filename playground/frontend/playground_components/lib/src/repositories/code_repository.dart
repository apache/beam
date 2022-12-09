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

import '../util/run_with_retry.dart';
import 'code_client/code_client.dart';
import 'models/output_response.dart';
import 'models/run_code_error.dart';
import 'models/run_code_request.dart';
import 'models/run_code_result.dart';

const kPipelineCheckDelay = Duration(seconds: 1);
const kTimeoutErrorText =
    'Pipeline exceeded Playground execution timeout and was terminated. '
    'We recommend installing Apache Beam '
    'https://beam.apache.org/get-started/downloads/ '
    'to try examples without timeout limitation.';
const kUnknownErrorText =
    'Something went wrong. Please try again later or create a GitHub issue';
const kProcessingStartedText = 'The processing has started\n';

// TODO(alexeyinkin): Rename. This is not a repository but a higher level client.
class CodeRepository {
  final CodeClient _client;

  CodeRepository({required CodeClient client,}): _client = client;

  Stream<RunCodeResult> runCode(RunCodeRequest request) async* {
    try {
      const initResult = RunCodeResult(
        status: RunCodeStatus.preparation,
        log: kProcessingStartedText,
      );
      yield initResult;

      final runCodeResponse = await _client.runCode(request);
      final pipelineUuid = runCodeResponse.pipelineUuid;

      yield* _checkPipelineExecution(
        pipelineUuid,
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
    String pipelineUuid, {
    RunCodeResult? prevResult,
  }) async* {
    try {
      final statusResponse = await runWithRetry(
        () => _client.checkStatus(pipelineUuid),
      );
      final result = await _getPipelineResult(
        pipelineUuid,
        statusResponse.status,
        prevResult,
      );
      yield result;
      if (!result.isFinished) {
        await Future.delayed(kPipelineCheckDelay);
        yield* _checkPipelineExecution(
          pipelineUuid,
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
  ) async {
    final prevOutput = prevResult?.output ?? '';
    final prevLog = prevResult?.log ?? '';
    final prevGraph = prevResult?.graph ?? '';

    switch (status) {
      case RunCodeStatus.compileError:
        final compileOutput = await _client.getCompileOutput(pipelineUuid);
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          output: compileOutput.output,
          log: prevLog,
          graph: prevGraph,
        );

      case RunCodeStatus.timeout:
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          errorMessage: kTimeoutErrorText,
          output: kTimeoutErrorText,
          log: prevLog,
          graph: prevGraph,
        );

      case RunCodeStatus.runError:
        final output = await _client.getRunErrorOutput(pipelineUuid);
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          output: output.output,
          log: prevLog,
          graph: prevGraph,
        );

      case RunCodeStatus.validationError:
        final output =
            await _client.getValidationErrorOutput(pipelineUuid);
        return RunCodeResult(
          status: status,
          output: output.output,
          log: prevLog,
          graph: prevGraph,
        );

      case RunCodeStatus.preparationError:
        final output =
            await _client.getPreparationErrorOutput(pipelineUuid);
        return RunCodeResult(
          status: status,
          output: output.output,
          log: prevLog,
          graph: prevGraph,
        );

      case RunCodeStatus.unknownError:
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          errorMessage: kUnknownErrorText,
          output: kUnknownErrorText,
          log: prevLog,
          graph: prevGraph,
        );

      case RunCodeStatus.executing:
        final responses = await Future.wait([
          _client.getRunOutput(pipelineUuid),
          _client.getLogOutput(pipelineUuid),
          prevGraph.isEmpty
              ? _client.getGraphOutput(pipelineUuid)
              : Future.value(OutputResponse(output: prevGraph)),
        ]);
        final output = responses[0];
        final log = responses[1];
        final graph = responses[2];
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          output: prevOutput + output.output,
          log: prevLog + log.output,
          graph: graph.output,
        );

      case RunCodeStatus.finished:
        final responses = await Future.wait([
          _client.getRunOutput(pipelineUuid),
          _client.getLogOutput(pipelineUuid),
          _client.getRunErrorOutput(pipelineUuid),
          prevGraph.isEmpty
              ? _client.getGraphOutput(pipelineUuid)
              : Future.value(OutputResponse(output: prevGraph)),
        ]);
        final output = responses[0];
        final log = responses[1];
        final error = responses[2];
        final graph = responses[3];
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          status: status,
          output: prevOutput + output.output + error.output,
          log: prevLog + log.output,
          graph: graph.output,
        );

      default:
        return RunCodeResult(
          pipelineUuid: pipelineUuid,
          log: prevLog,
          status: status,
          graph: prevGraph,
        );
    }
  }
}
