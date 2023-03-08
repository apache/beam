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

import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:playground_components/src/models/sdk.dart';
import 'package:playground_components/src/repositories/code_client/code_client.dart';
import 'package:playground_components/src/repositories/code_repository.dart';
import 'package:playground_components/src/repositories/models/check_status_response.dart';
import 'package:playground_components/src/repositories/models/output_response.dart';
import 'package:playground_components/src/repositories/models/run_code_request.dart';
import 'package:playground_components/src/repositories/models/run_code_response.dart';
import 'package:playground_components/src/repositories/models/run_code_result.dart';

import 'code_repository_test.mocks.dart';

const kRequestMock = RunCodeRequest(
  code: 'code',
  sdk: Sdk.java,
  pipelineOptions: {},
);

const kPipelineUuid = '1234';
const kRunOutput = 'RunOutput';
const kLogOutput = 'LogOutput';
const kCompileOutput = 'CompileOutput';
const kGraphOutput = 'GraphOutput';
const kRunErrorOutput = 'RunErrorOutput';
const kPreparationErrorOutput = 'PreparationErrorOutput';
const kValidationErrorOutput = 'ValidationErrorOutput';

const kRunCodeResponse = RunCodeResponse(pipelineUuid: kPipelineUuid);
const kFinishedStatusResponse = CheckStatusResponse(status: RunCodeStatus.finished,);
const kErrorStatusResponse = CheckStatusResponse(status: RunCodeStatus.unknownError,);
const kRunErrorStatusResponse = CheckStatusResponse(status: RunCodeStatus.runError,);
const kExecutingStatusResponse = CheckStatusResponse(status: RunCodeStatus.executing,);
const kCompileErrorStatusResponse =
    CheckStatusResponse(status: RunCodeStatus.compileError,);
const kValidationErrorStatusResponse =
    CheckStatusResponse(status: RunCodeStatus.validationError,);
const kPreparationErrorStatusResponse =
    CheckStatusResponse(status: RunCodeStatus.preparationError,);

const kRunOutputResponse = OutputResponse(output: kRunOutput);
const kLogOutputResponse = OutputResponse(output: kLogOutput);
const kCompileOutputResponse = OutputResponse(output: kCompileOutput);
const kRunErrorOutputResponse = OutputResponse(output: kRunErrorOutput);
const kGraphResponse = OutputResponse(output: kGraphOutput);
const kValidationErrorOutputResponse = OutputResponse(output: kValidationErrorOutput);
const kPreparationErrorOutputResponse = OutputResponse(output: kPreparationErrorOutput);

@GenerateMocks([CodeClient])
void main() {
  group('CodeRepository runCode', () {
    test('should complete finished run code request', () async {
      // stubs
      final client = MockCodeClient();
      when(client.runCode(kRequestMock)).thenAnswer(
        (_) async => kRunCodeResponse,
      );
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kFinishedStatusResponse,
      );
      when(client.getRunOutput(kPipelineUuid)).thenAnswer(
        (_) async => kRunOutputResponse,
      );
      when(client.getCompileOutput(kPipelineUuid)).thenAnswer(
        (_) async => kCompileOutputResponse,
      );
      when(client.getRunErrorOutput(kPipelineUuid)).thenAnswer(
        (_) async => kRunErrorOutputResponse,
      );
      when(client.getLogOutput(kPipelineUuid)).thenAnswer(
        (_) async => kLogOutputResponse,
      );
      when(client.getGraphOutput(kPipelineUuid)).thenAnswer(
        (_) async => kGraphResponse,
      );

      // test variables
      final repository = CodeRepository(client: client);
      final stream = repository.runCode(kRequestMock);

      // test assertion
      await expectLater(
        stream,
        emitsInOrder([
          RunCodeResult(
            status: RunCodeStatus.preparation,
            log: kProcessingStartedText,
          ),
          RunCodeResult(
            pipelineUuid: kPipelineUuid,
            status: RunCodeStatus.finished,
            output: kRunOutput + kRunErrorOutput,
            log: kProcessingStartedText + kLogOutput,
            graph: kGraphOutput
          ),
        ]),
      );
      // compile output should not be called
      verifyNever(client.getCompileOutput(kPipelineUuid));
    });

    test('should return output from compilation if failed', () async {
      // stubs
      final client = MockCodeClient();
      when(client.runCode(kRequestMock)).thenAnswer(
        (_) async => kRunCodeResponse,
      );
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kCompileErrorStatusResponse,
      );
      when(client.getCompileOutput(kPipelineUuid)).thenAnswer(
        (_) async => kCompileOutputResponse,
      );
      when(client.getRunOutput(kPipelineUuid)).thenAnswer(
        (_) async => kRunOutputResponse,
      );
      when(client.getLogOutput(kPipelineUuid)).thenAnswer(
        (_) async => kLogOutputResponse,
      );
      when(client.getGraphOutput(kPipelineUuid)).thenAnswer(
            (_) async => kGraphResponse,
      );

      // test variables
      final repository = CodeRepository(client: client);
      final stream = repository.runCode(kRequestMock);

      // test assertion
      await expectLater(
        stream,
        emitsInOrder([
          RunCodeResult(
            status: RunCodeStatus.preparation,
            log: kProcessingStartedText,
          ),
          RunCodeResult(
            pipelineUuid: kPipelineUuid,
            status: RunCodeStatus.compileError,
            output: kCompileOutput,
            log: kProcessingStartedText,
            graph: '',
          ),
        ]),
      );
    });

    test('should return validation error output for validation error',
        () async {
      // stubs
      final client = MockCodeClient();
      when(client.runCode(kRequestMock)).thenAnswer(
        (_) async => kRunCodeResponse,
      );
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kValidationErrorStatusResponse,
      );
      when(client.getValidationErrorOutput(kPipelineUuid))
          .thenAnswer(
        (_) async => kValidationErrorOutputResponse,
      );
      when(client.getGraphOutput(kPipelineUuid)).thenAnswer(
            (_) async => kGraphResponse,
      );

      // test variables
      final repository = CodeRepository(client: client);
      final stream = repository.runCode(kRequestMock);

      // test assertion
      await expectLater(
        stream,
        emitsInOrder([
          RunCodeResult(
            status: RunCodeStatus.preparation,
            log: kProcessingStartedText,
          ),
          RunCodeResult(
            status: RunCodeStatus.validationError,
            output: kValidationErrorOutput,
            log: kProcessingStartedText,
            graph: '',
          ),
        ]),
      );
    });

    test('should return preparation error output for preparation error',
        () async {
      // stubs
      final client = MockCodeClient();
      when(client.runCode(kRequestMock)).thenAnswer(
        (_) async => kRunCodeResponse,
      );
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kPreparationErrorStatusResponse,
      );
      when(client.getPreparationErrorOutput(kPipelineUuid))
          .thenAnswer(
        (_) async => kPreparationErrorOutputResponse,
      );
      when(client.getGraphOutput(kPipelineUuid)).thenAnswer(
            (_) async => kGraphResponse,
      );

      // test variables
      final repository = CodeRepository(client: client);
      final stream = repository.runCode(kRequestMock);

      // test assertion
      await expectLater(
        stream,
        emitsInOrder([
          RunCodeResult(
            status: RunCodeStatus.preparation,
            log: kProcessingStartedText,
          ),
          RunCodeResult(
            status: RunCodeStatus.preparationError,
            output: kPreparationErrorOutput,
            log: kProcessingStartedText,
            graph: '',
          ),
        ]),
      );
    });

    test('should return output from runError if failed while running',
        () async {
      // stubs
      final client = MockCodeClient();
      when(client.runCode(kRequestMock)).thenAnswer(
        (_) async => kRunCodeResponse,
      );
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kRunErrorStatusResponse,
      );
      when(client.getCompileOutput(kPipelineUuid)).thenAnswer(
        (_) async => kCompileOutputResponse,
      );
      when(client.getRunOutput(kPipelineUuid)).thenAnswer(
        (_) async => kRunOutputResponse,
      );
      when(client.getRunErrorOutput(kPipelineUuid)).thenAnswer(
        (_) async => kRunErrorOutputResponse,
      );
      when(client.getLogOutput(kPipelineUuid)).thenAnswer(
        (_) async => kLogOutputResponse,
      );
      when(client.getGraphOutput(kPipelineUuid)).thenAnswer(
            (_) async => kGraphResponse,
      );

      // test variables
      final repository = CodeRepository(client: client);
      final stream = repository.runCode(kRequestMock);

      // test assertion
      await expectLater(
        stream,
        emitsInOrder([
          RunCodeResult(
            status: RunCodeStatus.preparation,
            log: kProcessingStartedText,
          ),
          RunCodeResult(
            pipelineUuid: kPipelineUuid,
            status: RunCodeStatus.runError,
            output: kRunErrorOutput,
            log: kProcessingStartedText,
            graph: '',
          ),
        ]),
      );
    });
  });

  test('should return full output and log using streaming api when finished',
      () async {
    // stubs
    final client = MockCodeClient();
    when(client.runCode(kRequestMock)).thenAnswer(
      (_) async => kRunCodeResponse,
    );
    final answers = [
      kExecutingStatusResponse,
      kExecutingStatusResponse,
      kFinishedStatusResponse
    ];
    when(client.checkStatus(kPipelineUuid)).thenAnswer(
      (_) async => answers.removeAt(0),
    );
    when(client.getRunOutput(kPipelineUuid)).thenAnswer(
      (_) async => kRunOutputResponse,
    );
    when(client.getRunErrorOutput(kPipelineUuid)).thenAnswer(
      (_) async => kRunErrorOutputResponse,
    );
    when(client.getLogOutput(kPipelineUuid)).thenAnswer(
      (_) async => kLogOutputResponse,
    );
    when(client.getGraphOutput(kPipelineUuid)).thenAnswer(
          (_) async => kGraphResponse,
    );

    // test variables
    final repository = CodeRepository(client: client);
    final stream = repository.runCode(kRequestMock);

    // test assertion
    await expectLater(
      stream,
      emitsInOrder([
        RunCodeResult(
          status: RunCodeStatus.preparation,
          log: kProcessingStartedText,
        ),
        RunCodeResult(
          pipelineUuid: kPipelineUuid,
          status: RunCodeStatus.executing,
          output: kRunOutput,
          log: kProcessingStartedText + kLogOutput,
          graph: kGraphOutput,
        ),
        RunCodeResult(
          pipelineUuid: kPipelineUuid,
          status: RunCodeStatus.executing,
          output: kRunOutput * 2,
          log: kProcessingStartedText + kLogOutput * 2,
          graph: kGraphOutput,
        ),
        RunCodeResult(
          pipelineUuid: kPipelineUuid,
          status: RunCodeStatus.finished,
          output: kRunOutput * 3 + kRunErrorOutput,
          log: kProcessingStartedText + kLogOutput * 3,
          graph: kGraphOutput,
        ),
      ]),
    );
  });
}
