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
import 'package:playground/modules/editor/repository/code_repository/code_client/check_status_response.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/code_client.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/output_response.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/run_code_response.dart';
import 'package:playground/modules/editor/repository/code_repository/code_repository.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_request.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_result.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

import 'code_repository_test.mocks.dart';

final kRequestMock = RunCodeRequestWrapper(
  code: 'code',
  sdk: SDK.java,
  pipelineOptions: {},
);

const kPipelineUuid = '1234';
const kRunOutput = 'RunOutput';
const kLogOutput = 'LogOutput';
const kCompileOutput = 'CompileOutput';
const kRunErrorOutput = 'RunErrorOutput';
const kPreparationErrorOutput = 'PreparationErrorOutput';
const kValidationErrorOutput = 'ValidationErrorOutput';

final kRunCodeResponse = RunCodeResponse(kPipelineUuid);
final kFinishedStatusResponse = CheckStatusResponse(RunCodeStatus.finished);
final kErrorStatusResponse = CheckStatusResponse(RunCodeStatus.unknownError);
final kRunErrorStatusResponse = CheckStatusResponse(RunCodeStatus.runError);
final kExecutingStatusResponse = CheckStatusResponse(RunCodeStatus.executing);
final kCompileErrorStatusResponse =
    CheckStatusResponse(RunCodeStatus.compileError);
final kValidationErrorStatusResponse =
    CheckStatusResponse(RunCodeStatus.validationError);
final kPreparationErrorStatusResponse =
    CheckStatusResponse(RunCodeStatus.preparationError);

final kRunOutputResponse = OutputResponse(kRunOutput);
final kLogOutputResponse = OutputResponse(kLogOutput);
final kCompileOutputResponse = OutputResponse(kCompileOutput);
final kRunErrorOutputResponse = OutputResponse(kRunErrorOutput);
final kValidationErrorOutputResponse = OutputResponse(kValidationErrorOutput);
final kPreparationErrorOutputResponse = OutputResponse(kPreparationErrorOutput);

@GenerateMocks([CodeClient])
void main() {
  group('CodeRepository runCode', () {
    test('should complete finished run code request', () async {
      // stubs
      final client = MockCodeClient();
      when(client.runCode(kRequestMock)).thenAnswer(
        (_) async => kRunCodeResponse,
      );
      when(client.checkStatus(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kFinishedStatusResponse,
      );
      when(client.getRunOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kRunOutputResponse,
      );
      when(client.getCompileOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kCompileOutputResponse,
      );
      when(client.getRunErrorOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kRunErrorOutputResponse,
      );
      when(client.getLogOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kLogOutputResponse,
      );

      // test variables
      final repository = CodeRepository(client);
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
          ),
        ]),
      );
      // compile output should not be called
      verifyNever(client.getCompileOutput(kPipelineUuid, kRequestMock));
    });

    test('should return output from compilation if failed', () async {
      // stubs
      final client = MockCodeClient();
      when(client.runCode(kRequestMock)).thenAnswer(
        (_) async => kRunCodeResponse,
      );
      when(client.checkStatus(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kCompileErrorStatusResponse,
      );
      when(client.getCompileOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kCompileOutputResponse,
      );
      when(client.getRunOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kRunOutputResponse,
      );
      when(client.getLogOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kLogOutputResponse,
      );

      // test variables
      final repository = CodeRepository(client);
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
      when(client.checkStatus(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kValidationErrorStatusResponse,
      );
      when(client.getValidationErrorOutput(kPipelineUuid, kRequestMock))
          .thenAnswer(
        (_) async => kValidationErrorOutputResponse,
      );

      // test variables
      final repository = CodeRepository(client);
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
      when(client.checkStatus(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kPreparationErrorStatusResponse,
      );
      when(client.getPreparationErrorOutput(kPipelineUuid, kRequestMock))
          .thenAnswer(
        (_) async => kPreparationErrorOutputResponse,
      );

      // test variables
      final repository = CodeRepository(client);
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
      when(client.checkStatus(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kRunErrorStatusResponse,
      );
      when(client.getCompileOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kCompileOutputResponse,
      );
      when(client.getRunOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kRunOutputResponse,
      );
      when(client.getRunErrorOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kRunErrorOutputResponse,
      );
      when(client.getLogOutput(kPipelineUuid, kRequestMock)).thenAnswer(
        (_) async => kLogOutputResponse,
      );

      // test variables
      final repository = CodeRepository(client);
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
    when(client.checkStatus(kPipelineUuid, kRequestMock)).thenAnswer(
      (_) async => answers.removeAt(0),
    );
    when(client.getRunOutput(kPipelineUuid, kRequestMock)).thenAnswer(
      (_) async => kRunOutputResponse,
    );
    when(client.getRunErrorOutput(kPipelineUuid, kRequestMock)).thenAnswer(
      (_) async => kRunErrorOutputResponse,
    );
    when(client.getLogOutput(kPipelineUuid, kRequestMock)).thenAnswer(
      (_) async => kLogOutputResponse,
    );

    // test variables
    final repository = CodeRepository(client);
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
        ),
        RunCodeResult(
          pipelineUuid: kPipelineUuid,
          status: RunCodeStatus.executing,
          output: kRunOutput * 2,
          log: kProcessingStartedText + kLogOutput * 2,
        ),
        RunCodeResult(
          pipelineUuid: kPipelineUuid,
          status: RunCodeStatus.finished,
          output: kRunOutput * 3 + kRunErrorOutput,
          log: kProcessingStartedText + kLogOutput * 3,
        ),
      ]),
    );
  });
}
