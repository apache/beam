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
import 'package:playground/modules/editor/repository/code_repository/code_client/check_status_response.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/code_client.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/output_response.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/run_code_response.dart';
import 'package:playground/modules/editor/repository/code_repository/code_repository.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_request.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_result.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:mockito/mockito.dart';

import 'code_repository_test.mocks.dart';

final kRequestMock = RunCodeRequestWrapper(
  code: 'code',
  sdk: SDK.java,
);

const kPipelineUuid = "1234";
const kRunOutput = "RunOutput";
const kCompileOutput = "CompileOutput";

final kRunCodeResponse = RunCodeResponse(kPipelineUuid);
final kFinishedStatusResponse = CheckStatusResponse(RunCodeStatus.finished);
final kErrorStatusResponse = CheckStatusResponse(RunCodeStatus.error);
final kCompileErrorStatusResponse =
    CheckStatusResponse(RunCodeStatus.compileError);
final kRunOutputResponse = OutputResponse(kRunOutput);
final kCompileOutputResponse = OutputResponse(kCompileOutput);
final kEmptyCompileOutputResponse = OutputResponse("");

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

      // test variables
      final repository = CodeRepository(client);
      final stream = repository.runCode(kRequestMock);

      // test assertion
      await expectLater(
        stream,
        emitsInOrder([
          RunCodeResult(status: RunCodeStatus.executing),
          RunCodeResult(status: RunCodeStatus.finished, output: kRunOutput),
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

      // test variables
      final repository = CodeRepository(client);
      final stream = repository.runCode(kRequestMock);

      // test assertion
      await expectLater(
        stream,
        emitsInOrder([
          RunCodeResult(status: RunCodeStatus.executing),
          RunCodeResult(
              status: RunCodeStatus.compileError, output: kCompileOutput),
        ]),
      );
    });

    test('should return output from run if failed and compile output is empty',
        () async {
      // stubs
      final client = MockCodeClient();
      when(client.runCode(kRequestMock)).thenAnswer(
        (_) async => kRunCodeResponse,
      );
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kErrorStatusResponse,
      );
      when(client.getCompileOutput(kPipelineUuid)).thenAnswer(
        (_) async => kEmptyCompileOutputResponse,
      );
      when(client.getRunOutput(kPipelineUuid)).thenAnswer(
        (_) async => kRunOutputResponse,
      );

      // test variables
      final repository = CodeRepository(client);
      final stream = repository.runCode(kRequestMock);

      // test assertion
      await expectLater(
        stream,
        emitsInOrder([
          RunCodeResult(status: RunCodeStatus.executing),
          RunCodeResult(status: RunCodeStatus.error, output: kRunOutput),
        ]),
      );
    });
  });
}
