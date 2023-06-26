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
import 'package:playground_components/src/cache/example_cache.dart';
import 'package:playground_components/src/controllers/code_runner.dart';
import 'package:playground_components/src/controllers/example_loaders/examples_loader.dart';
import 'package:playground_components/src/controllers/snippet_editing_controller.dart';
import 'package:playground_components/src/models/sdk.dart';
import 'package:playground_components/src/repositories/code_client/code_client.dart';
import 'package:playground_components/src/repositories/models/check_status_response.dart';
import 'package:playground_components/src/repositories/models/output_response.dart';
import 'package:playground_components/src/repositories/models/run_code_response.dart';
import 'package:playground_components/src/repositories/models/run_code_result.dart';

import 'code_runner_test.mocks.dart';

const _sdk = Sdk.java;

const kPipelineUuid = '1234';
const kRunOutput = 'RunOutput';
const kLogOutput = 'LogOutput';
const kCompileOutput = 'CompileOutput';
const kGraphOutput = 'GraphOutput';
const kRunErrorOutput = 'RunErrorOutput';
const kPreparationErrorOutput = 'PreparationErrorOutput';
const kValidationErrorOutput = 'ValidationErrorOutput';

const kRunCodeResponse = RunCodeResponse(pipelineUuid: kPipelineUuid);
const kFinishedStatusResponse = CheckStatusResponse(
  status: RunCodeStatus.finished,
);
const kRunErrorStatusResponse = CheckStatusResponse(
  status: RunCodeStatus.runError,
);
const kCompileErrorStatusResponse = CheckStatusResponse(
  status: RunCodeStatus.compileError,
);
const kValidationErrorStatusResponse = CheckStatusResponse(
  status: RunCodeStatus.validationError,
);
const kPreparationErrorStatusResponse = CheckStatusResponse(
  status: RunCodeStatus.preparationError,
);

const kRunOutputResponse = OutputResponse(output: kRunOutput);
const kLogOutputResponse = OutputResponse(output: kLogOutput);
const kCompileOutputResponse = OutputResponse(output: kCompileOutput);
const kRunErrorOutputResponse = OutputResponse(output: kRunErrorOutput);
const kGraphResponse = OutputResponse(output: kGraphOutput);

const kValidationErrorOutputResponse =
    OutputResponse(output: kValidationErrorOutput);

const kPreparationErrorOutputResponse =
    OutputResponse(output: kPreparationErrorOutput);

@GenerateMocks([CodeClient, ExamplesLoader, ExampleCache])
void main() {
  var results = <RunCodeResult?>[];
  var client = MockCodeClient();
  var runner = CodeRunner(
    snippetEditingControllerGetter: () => SnippetEditingController(sdk: _sdk),
  );

  setUp(() async {
    results = [];
    client = MockCodeClient();
    runner = CodeRunner(
      snippetEditingControllerGetter: () => SnippetEditingController(sdk: _sdk),
      codeClient: client,
    );

    runner.addListener(() {
      results.add(runner.result);
    });

    when(client.runCode(any)).thenAnswer(
      (_) async => kRunCodeResponse,
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
    when(client.getValidationErrorOutput(kPipelineUuid)).thenAnswer(
      (_) async => kValidationErrorOutputResponse,
    );
    when(client.getPreparationErrorOutput(kPipelineUuid)).thenAnswer(
      (_) async => kPreparationErrorOutputResponse,
    );
  });

  group('CodeRunner.runCode', () {
    test('finished', () async {
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kFinishedStatusResponse,
      );

      await runner.runCode();

      expect(
        results,
        const [
          RunCodeResult(
            log: kProcessingStartedText,
            sdk: _sdk,
            status: RunCodeStatus.preparation,
          ),
          RunCodeResult(
            pipelineUuid: kPipelineUuid,
            output: kRunOutput + kRunErrorOutput,
            log: kProcessingStartedText + kLogOutput,
            graph: kGraphOutput,
            sdk: _sdk,
            status: RunCodeStatus.finished,
          ),
        ],
      );

      // compile output should not be called
      verifyNever(client.getCompileOutput(kPipelineUuid));
    });

    test('compileError', () async {
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kCompileErrorStatusResponse,
      );

      await runner.runCode();

      expect(
        results,
        const [
          RunCodeResult(
            log: kProcessingStartedText,
            sdk: _sdk,
            status: RunCodeStatus.preparation,
          ),
          RunCodeResult(
            graph: '',
            log: kProcessingStartedText,
            output: kCompileOutput,
            pipelineUuid: kPipelineUuid,
            sdk: _sdk,
            status: RunCodeStatus.compileError,
          ),
        ],
      );
    });

    test('validationError', () async {
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kValidationErrorStatusResponse,
      );

      await runner.runCode();

      expect(
        results,
        const [
          RunCodeResult(
            log: kProcessingStartedText,
            sdk: _sdk,
            status: RunCodeStatus.preparation,
          ),
          RunCodeResult(
            graph: '',
            log: kProcessingStartedText,
            output: kValidationErrorOutput,
            sdk: _sdk,
            status: RunCodeStatus.validationError,
          ),
        ],
      );
    });

    test('preparationError', () async {
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kPreparationErrorStatusResponse,
      );

      await runner.runCode();

      expect(
        results,
        const [
          RunCodeResult(
            log: kProcessingStartedText,
            sdk: _sdk,
            status: RunCodeStatus.preparation,
          ),
          RunCodeResult(
            graph: '',
            log: kProcessingStartedText,
            output: kPreparationErrorOutput,
            sdk: _sdk,
            status: RunCodeStatus.preparationError,
          ),
        ],
      );
    });

    test('runError', () async {
      when(client.checkStatus(kPipelineUuid)).thenAnswer(
        (_) async => kRunErrorStatusResponse,
      );

      await runner.runCode();

      expect(
        results,
        const [
          RunCodeResult(
            log: kProcessingStartedText,
            sdk: _sdk,
            status: RunCodeStatus.preparation,
          ),
          RunCodeResult(
            graph: '',
            log: kProcessingStartedText,
            output: kRunErrorOutput,
            pipelineUuid: kPipelineUuid,
            sdk: _sdk,
            status: RunCodeStatus.runError,
          ),
        ],
      );
    });
  });
}
