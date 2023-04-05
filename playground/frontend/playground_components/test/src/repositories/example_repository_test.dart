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
import 'package:playground_components/src/repositories/example_client/example_client.dart';
import 'package:playground_components/src/repositories/example_repository.dart';

import '../common/requests.dart';
import 'example_repository_test.mocks.dart';

@GenerateMocks([ExampleClient])
void main() {
  late ExampleRepository repo;
  late ExampleClient client;

  setUp(
    () {
      client = MockExampleClient();
      repo = ExampleRepository(client: client);
    },
  );

  test(
    'Example repository getListOfExamples should return response with categories',
    () async {
      when(client.getPrecompiledObjects(kGetPrecompiledObjectsRequest))
          .thenAnswer((_) async => kGetPrecompiledObjectsResponse);
      expect(
        await repo.getPrecompiledObjects(kGetPrecompiledObjectsRequest),
        kGetPrecompiledObjectsResponse.categories,
      );
      verify(client.getPrecompiledObjects(kGetPrecompiledObjectsRequest))
          .called(1);
    },
  );

  test(
    'Example repository getDefaultExample should return defaultExample for chosen Sdk',
    () async {
      when(client
              .getDefaultPrecompiledObject(kGetDefaultPrecompiledObjectRequest))
          .thenAnswer((_) async => kGetDefaultPrecompiledObjectResponse);
      expect(
        await repo
            .getDefaultPrecompiledObject(kGetDefaultPrecompiledObjectRequest),
        kGetDefaultPrecompiledObjectResponse.example,
      );
      verify(client
              .getDefaultPrecompiledObject(kGetDefaultPrecompiledObjectRequest))
          .called(1);
    },
  );

  test(
    'Example repository getExampleSource should return files for example',
    () async {
      when(client.getPrecompiledObjectCode(kRequestForExampleInfo))
          .thenAnswer((_) async => kGetPrecompiledObjectCodeResponse);
      expect(
        await repo.getPrecompiledObjectCode(kRequestForExampleInfo),
        kGetPrecompiledObjectCodeResponse.files,
      );
      verify(client.getPrecompiledObjectCode(kRequestForExampleInfo)).called(1);
    },
  );

  test(
    'Example repository getExampleOutput should return output for example',
    () async {
      when(client.getPrecompiledObjectOutput(kRequestForExampleInfo))
          .thenAnswer((_) async => kOutputResponse);
      expect(
        await repo.getPrecompiledObjectOutput(kRequestForExampleInfo),
        kOutputResponse.output,
      );
      verify(client.getPrecompiledObjectOutput(kRequestForExampleInfo))
          .called(1);
    },
  );

  test(
    'Example repository getExampleLogs should return logs for example',
    () async {
      when(client.getPrecompiledObjectLogs(kRequestForExampleInfo))
          .thenAnswer((_) async => kOutputResponse);
      expect(
        await repo.getPrecompiledObjectLogs(kRequestForExampleInfo),
        kOutputResponse.output,
      );
      verify(client.getPrecompiledObjectLogs(kRequestForExampleInfo)).called(1);
    },
  );

  test(
    'Example repository getExampleGraph should return logs for example',
    () async {
      when(client.getPrecompiledObjectGraph(kRequestForExampleInfo))
          .thenAnswer((_) async => kOutputResponse);
      expect(
        await repo.getPrecompiledObjectGraph(kRequestForExampleInfo),
        kOutputResponse.output,
      );
      verify(client.getPrecompiledObjectGraph(kRequestForExampleInfo))
          .called(1);
    },
  );

  test(
    'Example repository getExample should return ExampleModel',
    () async {
      when(client.getPrecompiledObject(kRequestForExampleInfo))
          .thenAnswer((_) async => kGetDefaultPrecompiledObjectResponse);
      expect(
        await repo.getPrecompiledObject(kRequestForExampleInfo),
        kGetDefaultPrecompiledObjectResponse.example,
      );
      verify(client.getPrecompiledObject(kRequestForExampleInfo)).called(1);
    },
  );
}
