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
import 'package:playground/modules/examples/repositories/example_client/example_client.dart';
import 'package:playground/modules/examples/repositories/example_repository.dart';

import '../../../../pages/playground/states/mocks/request_mock.dart';
import 'example_repository_test.mocks.dart';

@GenerateMocks([ExampleClient])
void main() {
  late ExampleRepository repo;
  late ExampleClient client;

  setUp(
    () {
      client = MockExampleClient();
      repo = ExampleRepository(client);
    },
  );

  test(
    'Example repository getListOfExamples should return response with categories',
    () async {
      when(client.getListOfExamples(kGetListOfExamplesRequestMock))
          .thenAnswer((_) async => kGetListOfExamplesResponseMock);
      expect(
        await repo.getListOfExamples(kGetListOfExamplesRequestMock),
        kGetListOfExamplesResponseMock.categories,
      );
      verify(client.getListOfExamples(kGetListOfExamplesRequestMock)).called(1);
    },
  );

  test(
    'Example repository getDefaultExample should return defaultExample for chosen Sdk',
    () async {
      when(client.getDefaultExample(kGetExampleRequestMock))
          .thenAnswer((_) async => kGetExampleResponseMock);
      expect(
        await repo.getDefaultExample(kGetExampleRequestMock),
        kGetExampleResponseMock.example,
      );
      verify(client.getDefaultExample(kGetExampleRequestMock)).called(1);
    },
  );

  test(
    'Example repository getExampleSource should return source code for example',
    () async {
      when(client.getExampleSource(kGetExampleRequestMock))
          .thenAnswer((_) async => kGetExampleCodeResponseMock);
      expect(
        await repo.getExampleSource(kGetExampleRequestMock),
        kGetExampleCodeResponseMock.code,
      );
      verify(client.getExampleSource(kGetExampleRequestMock)).called(1);
    },
  );

  test(
    'Example repository getExampleOutput should return output for example',
    () async {
      when(client.getExampleOutput(kGetExampleRequestMock))
          .thenAnswer((_) async => kOutputResponseMock);
      expect(
        await repo.getExampleOutput(kGetExampleRequestMock),
        kOutputResponseMock.output,
      );
      verify(client.getExampleOutput(kGetExampleRequestMock)).called(1);
    },
  );

  test(
    'Example repository getExampleLogs should return logs for example',
    () async {
      when(client.getExampleLogs(kGetExampleRequestMock))
          .thenAnswer((_) async => kOutputResponseMock);
      expect(
        await repo.getExampleLogs(kGetExampleRequestMock),
        kOutputResponseMock.output,
      );
      verify(client.getExampleLogs(kGetExampleRequestMock)).called(1);
    },
  );

  test(
    'Example repository getExampleLogs should return logs for example',
    () async {
      when(client.getExampleGraph(kGetExampleRequestMock))
          .thenAnswer((_) async => kOutputResponseMock);
      expect(
        await repo.getExampleGraph(kGetExampleRequestMock),
        kOutputResponseMock.output,
      );
      verify(client.getExampleGraph(kGetExampleRequestMock)).called(1);
    },
  );

  test(
    'Example repository getExample should return ExampleModel',
        () async {
      when(client.getExample(kGetExampleRequestMock))
          .thenAnswer((_) async => kGetExampleResponseMock);
      expect(
        await repo.getExample(kGetExampleRequestMock),
        kGetExampleResponseMock.example,
      );
      verify(client.getExample(kGetExampleRequestMock)).called(1);
    },
  );
}
