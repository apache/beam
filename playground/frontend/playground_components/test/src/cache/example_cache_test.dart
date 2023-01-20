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

import 'dart:collection';

import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:playground_components/src/cache/example_cache.dart';
import 'package:playground_components/src/models/sdk.dart';

import '../common/categories.dart';
import '../common/example_repository_mock.dart';
import '../common/example_repository_mock.mocks.dart';
import '../common/examples.dart';
import '../common/requests.dart';

final _defaultExamplesMapMock = UnmodifiableMapView({
  Sdk.java: examplePython3,
  Sdk.go: examplePython3,
  Sdk.python: examplePython3,
  Sdk.scio: examplePython3,
});

void main() {
  late ExampleCache cache;
  late MockExampleRepository mockRepo;

  setUp(() {
    mockRepo = getMockExampleRepository();
    cache = ExampleCache(exampleRepository: mockRepo);
  });

  group('ExampleCache', () {
    test('defaultExamplesBySdk is initially empty', () {
      expect(cache.defaultExamplesBySdk, {});
    });

    test('isSelectorOpened is initially false', () {
      expect(cache.isSelectorOpened, false);
    });

    test(
      'loadAllPrecompiledObjectsIfNot loads all',
      () async {
        int notified = 0;
        cache.addListener(() => notified++);
        when(mockRepo.getPrecompiledObjects(kGetPrecompiledObjectsRequest))
            .thenAnswer((_) async => kGetPrecompiledObjectsResponse.categories);

        await cache.loadAllPrecompiledObjectsIfNot();

        expect(notified, 1);
        expect(cache.categoryListsBySdk, sdkCategoriesFromServerMock);
      },
    );

    group('loadExampleInfo tests', () {
      test(
        'If example info is fetched (source is not empty), '
        'then loadExampleInfo should return example immediately',
        () async {
          expect(
            await cache.loadExampleInfo(examplePython1),
            examplePython1,
          );
        },
      );

      test(
        'loadExampleInfo loads source, output, logs, graph for given example',
        () async {
          when(mockRepo.getPrecompiledObjectOutput(kRequestForExampleInfo))
              .thenAnswer((_) async => examplePython3.outputs!);
          when(mockRepo.getPrecompiledObjectCode(kRequestForExampleInfo))
              .thenAnswer((_) async => examplePython3.files);
          when(mockRepo.getPrecompiledObjectLogs(kRequestForExampleInfo))
              .thenAnswer((_) async => examplePython3.logs!);
          when(mockRepo.getPrecompiledObjectGraph(kRequestForExampleInfo))
              .thenAnswer((_) async => examplePython3.graph!);

          expect(
            await cache.loadExampleInfo(exampleBasePython3),
            examplePython3,
          );
        },
      );
    });

    group('loadDefaultExamples tests', () {
      test(
        'If defaultExamplesBySdk is not empty, '
        'loadDefaultExamples should not change it',
        () async {
          cache.defaultExamplesBySdk.addAll(_defaultExamplesMapMock);
          await cache.loadDefaultPrecompiledObjects();
          expect(cache.defaultExamplesBySdk, _defaultExamplesMapMock);
        },
      );

      test(
        'Example state loadDefaultExamples should load default example '
        'with all additions for every Sdk',
        () async {
          // stubs
          when(mockRepo.getPrecompiledObjectOutput(kRequestForExampleInfo))
              .thenAnswer((_) async => examplePython3.outputs!);
          when(mockRepo.getPrecompiledObjectCode(kRequestForExampleInfo))
              .thenAnswer((_) async => examplePython3.files);
          when(mockRepo.getPrecompiledObjectLogs(kRequestForExampleInfo))
              .thenAnswer((_) async => examplePython3.logs!);
          when(mockRepo.getPrecompiledObjectGraph(kRequestForExampleInfo))
              .thenAnswer((_) async => examplePython3.graph!);

          // test assertion
          await cache.loadDefaultPrecompiledObjects();
          expect(
            cache.defaultExamplesBySdk,
            _defaultExamplesMapMock,
          );
        },
      );
    });
  });
}
