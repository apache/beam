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

final kDefaultExamplesMapMock = UnmodifiableMapView({
  Sdk.java: exampleWithAllAdditionsMock,
  Sdk.go: exampleWithAllAdditionsMock,
  Sdk.python: exampleWithAllAdditionsMock,
  Sdk.scio: exampleWithAllAdditionsMock,
});

void main() {
  late ExampleCache state;
  late MockExampleRepository mockRepo;

  setUp(() {
    mockRepo = getMockExampleRepository();
    state = ExampleCache(exampleRepository: mockRepo, hasCatalog: true);
  });

  test('Initial value of defaultExamplesBySdk should be an empty map', () {
    expect(state.defaultExamplesBySdk, {});
  });

  test('Initial value of isSelectorOpened should be false', () {
    expect(state.isSelectorOpened, false);
  });

  test(
    'Example state init should initiate loading of categoryListsBySdk from server',
    () async {
      when(mockRepo.getListOfExamples(kGetPrecompiledObjectsRequest))
          .thenAnswer((_) async => kGetPrecompiledObjectsResponse.categories);
      await state.init();
      expect(state.categoryListsBySdk, sdkCategoriesFromServerMock);
    },
  );

  test(
    'Example state should notify all listeners about categoryListsBySdk is set',
    () {
      state.addListener(() {
        expect(state.categoryListsBySdk, sdkCategoriesFromServerMock);
      });
      state.setSdkCategories(sdkCategoriesFromServerMock);
    },
  );

  test(
    'Example state changeSelectorVisibility should change isSelectorOpened to !isSelectorOpened',
    () {
      state.changeSelectorVisibility();
      expect(state.isSelectorOpened, true);
      state.changeSelectorVisibility();
      expect(state.isSelectorOpened, false);
    },
  );

  test(
    'Example state getCategories should get the categories list for each SDK',
    () {
      state.setSdkCategories(sdkCategoriesFromServerMock);
      expect(state.getCategories(Sdk.java), categoriesMock);
      expect(state.getCategories(Sdk.go), categoriesMock);
      expect(state.getCategories(Sdk.python), categoriesMock);
      expect(state.getCategories(Sdk.scio), categoriesMock);
    },
  );

  test(
    'Example state getExampleOutput should return output for example',
    () async {
      when(mockRepo.getExampleOutput(kRequestForExampleInfo))
          .thenAnswer((_) async => kOutputResponse.output);
      expect(
        await state.getExampleOutput(
          kRequestForExampleInfo.path,
          kRequestForExampleInfo.sdk,
        ),
        kOutputResponse.output,
      );
    },
  );

  test(
    'Example state getExampleSource should return source code for example',
    () async {
      when(mockRepo.getExampleSource(kRequestForExampleInfo))
          .thenAnswer((_) async => kOutputResponse.output);
      expect(
        await state.getExampleSource(
          kRequestForExampleInfo.path,
          kRequestForExampleInfo.sdk,
        ),
        kOutputResponse.output,
      );
    },
  );

  test(
    'Example state getExampleLogs should return logs for example',
    () async {
      when(mockRepo.getExampleLogs(kRequestForExampleInfo))
          .thenAnswer((_) async => kOutputResponse.output);
      expect(
        await state.getExampleLogs(
          kRequestForExampleInfo.path,
          kRequestForExampleInfo.sdk,
        ),
        kOutputResponse.output,
      );
    },
  );

  test(
    'Example state getExampleGraph should return output for example',
    () async {
      when(mockRepo.getExampleGraph(kRequestForExampleInfo))
          .thenAnswer((_) async => kOutputResponse.output);
      expect(
        await state.getExampleGraph(
          kRequestForExampleInfo.path,
          kRequestForExampleInfo.sdk,
        ),
        kOutputResponse.output,
      );
    },
  );

  group('loadExampleInfo tests', () {
    test(
      'If example info is fetched (source is not empty), '
      'then loadExampleInfo should return example immediately',
      () async {
        expect(
          await state.loadExampleInfo(exampleMock1),
          exampleMock1,
        );
      },
    );

    test(
      'Example state loadExampleInfo should load source, output, logs, graph for given example',
      () async {
        expect(
          await state.loadExampleInfo(exampleWithoutSourceMock),
          exampleWithAllAdditionsMock,
        );
      },
    );
  });

  group('loadDefaultExamples tests', () {
    test(
      'If defaultExamplesBySdk is not empty, then loadDefaultExamples should not change it',
      () async {
        state.defaultExamplesBySdk.addAll(kDefaultExamplesMapMock);
        await state.loadDefaultExamples();
        expect(state.defaultExamplesBySdk, kDefaultExamplesMapMock);
      },
    );
    test(
      'Example state loadDefaultExamples should load default example '
      'with all additions for every Sdk',
      () async {
        // stubs
        when(mockRepo.getExampleOutput(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponse.output);
        when(mockRepo.getExampleSource(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponse.output);
        when(mockRepo.getExampleLogs(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponse.output);
        when(mockRepo.getExampleGraph(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponse.output);

        // test assertion
        await state.loadDefaultExamples();
        expect(
          state.defaultExamplesBySdk,
          kDefaultExamplesMapMock,
        );
      },
    );
  });
}
