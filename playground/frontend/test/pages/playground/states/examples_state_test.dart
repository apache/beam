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
import 'package:playground/modules/examples/repositories/example_repository.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/states/examples_state.dart';

import 'examples_state_test.mocks.dart';
import 'mocks/categories_mock.dart';
import 'mocks/example_mock.dart';
import 'mocks/request_mock.dart';

final kDefaultExamplesMapMock = {
  SDK.java: exampleWithAllAdditionsMock,
  SDK.go: exampleWithAllAdditionsMock,
  SDK.python: exampleWithAllAdditionsMock,
  SDK.scio: exampleWithAllAdditionsMock,
};

@GenerateMocks([ExampleRepository])
void main() {
  late ExampleState state;
  late MockExampleRepository mockRepo;

  setUp(() {
    mockRepo = MockExampleRepository();
    state = ExampleState(mockRepo);
  });

  test('Initial value of defaultExamplesMap should be an empty map', () {
    expect(state.defaultExamplesMap, {});
  });

  test('Initial value of isSelectorOpened should be false', () {
    expect(state.isSelectorOpened, false);
  });

  test(
    'Example state init should initiate loading of sdkCategories from server',
    () async {
      when(mockRepo.getListOfExamples(kGetListOfExamplesRequestMock))
          .thenAnswer((_) async => kGetListOfExamplesResponseMock.categories);
      await state.init();
      expect(state.sdkCategories, sdkCategoriesFromServerMock);
    },
  );

  test(
    'Example state should notify all listeners about sdkCategories is set',
    () {
      state.addListener(() {
        expect(state.sdkCategories, sdkCategoriesFromServerMock);
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
      expect(state.getCategories(SDK.java), categoriesMock);
      expect(state.getCategories(SDK.go), categoriesMock);
      expect(state.getCategories(SDK.python), categoriesMock);
      expect(state.getCategories(SDK.scio), categoriesMock);
    },
  );

  test(
    'Example state getExampleOutput should return output for example',
    () async {
      when(mockRepo.getExampleOutput(kGetExampleRequestMock))
          .thenAnswer((_) async => kOutputResponseMock.output);
      expect(
        await state.getExampleOutput('', SDK.java),
        kOutputResponseMock.output,
      );
    },
  );

  test(
    'Example state getExampleSource should return source code for example',
    () async {
      when(mockRepo.getExampleSource(kGetExampleRequestMock))
          .thenAnswer((_) async => kOutputResponseMock.output);
      expect(
        await state.getExampleSource('', SDK.java),
        kOutputResponseMock.output,
      );
    },
  );

  test(
    'Example state getExampleLogs should return logs for example',
    () async {
      when(mockRepo.getExampleLogs(kGetExampleRequestMock))
          .thenAnswer((_) async => kOutputResponseMock.output);
      expect(
        await state.getExampleLogs('', SDK.java),
        kOutputResponseMock.output,
      );
    },
  );

  test(
    'Example state getExampleGraph should return output for example',
    () async {
      when(mockRepo.getExampleGraph(kGetExampleRequestMock))
          .thenAnswer((_) async => kOutputResponseMock.output);
      expect(
        await state.getExampleGraph('', SDK.java),
        kOutputResponseMock.output,
      );
    },
  );

  group('loadExampleInfo tests', () {
    test(
      'If example info is fetched (source is not empty),'
      'then loadExampleInfo should return example immediately',
      () async {
        expect(
          await state.loadExampleInfo(exampleMock1, SDK.java),
          exampleMock1,
        );
      },
    );

    test(
      'Example state loadExampleInfo should load source, output, logs, graph for given example',
      () async {
        // stubs
        when(mockRepo.getExampleOutput(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponseMock.output);
        when(mockRepo.getExampleSource(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponseMock.output);
        when(mockRepo.getExampleLogs(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponseMock.output);
        when(mockRepo.getExampleGraph(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponseMock.output);

        // test assertion
        expect(
          await state.loadExampleInfo(exampleWithoutSourceMock, SDK.java),
          exampleWithAllAdditionsMock,
        );
      },
    );
  });

  group('loadDefaultExamples tests', () {
    test(
      'If defaultExamplesMap is not empty, then loadDefaultExamples should not change it',
      () async {
        state.defaultExamplesMap = kDefaultExamplesMapMock;
        await state.loadDefaultExamples();
        expect(state.defaultExamplesMap, kDefaultExamplesMapMock);
      },
    );
    test(
      'Example state loadDefaultExamples should load default example'
      'with all additions for every Sdk',
      () async {
        // stubs
        when(mockRepo.getDefaultExample(kRequestDefaultExampleForJava))
            .thenAnswer((_) async => exampleWithoutSourceMock);
        when(mockRepo.getDefaultExample(kRequestDefaultExampleForGo))
            .thenAnswer((_) async => exampleWithoutSourceMock);
        when(mockRepo.getDefaultExample(kRequestDefaultExampleForPython))
            .thenAnswer((_) async => exampleWithoutSourceMock);
        when(mockRepo.getDefaultExample(kRequestDefaultExampleForScio))
            .thenAnswer((_) async => exampleWithoutSourceMock);
        when(mockRepo.getExampleOutput(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponseMock.output);
        when(mockRepo.getExampleSource(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponseMock.output);
        when(mockRepo.getExampleLogs(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponseMock.output);
        when(mockRepo.getExampleGraph(kRequestForExampleInfo))
            .thenAnswer((_) async => kOutputResponseMock.output);

        // test assertion
        await state.loadDefaultExamples();
        expect(
          state.defaultExamplesMap,
          kDefaultExamplesMapMock,
        );
      },
    );
  });
}
