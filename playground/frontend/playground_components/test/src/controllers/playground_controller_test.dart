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
import 'package:playground_components/src/controllers/example_loaders/examples_loader.dart';
import 'package:playground_components/src/controllers/playground_controller.dart';
import 'package:playground_components/src/models/sdk.dart';

import '../common/examples.dart';
import 'playground_controller_test.mocks.dart';

@GenerateMocks([ExamplesLoader, ExampleCache])
void main() {
  late PlaygroundController state;
  final mockExamplesLoader = MockExamplesLoader();

  when(mockExamplesLoader.load(any)).thenAnswer((_) async => 1);

  setUp(() {
    state = PlaygroundController(
      examplesLoader: MockExamplesLoader(),
      exampleCache: MockExampleCache(),
    );
  });

  test('Initial value of SDK field should be null', () {
    expect(state.sdk, null);
    state.setSdk(Sdk.go);
    expect(state.sdk, Sdk.go);
  });

  test('Initial value of examplesTitle should be equal to kTitle', () {
    expect(state.examplesTitle, kTitle);
  });

  test('Initial value of isCodeRunning should be false', () {
    expect(state.isCodeRunning, false);
  });

  test('Initial value of pipelineOptions should be empty string', () {
    expect(state.pipelineOptions, null);
    state.setSdk(Sdk.go);
    expect(state.pipelineOptions, '');
  });

  test('Initial value of source should be empty string', () {
    expect(state.source, null);
    state.setSdk(Sdk.go);
    expect(state.source, '');
  });

  group('isExampleChanged Tests', () {
    test(
      'If example source is changed, value of isExampleChanged should be true',
      () {
        state.setExample(exampleMock1, setCurrentSdk: true);
        expect(state.isExampleChanged, false);
        // 'test' in this line hits a bug fixed here:
        // https://github.com/akvelon/flutter-code-editor/commit/c74ce566bf873dc76a5269ce6fe7b02df9c148e0
        // TODO(alexeyinkin): revert from 'test1' to 'test' when Akvelon's editor is integrated.
        state.setSource('test1');
        expect(state.isExampleChanged, true);
      },
    );

    test(
      'If pipelineOptions is changed, value of isExampleChanged should be true',
      () {
        state.setExample(exampleMock1, setCurrentSdk: true);
        expect(state.isExampleChanged, false);
        state.setPipelineOptions('test options');
        expect(state.isExampleChanged, true);
      },
    );
  });

  test(
    'If selected example type is not test and SDK is java or python, graph should be available',
    () {
      state.setExample(exampleMock1, setCurrentSdk: true);
      expect(state.graphAvailable, true);
    },
  );

  test(
    'Playground state setExample should update source and example and notify all listeners',
    () {
      state.addListener(() {
        expect(state.sdk, Sdk.go);
        expect(state.source, exampleMockGo.source);
        expect(state.selectedExample, exampleMockGo);
      });
      state.setExample(exampleMockGo, setCurrentSdk: true);
    },
  );

  test('Playground state should notify all listeners about sdk change', () {
    state.addListener(() {
      expect(state.sdk, Sdk.go);
    });
    state.setSdk(Sdk.go);
  });

  test(
      'Playground state reset should reset source to example notify all listeners',
      () {
    state.setExample(exampleMock1, setCurrentSdk: true);
    state.setSource('source');
    state.addListener(() {
      expect(state.source, exampleMock1.source);
    });
    state.reset();
  });

  test(
    'If Playground state result is empty, then resetError should break the execution',
    () {
      state.resetError();
      expect(state.result, null);
    },
  );

  test(
    'Playground state should notify all listeners about pipeline options change',
    () {
      state.setSdk(Sdk.go);
      state.addListener(() {
        expect(state.pipelineOptions, 'test options');
      });
      state.setPipelineOptions('test options');
    },
  );

  test('getLoadingDescriptor()', () {
    state.setExample(exampleMock2, setCurrentSdk: true);
    state.setExample(exampleMockGo, setCurrentSdk: false);

    final descriptor = state.getLoadingDescriptor();

    expect(
      descriptor.toJson(),
      {
        'descriptors': [
          {
            'sdk': 'python',
            'content': 'ex2',
            'name': 'Kata',
            'complexity': 'basic'
          },
          {
            'sdk': 'go',
            'content': 'ex1',
            'name': 'Example',
            'complexity': 'medium'
          },
        ],
      },
    );
  });
}
