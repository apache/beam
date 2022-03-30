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
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/states/playground_state.dart';

import 'mocks/example_mock.dart';

void main() {
  late PlaygroundState state;

  setUp(() {
    state = PlaygroundState();
  });

  test('Initial value of SDK field should be java', () {
    expect(state.sdk, equals(SDK.java));
  });

  test('Initial value of examplesTitle should be equal to kTitle', () {
    expect(state.examplesTitle, kTitle);
  });

  test('Initial value of isCodeRunning should be false', () {
    expect(state.isCodeRunning, false);
  });

  test('Initial value of pipelineOptions should be empty string', () {
    expect(state.pipelineOptions, '');
  });

  test('Initial value of source should be empty string', () {
    expect(state.source, '');
  });

  group('isExampleChanged Tests', () {
    test(
      'If example source is changed, value of isExampleChanged should be true',
      () {
        state.setExample(exampleMock1);
        state.selectedExample!.setSource('test');
        expect(state.isExampleChanged, true);
      },
    );

    test(
      'If pipelineOptions is changed, value of isExampleChanged should be true',
      () {
        state.setExample(exampleMock1);
        state.setPipelineOptions('test options');
        expect(state.isExampleChanged, true);
      },
    );
  });

  test(
    'If selected example type is not test and SDK is java or python, graph should be available',
    () {
      state.setExample(exampleMock1);
      expect(state.graphAvailable, true);
    },
  );

  test(
    'Playground state setExample should update source and example and notify all listeners',
    () {
      final state = PlaygroundState(sdk: SDK.go);
      state.addListener(() {
        expect(state.sdk, SDK.go);
        expect(state.source, exampleMock1.source);
        expect(state.selectedExample, exampleMock1);
      });
      state.setExample(exampleMock1);
    },
  );

  test('Playground state should notify all listeners about sdk change', () {
    state.addListener(() {
      expect(state.sdk, SDK.go);
    });
    state.setSdk(SDK.go);
  });

  test(
      'Playground state reset should reset source to example notify all listeners',
      () {
    final state = PlaygroundState(sdk: SDK.go, selectedExample: exampleMock1);
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
      state.addListener(() {
        expect(state.pipelineOptions, 'test options');
      });
      state.setPipelineOptions('test options');
    },
  );
}
