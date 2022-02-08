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
  test('Playground State initial value should be java', () {
    final state = PlaygroundState();
    expect(state.sdk, equals(SDK.java));
  });

  test('Playground state should notify all listeners about sdk change', () {
    final state = PlaygroundState();
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
      'Playground state setExample should update source and example and notify all listeners',
      () {
    final state = PlaygroundState(sdk: SDK.go);
    state.addListener(() {
      expect(state.sdk, SDK.go);
      expect(state.source, exampleMock1.source);
      expect(state.selectedExample, exampleMock1);
    });
    state.setExample(exampleMock1);
  });
}
