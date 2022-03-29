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
import 'package:playground/modules/examples/repositories/example_repository.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/states/examples_state.dart';

import 'examples_state_test.mocks.dart';
import 'mocks/categories_mock.dart';

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
}
