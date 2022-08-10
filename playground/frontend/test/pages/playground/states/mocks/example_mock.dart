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

import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

final ExampleModel exampleMock1 = ExampleModel(
  sdk: SDK.python,
  source: 'ex1',
  name: 'Example',
  type: ExampleType.example,
  description: 'description',
  path: 'SDK_PYTHON/Category/Name',
);

final ExampleModel exampleMock2 = ExampleModel(
  sdk: SDK.python,
  source: 'ex2',
  name: 'Kata',
  type: ExampleType.kata,
  description: 'description',
  path: 'SDK_PYTHON/Category/Name',
);

final ExampleModel exampleWithoutSourceMock = ExampleModel(
  sdk: SDK.python,
  name: 'Test example',
  type: ExampleType.example,
  description: 'description',
  path: 'SDK_PYTHON/Category/Name',
);

final ExampleModel exampleWithAllAdditionsMock = ExampleModel(
  sdk: SDK.python,
  name: 'Test example',
  type: ExampleType.example,
  description: 'description',
  path: 'SDK_PYTHON/Category/Name',
  source: 'test outputs',
  outputs: 'test outputs',
  logs: 'test outputs',
  graph: 'test outputs',
);

final ExampleModel exampleMockGo = ExampleModel(
  sdk: SDK.go,
  source: 'ex1',
  name: 'Example',
  type: ExampleType.example,
  description: 'description',
  path: 'SDK_GO/Category/Name',
);
