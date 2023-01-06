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

import 'package:playground_components/src/enums/complexity.dart';
import 'package:playground_components/src/models/example.dart';
import 'package:playground_components/src/models/example_base.dart';
import 'package:playground_components/src/models/sdk.dart';

const exampleMock1 = Example(
  complexity: Complexity.basic,
  description: 'description',
  name: 'Example X1',
  path: 'SDK_PYTHON/Category/Name1',
  sdk: Sdk.python,
  source: 'ex1',
  tags: ['tag1'],
  type: ExampleType.example,
);

const exampleMock2 = Example(
  complexity: Complexity.basic,
  description: 'description',
  name: 'Kata',
  path: 'SDK_PYTHON/Category/Name2',
  sdk: Sdk.python,
  source: 'ex2',
  tags: ['tag2'],
  type: ExampleType.kata,
);

const exampleWithoutSourceMock = ExampleBase(
  complexity: Complexity.basic,
  description: 'description',
  name: 'Test example',
  path: 'SDK_PYTHON/Category/Name',
  sdk: Sdk.python,
  type: ExampleType.example,
);

const exampleWithAllAdditionsMock = Example(
  complexity: Complexity.basic,
  description: 'description',
  graph: 'test outputs',
  logs: 'test outputs',
  name: 'Test example',
  outputs: 'test outputs',
  path: 'SDK_PYTHON/Category/Name',
  sdk: Sdk.python,
  source: 'test outputs',
  type: ExampleType.example,
);

const exampleGoPipelineOptions = Example(
  description: 'description',
  graph: 'test outputs',
  logs: 'test outputs',
  name: 'Test example',
  outputs: 'test outputs',
  path: 'SDK_PYTHON/Category/Name',
  pipelineOptions: 'pipeline options',
  sdk: Sdk.go,
  source: 'test outputs',
  type: ExampleType.example,
);

const exampleMockGo = Example(
  complexity: Complexity.medium,
  description: 'description',
  name: 'Example',
  path: 'SDK_GO/Category/Name',
  sdk: Sdk.go,
  source: 'ex1',
  type: ExampleType.example,
);
