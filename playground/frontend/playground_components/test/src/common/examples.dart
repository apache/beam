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

import 'package:playground_components/src/models/example.dart';
import 'package:playground_components/src/models/example_base.dart';
import 'package:playground_components/src/models/sdk.dart';

const exampleMock1 = Example(
  sdk: Sdk.python,
  source: 'ex1',
  name: 'Example',
  type: ExampleType.example,
  description: 'description',
  path: 'SDK_PYTHON/Category/Name',
  pipelineOptions: '',
);

const exampleMock2 = Example(
  sdk: Sdk.python,
  source: 'ex2',
  name: 'Kata',
  type: ExampleType.kata,
  description: 'description',
  path: 'SDK_PYTHON/Category/Name',
  pipelineOptions: '',
);

const exampleWithoutSourceMock = ExampleBase(
  sdk: Sdk.python,
  name: 'Test example',
  type: ExampleType.example,
  description: 'description',
  path: 'SDK_PYTHON/Category/Name',
  pipelineOptions: '',
);

const exampleWithAllAdditionsMock = Example(
  sdk: Sdk.python,
  name: 'Test example',
  type: ExampleType.example,
  description: 'description',
  path: 'SDK_PYTHON/Category/Name',
  source: 'test outputs',
  outputs: 'test outputs',
  logs: 'test outputs',
  graph: 'test outputs',
  pipelineOptions: '',
);

const exampleMockGo = Example(
  sdk: Sdk.go,
  source: 'ex1',
  name: 'Example',
  type: ExampleType.example,
  description: 'description',
  path: 'SDK_GO/Category/Name',
  pipelineOptions: '',
);
