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
import 'package:playground_components/src/models/snippet_file.dart';

const examplePython1 = Example(
  complexity: Complexity.basic,
  description: 'description',
  files: [SnippetFile(content: 'ex1', isMain: true)],
  name: 'Example X1',
  path: 'SDK_PYTHON/Category/Name1',
  sdk: Sdk.python,
  tags: ['tag1'],
  type: ExampleType.example,
);

const examplePython2 = Example(
  complexity: Complexity.basic,
  description: 'description',
  files: [SnippetFile(content: 'ex2', isMain: true)],
  name: 'Kata',
  path: 'SDK_PYTHON/Category/Name2',
  sdk: Sdk.python,
  tags: ['tag2'],
  type: ExampleType.kata,
);

const exampleBasePython3 = ExampleBase(
  complexity: Complexity.basic,
  description: 'description',
  name: 'Test example',
  path: 'SDK_PYTHON/Category/Name',
  sdk: Sdk.python,
  type: ExampleType.example,
);

const examplePython3 = Example(
  complexity: Complexity.basic,
  description: 'description',
  files: [SnippetFile(content: 'test source', isMain: true)],
  graph: 'test graph',
  logs: 'test logs',
  name: 'Test example',
  outputs: 'test outputs',
  path: 'SDK_PYTHON/Category/Name',
  sdk: Sdk.python,
  type: ExampleType.example,
);

const exampleGo4Multifile = Example(
  files: [
    SnippetFile(content: 'go1', isMain: false, name: '1'),
    SnippetFile(content: 'go2', isMain: true, name: '2'),
  ],
  name: 'exampleGo4Multifile',
  sdk: Sdk.go,
  type: ExampleType.example,
  path: 'SDK_GO/Category/exampleGo4Multifile',
);

const exampleGo5PipelineOptions = Example(
  description: 'description',
  files: [SnippetFile(content: 'test source', isMain: true)],
  graph: 'test graph',
  logs: 'test logs',
  name: 'Test example',
  outputs: 'test outputs',
  path: 'SDK_PYTHON/Category/Name',
  pipelineOptions: 'pipeline options',
  sdk: Sdk.go,
  type: ExampleType.example,
);

const exampleGo6 = Example(
  complexity: Complexity.medium,
  description: 'description',
  name: 'Example',
  files: [SnippetFile(content: 'ex6', isMain: true)],
  path: 'SDK_GO/Category/Name',
  sdk: Sdk.go,
  type: ExampleType.example,
);
