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

import 'example_base.dart';

/// A [ExampleBase] that also has all large fields fetched.
class Example extends ExampleBase {
  final String source;
  final String? outputs;
  final String? logs;
  final String? graph;

  const Example({
    required super.sdk,
    required super.type,
    required super.name,
    required super.path,
    required super.description,
    super.contextLine,
    super.isMultiFile,
    super.link,
    required super.pipelineOptions,
    required this.source,
    this.outputs,
    this.logs,
    this.graph,
    required super.complexity,
  });

  Example.fromBase(
    ExampleBase example, {
    required this.source,
    required this.outputs,
    required this.logs,
    this.graph,
  }) : super(
          sdk: example.sdk,
          name: example.name,
          path: example.path,
          description: example.description,
          type: example.type,
          contextLine: example.contextLine,
          isMultiFile: example.isMultiFile,
          link: example.link,
          pipelineOptions: example.pipelineOptions,
          complexity: example.complexity,
        );
}
