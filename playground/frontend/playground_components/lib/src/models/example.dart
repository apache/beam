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
  final String? graph;
  final String? logs;
  final String? outputs;
  final String source;

  const Example({
    required this.source,
    required super.name,
    required super.sdk,
    required super.type,
    required super.path,
    this.graph,
    this.logs,
    this.outputs,
    super.complexity,
    super.contextLine,
    super.description,
    super.isMultiFile,
    super.link,
    super.pipelineOptions,
    super.tags,
    super.viewOptions,
  });

  Example.fromBase(
    ExampleBase example, {
    required this.logs,
    required this.outputs,
    required this.source,
    this.graph,
  }) : super(
          complexity: example.complexity,
          contextLine: example.contextLine,
          description: example.description,
          isMultiFile: example.isMultiFile,
          link: example.link,
          name: example.name,
          path: example.path,
          pipelineOptions: example.pipelineOptions,
          sdk: example.sdk,
          tags: example.tags,
          type: example.type,
          viewOptions: example.viewOptions,
        );
}
