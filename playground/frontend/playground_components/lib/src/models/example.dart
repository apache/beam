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

import 'package:json_annotation/json_annotation.dart';

import '../enums/complexity.dart';
import 'dataset.dart';
import 'example_base.dart';
import 'example_view_options.dart';
import 'sdk.dart';
import 'snippet_file.dart';

part 'example.g.dart';

/// A [ExampleBase] that also has all large fields fetched.
@JsonSerializable()
class Example extends ExampleBase {
  final List<SnippetFile> files;
  final String? graph;
  final String? logs;
  final String? outputs;

  const Example({
    required this.files,
    required super.name,
    required super.sdk,
    required super.type,
    required super.path,
    super.alwaysRun,
    super.complexity,
    super.contextLine,
    super.datasets,
    super.description,
    this.graph,
    super.isMultiFile,
    this.logs,
    this.outputs,
    super.pipelineOptions,
    super.tags,
    super.urlNotebook,
    super.urlVcs,
    super.viewOptions,
  });

  factory Example.fromJson(Map<String, dynamic> json) =>
      _$ExampleFromJson(json);

  @override
  Map<String, dynamic> toJson() => _$ExampleToJson(this);

  Example.fromBase(
    ExampleBase example, {
    required this.files,
    required this.logs,
    required this.outputs,
    this.graph,
  }) : super(
          alwaysRun: example.alwaysRun,
          complexity: example.complexity,
          contextLine: example.contextLine,
          datasets: example.datasets,
          description: example.description,
          isMultiFile: example.isMultiFile,
          name: example.name,
          path: example.path,
          pipelineOptions: example.pipelineOptions,
          sdk: example.sdk,
          tags: example.tags,
          type: example.type,
          urlNotebook: example.urlNotebook,
          urlVcs: example.urlVcs,
          viewOptions: example.viewOptions,
        );

  Example.empty(Sdk sdk)
      : this(
          name: 'Untitled Example',
          files: [SnippetFile.empty],
          path: '',
          sdk: sdk,
          type: ExampleType.example,
        );

  Example copyWith({
    ExampleViewOptions? viewOptions,
  }) {
    return Example(
      alwaysRun: alwaysRun,
      complexity: complexity,
      contextLine: contextLine,
      datasets: datasets,
      description: description,
      files: files,
      graph: graph,
      isMultiFile: isMultiFile,
      logs: logs,
      name: name,
      outputs: outputs,
      path: path,
      pipelineOptions: pipelineOptions,
      sdk: sdk,
      tags: tags,
      type: type,
      urlNotebook: urlNotebook,
      urlVcs: urlVcs,
      viewOptions: viewOptions ?? this.viewOptions,
    );
  }
}
