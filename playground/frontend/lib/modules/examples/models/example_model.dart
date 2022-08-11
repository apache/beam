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

import 'package:playground/modules/sdk/models/sdk.dart';

enum ExampleType {
  all,
  example,
  kata,
  test,
}

extension ExampleTypeToString on ExampleType {
  String get name {
    switch (this) {
      case ExampleType.example:
        return 'Examples';
      case ExampleType.kata:
        return 'Katas';
      case ExampleType.test:
        return 'Unit tests';
      case ExampleType.all:
        return 'All';
    }
  }
}

class ExampleModel with Comparable<ExampleModel> {
  final SDK sdk;
  final ExampleType type;
  final String name;
  final String path;
  final String description;
  int contextLine;
  bool isMultiFile;
  String? link;
  String? source;
  String? outputs;
  String? logs;
  String? pipelineOptions;
  String? graph;

  ExampleModel({
    required this.sdk,
    required this.name,
    required this.path,
    required this.description,
    required this.type,
    this.contextLine = 1,
    this.isMultiFile = false,
    this.link,
    this.source,
    this.outputs,
    this.logs,
    this.pipelineOptions,
    this.graph,
  });

  setSource(String source) {
    this.source = source;
  }

  setOutputs(String outputs) {
    this.outputs = outputs;
  }

  setLogs(String logs) {
    this.logs = logs;
  }

  setGraph(String graph) {
    this.graph = graph;
  }

  setContextLine(int contextLine) {
    this.contextLine = contextLine;
  }

  bool isInfoFetched() {
    // checking only source, because outputs/logs can be empty
    return source?.isNotEmpty ?? false;
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) || (other is ExampleModel && path == other.path);

  @override
  int get hashCode => path.hashCode;

  @override
  int compareTo(ExampleModel other) {
    return name.toLowerCase().compareTo(other.name.toLowerCase());
  }
}
