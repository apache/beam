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

import 'package:equatable/equatable.dart';
import 'package:json_annotation/json_annotation.dart';

import '../enums/complexity.dart';
import '../repositories/example_repository.dart';
import 'dataset.dart';
import 'example_view_options.dart';
import 'sdk.dart';

part 'example_base.g.dart';

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

/// An example's basic info that does not contain source code
/// and other large fields.
/// These objects are fetched as lists from [ExampleRepository].
@JsonSerializable()
class ExampleBase with EquatableMixin implements Comparable<ExampleBase> {
  final bool alwaysRun;
  final Complexity? complexity;

  /// Index of the line to focus, 1-based.
  final int contextLine;
  final List<Dataset> datasets;
  final String description;
  final bool isMultiFile;
  final String name;
  final String path;
  final String pipelineOptions;
  final Sdk sdk;
  final List<String> tags;
  final ExampleType type;
  final String? urlNotebook;
  final String? urlVcs;
  final ExampleViewOptions viewOptions;

  const ExampleBase({
    required this.name,
    required this.path,
    required this.sdk,
    required this.type,
    this.alwaysRun = false,
    this.complexity,
    this.contextLine = 1,
    this.datasets = const [],
    this.description = '',
    this.isMultiFile = false,
    this.pipelineOptions = '',
    this.tags = const [],
    this.urlNotebook,
    this.urlVcs,
    this.viewOptions = ExampleViewOptions.empty,
  });

  factory ExampleBase.fromJson(Map<String, dynamic> json) =>
      _$ExampleBaseFromJson(json);

  Map<String, dynamic> toJson() => _$ExampleBaseToJson(this);

  // TODO(alexeyinkin): Use all fields, https://github.com/apache/beam/issues/23979
  @override
  List<Object> get props => [path];

  @override
  int compareTo(ExampleBase other) {
    return name.toLowerCase().compareTo(other.name.toLowerCase());
  }

  bool get usesEmulatedData => datasets.any(
        (dataset) => dataset.type != null,
      );
}
