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

part 'example_model.g.dart';

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

@JsonSerializable()
class ExampleModel {
  @JsonKey(includeIfNull: false)
  final ExampleType? type;

  @JsonKey(includeIfNull: false)
  final String? name;

  @JsonKey(includeIfNull: false)
  final int? id;

  @JsonKey(includeIfNull: false)
  final String? description;

  @JsonKey(includeIfNull: false)
  String? source;

  ExampleModel({
    this.source,
    this.name,
    this.type,
    this.id,
    this.description,
  });

  setSource(String source) {
    this.source = source;
  }

  factory ExampleModel.fromJson(Map<String, dynamic> data) =>
      _$ExampleModelFromJson(data);

  Map<String, dynamic> toJson() => _$ExampleModelToJson(this);
}
