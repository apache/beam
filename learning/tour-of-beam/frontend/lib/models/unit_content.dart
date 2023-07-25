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

part 'unit_content.g.dart';

@JsonSerializable(createToJson: false)
class UnitContentModel extends Equatable {
  final String id;
  final String description;
  @JsonKey(defaultValue: [])
  final List<String> hints;
  final String? solutionSnippetId;
  final String title;
  final String? taskSnippetId;

  @JsonKey(ignore: true)
  bool get isChallenge => hints.isNotEmpty;

  const UnitContentModel({
    required this.id,
    required this.description,
    required this.hints,
    required this.solutionSnippetId,
    required this.title,
    required this.taskSnippetId,
  });

  @override
  List<Object?> get props => [
        description,
        hints,
        id,
        solutionSnippetId,
        title,
        taskSnippetId,
      ];

  factory UnitContentModel.fromJson(Map<String, dynamic> json) =>
      _$UnitContentModelFromJson(json);
}
