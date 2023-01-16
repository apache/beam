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

part 'snippet_file.g.dart';

@JsonSerializable()
class SnippetFile with EquatableMixin {
  final String content;
  final bool isMain;
  final String name;

  const SnippetFile({
    required this.content,
    required this.isMain,
    this.name = '',
  });

  static const empty = SnippetFile(
    content: '',
    isMain: true,
  );

  Map<String, dynamic> toJson() => _$SnippetFileToJson(this);

  factory SnippetFile.fromJson(Map<String, dynamic> map) =>
      _$SnippetFileFromJson(map);

  @override
  List<Object?> get props => [
        content,
        isMain,
        name,
      ];
}
