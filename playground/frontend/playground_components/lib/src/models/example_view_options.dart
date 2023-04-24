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

import '../util/string.dart';

part 'example_view_options.g.dart';

@JsonSerializable()
class ExampleViewOptions with EquatableMixin {
  final bool foldCommentAtLineZero;
  final bool foldImports;
  final List<String> readOnlySectionNames;
  final List<String> showSectionNames;
  final List<String> unfoldSectionNames;

  const ExampleViewOptions({
    required this.readOnlySectionNames,
    required this.showSectionNames,
    required this.unfoldSectionNames,
    this.foldCommentAtLineZero = true,
    this.foldImports = true,
  });

  /// Parses a fully normalized map.
  factory ExampleViewOptions.fromJson(Map<String, dynamic> json) =>
      _$ExampleViewOptionsFromJson(json);

  Map<String, dynamic> toJson() => _$ExampleViewOptionsToJson(this);

  /// Parses a simplified map that comes from a URL.
  ///
  /// This map has CSV strings instead of JSON arrays
  /// and cannot override folding parameters' defaults.
  factory ExampleViewOptions.fromShortMap(Map<String, dynamic> map) {
    return ExampleViewOptions(
      readOnlySectionNames: _split(map['readonly']),
      showSectionNames: _split(map['show']),
      unfoldSectionNames: _split(map['unfold']),
    );
  }

  static List<String> _split(Object? value) {
    if (value is! String) {
      return [];
    }

    return value.splitNotEmpty(',');
  }

  Map<String, String> toShortMap() {
    return {
      if (readOnlySectionNames.isNotEmpty)
        'readonly': readOnlySectionNames.join(','),
      if (showSectionNames.isNotEmpty) 'show': showSectionNames.join(','),
      if (unfoldSectionNames.isNotEmpty) 'unfold': unfoldSectionNames.join(','),
    };
  }

  static const empty = ExampleViewOptions(
    readOnlySectionNames: [],
    showSectionNames: [],
    unfoldSectionNames: [],
  );

  @override
  List<Object> get props => [
        foldCommentAtLineZero,
        foldImports,
        readOnlySectionNames,
        showSectionNames,
        unfoldSectionNames,
      ];
}
