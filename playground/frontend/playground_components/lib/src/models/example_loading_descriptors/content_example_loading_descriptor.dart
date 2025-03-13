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

import 'dart:convert';

import '../../enums/complexity.dart';
import '../example_view_options.dart';
import '../sdk.dart';
import '../snippet_file.dart';
import 'example_loading_descriptor.dart';

/// Fully contains an example data to be loaded.
class ContentExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  final Complexity? complexity;

  final List<SnippetFile> files;

  /// The name of the example, if any, to show in the dropdown.
  final String? name;

  final String pipelineOptions;

  @override
  final Sdk sdk;

  const ContentExampleLoadingDescriptor({
    required this.files,
    required this.sdk,
    this.complexity,
    this.name,
    this.pipelineOptions = '',
    super.viewOptions,
  });

  static ContentExampleLoadingDescriptor? tryParse(Map<String, dynamic> map) {
    final files = _getFilesFromMap(map);
    if (files == null) {
      return null;
    }

    final sdk = Sdk.tryParse(map['sdk']);
    if (sdk == null) {
      return null;
    }

    return ContentExampleLoadingDescriptor(
      complexity: Complexity.fromString(map['complexity']),
      files: files
          .map((file) => SnippetFile.fromJson(file as Map<String, dynamic>))
          .toList(growable: false),
      name: map['name']?.toString(),
      pipelineOptions: map['pipelineOptions'] ?? '',
      sdk: sdk,
      viewOptions: ExampleViewOptions.fromShortMap(map),
    );
  }

  static List? _getFilesFromMap(Map<String, dynamic> map) {
    final files = map['files'];

    if (files is List) {
      return files;
    }

    if (files is String) {
      final list = jsonDecode(files);
      if (list is List) {
        return list;
      }
    }

    return null;
  }

  @override
  List<Object?> get props => [
        complexity,
        files,
        name,
        pipelineOptions,
        sdk.id,
        viewOptions,
      ];

  @override
  ContentExampleLoadingDescriptor copyWithoutViewOptions() =>
      ContentExampleLoadingDescriptor(
        complexity: complexity,
        files: files,
        name: name,
        pipelineOptions: pipelineOptions,
        sdk: sdk,
      );

  @override
  Map<String, dynamic> toJson() => {
        'complexity': complexity?.name,
        'files': files.map((e) => e.toJson()).toList(growable: false),
        'name': name,
        'pipelineOptions': pipelineOptions,
        'sdk': sdk.id,
        ...viewOptions.toShortMap(),
      };

  @override
  bool get isSerializableToUrl => false;
}
