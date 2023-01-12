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

import '../../enums/complexity.dart';
import '../example_view_options.dart';
import '../sdk.dart';
import '../snippet_file.dart';
import 'example_loading_descriptor.dart';

/// Fully contains an example data to be loaded.
class ContentExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  final List<SnippetFile> files;

  /// The name of the example, if any, to show in the dropdown.
  final String? name;

  final Complexity? complexity;

  final Sdk sdk;

  const ContentExampleLoadingDescriptor({
    required this.files,
    required this.sdk,
    this.complexity,
    this.name,
    super.viewOptions,
  });

  static ContentExampleLoadingDescriptor? tryParse(Map<String, dynamic> map) {
    final files = map['files'];
    if (files is! List) {
      return null;
    }

    final sdk = Sdk.tryParse(map['sdk']);
    if (sdk == null) {
      return null;
    }

    return ContentExampleLoadingDescriptor(
      files: (map['files'] as List<dynamic>)
          .map((file) => SnippetFile.fromJson(file as Map<String, dynamic>))
          .toList(growable: false),
      name: map['name']?.toString(),
      sdk: sdk,
      complexity: Complexity.fromString(map['complexity']),
      viewOptions: ExampleViewOptions.fromShortMap(map),
    );
  }

  @override
  List<Object?> get props => [
        complexity,
        files,
        name,
        sdk.id,
      ];

  @override
  Map<String, dynamic> toJson() => {
        'complexity': complexity?.name,
        'files': files.map((e) => e.toJson()).toList(growable: false),
        'name': name,
        'sdk': sdk.id,
      };
}
