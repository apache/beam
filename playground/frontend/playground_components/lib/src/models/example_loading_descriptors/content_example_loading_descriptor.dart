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
import 'example_loading_descriptor.dart';

/// Fully contains an example data to be loaded.
class ContentExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  /// The source code.
  final String content;

  /// The name of the example, if any, to show in the dropdown.
  final String? name;

  final Complexity? complexity;

  final Sdk sdk;

  const ContentExampleLoadingDescriptor({
    required this.content,
    required this.sdk,
    this.complexity,
    this.name,
    super.viewOptions,
  });

  static ContentExampleLoadingDescriptor? tryParse(Map<String, dynamic> map) {
    final content = map['content']?.toString();
    if (content == null) {
      return null;
    }

    final sdk = Sdk.tryParse(map['sdk']);
    if (sdk == null) {
      return null;
    }

    return ContentExampleLoadingDescriptor(
      content: content,
      name: map['name']?.toString(),
      sdk: sdk,
      complexity: Complexity.fromString(map['complexity']),
      viewOptions: ExampleViewOptions.fromShortMap(map),
    );
  }

  @override
  List<Object?> get props => [
        complexity,
        content,
        name,
        sdk.id,
      ];

  @override
  Map<String, dynamic> toJson() => {
        'complexity': complexity?.name,
        'content': content,
        'name': name,
        'sdk': sdk.id,
      };
}
