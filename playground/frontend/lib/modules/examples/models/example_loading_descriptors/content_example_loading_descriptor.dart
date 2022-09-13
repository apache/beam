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

import 'package:playground/modules/examples/models/example_loading_descriptors/example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_origin.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class ContentExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  /// The source code.
  final String content;

  /// The name of the example, if any, to show in the dropdown.
  final String? name;

  final SDK sdk;

  const ContentExampleLoadingDescriptor({
    required this.content,
    required this.name,
    required this.sdk,
  });

  static ContentExampleLoadingDescriptor? tryParse(Map eventData) {
    final content = _tryParseContent(eventData);
    if (content == null) {
      return null;
    }

    final sdk = _tryParseSdk(eventData);
    if (sdk == null) {
      return null;
    }

    return ContentExampleLoadingDescriptor(
      content: content,
      name: _tryParseName(eventData),
      sdk: sdk,
    );
  }

  static String? _tryParseContent(Map map) {
    return map['content']?.toString();
  }

  static String? _tryParseName(Map map) {
    return map['name']?.toString();
  }

  static SDK? _tryParseSdk(Map map) {
    return SDK.tryParse(map['sdk']);
  }

  @override
  int get hashCode => Object.hash(
    content.hashCode,
    sdk.hashCode,
  );

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) {
      return true;
    }

    return other is ContentExampleLoadingDescriptor &&
        content == other.content &&
        name == other.name &&
        sdk == other.sdk;
  }

  @override
  ExampleOrigin get origin => ExampleOrigin.content;

  @override
  Map<String, dynamic> toJson() => {
    'content': content,
    'name': name,
    'sdk': sdk.name,
  };
}
