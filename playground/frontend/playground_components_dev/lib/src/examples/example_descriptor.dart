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

// ignore_for_file: prefer_interpolation_to_compose_strings

import 'package:http/http.dart' as http;
import 'package:playground_components/playground_components.dart';

import '../code.dart';

const _noGraphSdks = [Sdk.go, Sdk.scio];

const areExamplesDeployed = ExampleDescriptor._repository != '' &&
    ExampleDescriptor._ref != '';

/// Describes an example for the purpose of integration tests.
class ExampleDescriptor {
  static const _schemaAndHost = 'https://github.com';
  static const _rawSchemaAndHost = 'https://raw.githubusercontent.com';

  static const _repository = String.fromEnvironment('example-repository');
  static const _ref = String.fromEnvironment('example-ref');

  const ExampleDescriptor(
    this.name, {
    required this.dbPath,
    required this.path,
    required this.sdk,
    this.contextLine1Based,
    this.croppedFoldedVisibleText,
    this.foldedVisibleText,
    this.fullText,
    this.outputContains,
    this.outputTail,
    this.repository = _repository,
    this.ref = _ref,
  });

  /// 1-based line index to set cursor to.
  final int? contextLine1Based;

  /// A token to retrieve the example from server.
  final String dbPath;

  /// Name as shown in the dropdown.
  final String name;

  /// File path relative to the repository root, starting with `/`.
  final String path;

  /// The owner and repository where this example code is stored,
  /// like 'apache/beam'.
  final String repository;

  /// The branch name or commit hash of the GitHub repository
  /// to use when fetching the code for this example.
  final String ref;

  /// The SDK of this example.
  final Sdk sdk;

  /// Full text to override the one we would get from HTTPS.
  final String? fullText;

  /// Visible text when using `visibleSectionNames` and `foldOutsideSections()`.
  final String? croppedFoldedVisibleText;

  /// Visible text when using `foldOutsideSections()`.
  final String? foldedVisibleText;

  /// The strings that the example's output must contain.
  final List<String>? outputContains;

  /// The string that the example's output must end with.
  final String? outputTail;

  /// Whether the example tab must be visible after running this example.
  bool get hasGraphTab => !_noGraphSdks.contains(sdk);

  /// The basename of the main file with extension.
  String get mainFileName => '$name${sdk.fileExtension}';

  /// The URL to view the file with GitHub UI elements.
  ///
  /// Example:
  /// https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/MinimalWordCount.java
  String get url => '$_schemaAndHost/$repository/blob/$ref$path';

  /// The URL to view the file raw content on GitHub.
  ///
  /// Example:
  /// https://raw.githubusercontent.com/apache/beam/master/examples/java/src/main/java/org/apache/beam/examples/MinimalWordCount.java
  String get rawUrl => '$_rawSchemaAndHost/$repository/$ref$path';

  /// The visible text in the code editor after required foldings.
  Future<String> getVisibleText() async {
    final content = await getFullText();

    return foldLicenseAndImports(content, sdk.highlightMode!);
  }

  /// The full code of the example.
  Future<String> getFullText() async {
    final text = fullText ?? (await http.get(Uri.parse(rawUrl))).body;
    return cutTagComments(text);
  }

  /// Cuts the comments containing meta tags from the file in the repository
  /// so that it matches the source that must be provided by the backend.
  static String cutTagComments(String code) {
    const commentSequences = ['//', '#'];
    const firstString = 'beam-playground:';

    for (final commentSequence in commentSequences) {
      final firstStringRe = RegExp(
        '^' +
            RegExp.escape(commentSequence) +
            r'\s+' +
            RegExp.escape(firstString) +
            r'$',
        multiLine: true,
      );

      final start = code.indexOf(firstStringRe);
      if (start == -1) {
        continue;
      }

      final lastStringRe = RegExp(r'^(\s)*$', multiLine: true);
      final end = code.indexOf(lastStringRe, start);
      if (end == -1) {
        continue;
      }

      return code.substring(0, start) + code.substring(end);
    }

    return code;
  }
}
