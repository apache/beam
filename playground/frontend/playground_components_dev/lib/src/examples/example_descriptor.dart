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

/// Describes an example for the purpose of integration tests.
class ExampleDescriptor {
  static const _schemaAndHost = 'https://raw.githubusercontent.com/';

  static const _defaultRepositoryAndRef = 'apache/beam/master';
  // If running before this is deployed, change to this:
  // static const _defaultRepositoryAndRef =
  //     'akvelon/beam/issue24959_test-loading-url';

  const ExampleDescriptor(
    this.name, {
    required this.dbPath,
    required this.path,
    required this.sdk,
    this.contextLine1Based,
    this.croppedFoldedVisibleText,
    this.foldedVisibleText,
    this.outputContains,
    this.outputTail,
    this.repositoryAndRef = _defaultRepositoryAndRef,
  });

  /// 1-based line index to set cursor to.
  final int? contextLine1Based;

  /// A token to retrieve the example from server.
  final String dbPath;

  /// Name as shown in the dropdown.
  final String name;

  /// File path relative to the repository root, starting with `/`.
  final String path;

  final String repositoryAndRef;

  final Sdk sdk;

  /// Visible text when using `visibleSectionNames` and `foldOutsideSections()`.
  final String? croppedFoldedVisibleText;

  /// Visible text when using `foldOutsideSections()`.
  final String? foldedVisibleText;

  final List<String>? outputContains;
  final String? outputTail;

  bool get hasGraphTab => !_noGraphSdks.contains(sdk);

  /// The URL to view the file raw content on GitHub.
  String get url => '$_schemaAndHost$repositoryAndRef$path';

  Future<String> getVisibleText() async {
    final content = await getFullText();

    return foldLicenseAndImports(content, sdk.highlightMode!);
  }

  Future<String> getFullText() async {
    final response = await http.get(Uri.parse(url));
    return cutTagComments(response.body);
  }

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
