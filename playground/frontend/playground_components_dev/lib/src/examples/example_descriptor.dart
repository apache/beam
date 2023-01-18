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

import 'examples.dart';

/// Describes an example for the purpose of integration tests.
class ExampleDescriptor {
  const ExampleDescriptor(
    this.name, {
    required this.dbPath,
    required this.path,
    this.croppedVisibleText,
    this.fullVisibleText,
    this.outputContains,
    this.outputTail,
  });

  /// A token to retrieve the example from server.
  final String dbPath;

  /// Name as shown in the dropdown.
  final String name;

  /// File path relative to the repository root, starting with `/`.
  final String path;

  /// Visible text when using `visibleSectionNames`.
  final String? croppedVisibleText;

  /// Visible text without using `visibleSectionNames`.
  final String? fullVisibleText;

  final String? outputContains;
  final String? outputTail;

  /// The URL to view the file raw content on GitHub.
  String get url => '${Examples.urlPrefix}$path';
}
