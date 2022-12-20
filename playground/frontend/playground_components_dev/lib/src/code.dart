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

extension StringCodeExtension on String {
  /// Returns the visible text mimicking the flutter_code_editor's folding of
  /// license.
  String foldJavaLicense() {
    return replaceFirstMapped(
      RegExp(r'^(/\*)(.*?\*/)(.*)$', dotAll: true),
      (m) => '${m[1]}${m[3]}',
    );
  }

  /// Returns the visible text mimicking the flutter_code_editor's folding of
  /// sequential imports.
  String foldJavaImports() {
    final packageRegExp = RegExp('^package .*?\n', multiLine: true);
    final cutStart = indexOf(packageRegExp) +
        (packageRegExp.firstMatch(this)?[0]?.length ?? 0);

    final importRegExp = RegExp('^import .*?\n', multiLine: true);
    final lastImportStart = lastIndexOf(importRegExp);

    return substring(0, cutStart) +
        substring(lastImportStart).replaceFirst(importRegExp, '');
  }
}
