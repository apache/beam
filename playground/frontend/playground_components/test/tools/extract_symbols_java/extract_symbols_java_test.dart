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

import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

import '../common.dart';

const _lang = 'java';
const _dependenciesDir = 'test/tools/extract_symbols_$_lang/dependencies';

void main() {
  test('Extract SDK Symbols. $_lang', () async {
    final classPath = await _buildClassPath();
    await _compileClasses(classPath);
    await testExtractSymbols(
      language: _lang,
      executables: ['java'],
      arguments: [
        '-classpath',
        classPath,
        'com.playground.extract_symbols.Main',
        '../../test/tools/extract_symbols_$_lang/sdk_mock',
      ],
    );
  });
}

Future<String> _buildClassPath() async {
  const dependencies = [
    'https://repo1.maven.org/maven2/com/github/javaparser/javaparser-core/3.24.9/javaparser-core-3.24.9.jar',
    'https://repo1.maven.org/maven2/com/esotericsoftware/yamlbeans/yamlbeans/1.15/yamlbeans-1.15.jar',
  ];

  await _downloadDependenciesIfNeed(dependencies);

  final workingDirectory = Directory.current.path;

  return [
    '$workingDirectory/tools/extract_symbols_$_lang/build/classes/java/main',
    ...dependencies.map(
      (f) => '$workingDirectory/$_dependenciesDir/${f.split('/').last}',
    ),
  ].join(':');
}

Future<void> _downloadDependenciesIfNeed(List<String> dependencies) async {
  for (final dependency in dependencies) {
    final fileName = dependency.split('/').last;
    final file = File('$_dependenciesDir/$fileName');
    if (!file.existsSync()) {
      final request = await HttpClient().getUrl(Uri.parse(dependency));
      final response = await request.close();
      await file.create(recursive: true);
      await response.pipe(file.openWrite());
    }
  }
}

Future<void> _compileClasses(String classPath) async {
  await Process.run(
    'mkdir',
    ['-p', 'build/classes/java/main'],
    workingDirectory: 'tools/extract_symbols_$_lang',
  );
  await Process.run(
    'javac',
    [
      '-d',
      'build/classes/java/main/',
      '-classpath',
      classPath,
      'src/main/java/com/playground/extract_symbols/Main.java',
      'src/main/java/com/playground/extract_symbols/ClassInfo.java',
    ],
    workingDirectory: 'tools/extract_symbols_$_lang',
  );
}
