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

Future<void> testExtractSymbols({
  required String language,
  required List<String> executables,
  required List<String> arguments,
}) async {
  final directory = 'tools/extract_symbols_$language';
  final fileName = 'test/$directory/$language.golden.yaml';
  final results = <String, ProcessResult>{};

  for (final executable in await _getExecutables(executables)) {
    final result = await Process.run(
      executable,
      arguments,
      workingDirectory: directory,
    );

    if (result.exitCode != 0) {
      results[executable] = result;
      continue;
    }

    expect(result.stdout, File(fileName).readAsStringSync());
    return;
  }

  final path = (await Process.run('printenv', ['PATH'])).stdout;
  fail(
    'Script error '
    'or No $executables in your \$PATH: $path\n${results.toStringDebug()}',
  );
}

/// Returns all executables found in $PATH.
///
/// Flutter comes with it's own copy of `python3` which has neither
/// `pyyaml` package nor `pip3` to install it.
/// The test environment overrides $PATH to put that copy of `python3` first,
/// so we cannot automatically get the system's default `python3`.
/// So we must try all available copies of `python3`.
///
/// The same may happen with other SDKs.
Future<Iterable<String>> _getExecutables(List<String> executables) async {
  final result = await Process.run('which', ['-a', executables.join(' ')]);
  return result.stdout
      .toString()
      .split('\n')
      .where((command) => command.isNotEmpty);
}

extension on ProcessResult {
  String toStringDebug() {
    final buffer = StringBuffer();

    buffer.writeln('Exit code: ${this.exitCode}');
    buffer.writeln('Stdout:\n${this.stdout}');
    buffer.writeln('Stderr:\n${this.stderr}');

    return buffer.toString();
  }
}

extension on Map<dynamic, ProcessResult> {
  String toStringDebug() {
    return entries
        .map((e) => '${e.key}\n${e.value.toStringDebug()}')
        .join('\n\n');
  }
}
