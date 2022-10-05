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

void main() {
  test('Extract SDK Symbols. Python', () async {
    const arguments = [
      'tools/extract_symbols_python/extract_symbols_python.py',
      'test/tools/extract_symbols_python/sdk_mock',
    ];
    const fileName = 'test/tools/extract_symbols_python/python.golden.yaml';

    for (final executable in await _getPythonExecutables()) {
      final result = await Process.run(executable, arguments);
      if (result.exitCode != 0) {
        continue;
      }

      expect(result.stdout, File(fileName).readAsStringSync());
      return;
    }

    final path = (await Process.run('printenv', ['PATH'])).stdout;
    fail('Python error or No python3 executable found in your \$PATH: $path');
  });
}

/// Returns all `python3` executables found in $PATH.
///
/// Flutter comes with it's own copy of `python3` which has neither
/// `pyyaml` package nor `pip3` to install it.
/// The test environment overrides $PATH to put that copy of `python3` first,
/// so we cannot automatically get the system's default `python3`.
/// So we must try all available copies of `python3`.
Future<Iterable<String>> _getPythonExecutables() async {
  final result = await Process.run('which', ['-a', 'python3']);
  return result.stdout
      .toString()
      .split('\n')
      .where((command) => command.isNotEmpty);
}
