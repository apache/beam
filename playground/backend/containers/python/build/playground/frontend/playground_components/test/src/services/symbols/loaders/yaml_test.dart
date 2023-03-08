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

// ignore_for_file: unnecessary_lambdas

import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/playground_components.dart';

void main() {
  setUp(() {
    TestWidgetsFlutterBinding.ensureInitialized();
  });

  test('YamlSymbolsLoader', () async {
    final loader = YamlSymbolsLoader(
      path: 'assets/symbols/python.g.yaml',
      package: PlaygroundComponents.packageName,
    );

    final symbolsDictionary = await loader.future;

    expect(
      symbolsDictionary.symbols,
      containsAll([
        'ANamedTuple',
        'PCollection',
        'Xyz',
      ]),
      reason: 'Classes should be loaded but are not.',
    );

    expect(
      symbolsDictionary.symbols,
      containsAll([
        'get_message_stream',
        'windowing',
        'leave_composite_transform',
      ]),
      reason: 'Methods should be loaded but are not.',
    );

    expect(
      symbolsDictionary.symbols.contains('f_int64'),
      false,
      reason: 'Properties should not be loaded but are.',
    );
  });
}
