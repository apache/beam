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

import 'package:flutter_test/flutter_test.dart';
import 'package:playground/modules/editor/parsers/run_options_parser.dart';

void main() {
  group('RunOptions parser', () {
    test('should return null if cant parse options', () {
      expect(parseRunOptions('--key1'), null);
      expect(parseRunOptions('--key1 value --key'), null);
      expect(parseRunOptions('-key1 value'), null);
      expect(parseRunOptions('- -key1 value'), null);
    });
    test('should return empty map if empty', () {
      expect(parseRunOptions(''), {});
    });
    test('should parse correct options string', () {
      expect(
        parseRunOptions('--key1 value1 --key2 value2'),
        {'key1': 'value1', 'key2': 'value2'},
      );
      expect(
        parseRunOptions('--key1    value1     --key2     value2'),
        {'key1': 'value1', 'key2': 'value2'},
      );
    });
  });
}
