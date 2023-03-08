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
import 'package:playground/modules/examples/models/example_token_type.dart';

void main() {
  test('ExampleTokenType.fromToken', () {
    const examples = {
      //
      'http://': ExampleTokenType.http,
      'https://example.com/path': ExampleTokenType.http,

      'SDK_GO': ExampleTokenType.standard,
      'SDK_JAVA_something': ExampleTokenType.standard,
      'SDK_PYTHONNNN': ExampleTokenType.standard,
      'SDK_SCIO/something': ExampleTokenType.standard,

      '': ExampleTokenType.userShared,
      '123': ExampleTokenType.userShared,
      'abc': ExampleTokenType.userShared,
      'SDK_TYPESCRIPT': ExampleTokenType.userShared,
      'SDK_RUST': ExampleTokenType.userShared,
    };

    for (final example in examples.entries) {
      final token = example.key;
      final result = ExampleTokenType.fromToken(token);

      expect(result, example.value, reason: token);
    }
  });
}
