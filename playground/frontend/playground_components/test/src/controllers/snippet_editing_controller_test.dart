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
import 'package:playground_components/src/controllers/snippet_editing_controller.dart';
import 'package:playground_components/src/enums/complexity.dart';
import 'package:playground_components/src/models/example_loading_descriptors/content_example_loading_descriptor.dart';
import 'package:playground_components/src/models/sdk.dart';

import '../common/descriptors.dart';
import '../common/examples.dart';

void main() {
  group(
    'Snippet editing controller',
    () {
      test(
        'Returns given descriptor if code has not been changed',
        () {
          final controller = SnippetEditingController(sdk: Sdk.python);
          controller.configure(
            example: exampleMock1,
            descriptor: standardDescriptor1,
          );

          final descriptor = controller.getLoadingDescriptor();

          expect(descriptor, standardDescriptor1);
        },
      );

      test(
        'Returns content descriptor if code has been changed',
        () {
          final controller = SnippetEditingController(sdk: Sdk.python);
          controller.configure(
            example: exampleMock1,
            descriptor: standardDescriptor1,
          );

          controller.codeController.value = const TextEditingValue(text: 'ex4');
          final descriptor = controller.getLoadingDescriptor();

          const expected = ContentExampleLoadingDescriptor(
            content: 'ex4',
            sdk: Sdk.python,
            name: 'Example X1',
            complexity: Complexity.basic,
          );

          expect(descriptor, expected);
        },
      );

      test(
        'Returns content descriptor if descriptor has not been set',
        () {
          final controller = SnippetEditingController(sdk: Sdk.python);
          controller.configure(example: exampleMock1);

          controller.configure(example: exampleMock2);
          final descriptor = controller.getLoadingDescriptor();

          const expected = ContentExampleLoadingDescriptor(
            content: 'ex2',
            sdk: Sdk.python,
            name: 'Kata',
            complexity: Complexity.basic,
          );

          expect(descriptor, expected);
        },
      );

      test(
        'Returns configured descriptor if example has been changed',
        () {
          final controller = SnippetEditingController(sdk: Sdk.python);
          controller.configure(
            example: exampleMock1,
            descriptor: standardDescriptor1,
          );

          controller.configure(example: exampleMock2);
          final descriptor = controller.getLoadingDescriptor();

          expect(descriptor, contentDescriptor2);
        },
      );
    },
  );
}
