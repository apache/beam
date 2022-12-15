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

// ignore_for_file: avoid_redundant_argument_values

import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/src/controllers/snippet_editing_controller.dart';
import 'package:playground_components/src/enums/complexity.dart';
import 'package:playground_components/src/models/example_loading_descriptors/content_example_loading_descriptor.dart';
import 'package:playground_components/src/models/sdk.dart';
import 'package:playground_components/src/playground_components.dart';

import '../common/descriptors.dart';
import '../common/examples.dart';

void main() async {
  await PlaygroundComponents.ensureInitialized();

  int notified = 0;
  late SnippetEditingController controller;

  setUp((){
    notified = 0;
    controller = SnippetEditingController(sdk: Sdk.python);
    controller.addListener(() => notified++);
  });

  group('SnippetEditingController.', () {
    group('Changes.', (){
      test('Unchanged initially', (){
        expect(controller.isChanged, false);
        expect(notified, 0);
      });

      test('Unchanged after setting an example', () {
        controller.setExample(exampleMock1);

        expect(controller.isChanged, false);
        expect(notified, 1);
      });

      test('Changes when changing code, notifies once', () {
        controller.setExample(exampleMock1);
        controller.codeController.text = exampleMock1.source;

        expect(controller.isChanged, false);
        expect(notified, 1);

        controller.codeController.text = 'changed';

        expect(controller.isChanged, true);
        expect(notified, 2);

        controller.codeController.text = 'changed2';

        expect(controller.isChanged, true);
        expect(notified, 2);

        controller.codeController.text = exampleMock1.source;

        expect(controller.isChanged, false);
        expect(notified, 3);
      });

      test('Changes when changing pipelineOptions, notifies once', () {
        controller.setExample(exampleGoPipelineOptions);
        controller.pipelineOptions = exampleGoPipelineOptions.pipelineOptions;

        expect(controller.isChanged, false);
        expect(notified, 1);

        controller.pipelineOptions = 'changed';

        expect(controller.isChanged, true);
        expect(notified, 2);

        controller.pipelineOptions = 'changed2';

        expect(controller.isChanged, true);
        expect(notified, 2);

        controller.pipelineOptions = exampleGoPipelineOptions.pipelineOptions;

        expect(controller.isChanged, false);
        expect(notified, 3);
      });
    });

    group('Descriptors.', () {
      test('Returns the original descriptor if unchanged', () {
        controller.setExample(
          exampleMock1,
          descriptor: standardDescriptor1,
        );

        final descriptor = controller.getLoadingDescriptor();

        expect(descriptor, standardDescriptor1);
      });

      test('Returns a ContentExampleLoadingDescriptor if changed', () {
        controller.setExample(
          exampleMock1,
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
      });

      test('Returns a ContentExampleLoadingDescriptor if no descriptor', () {
        controller.setExample(exampleMock1, descriptor: null);

        controller.setExample(exampleMock2, descriptor: null);
        final descriptor = controller.getLoadingDescriptor();

        const expected = ContentExampleLoadingDescriptor(
          complexity: Complexity.basic,
          content: 'ex2',
          name: 'Kata',
          sdk: Sdk.python,
        );

        expect(descriptor, expected);
      });
    });
  });
}
