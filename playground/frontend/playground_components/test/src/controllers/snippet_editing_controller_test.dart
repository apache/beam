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
import 'package:playground_components/playground_components.dart';
import 'package:playground_components/src/controllers/snippet_editing_controller.dart';

import '../common/descriptors.dart';
import '../common/examples.dart';

void main() async {
  await PlaygroundComponents.ensureInitialized();

  int notified = 0;
  late SnippetEditingController controller;

  setUp(() {
    notified = 0;
    controller = SnippetEditingController(sdk: Sdk.python);
    controller.addListener(() => notified++);
  });

  group('SnippetEditingController.', () {
    group('Changes.', () {
      test('Unchanged initially', () {
        expect(controller.isChanged, false);
        expect(notified, 0);
      });

      test('Unchanged after setting an example', () {
        controller.setExample(examplePython1);

        expect(controller.isChanged, false);
        expect(notified, 1);
      });

      test('Changes when changing code, notifies once', () {
        controller.setExample(examplePython1);
        controller.fileControllers.first.codeController.text =
            examplePython1.files.first.content;

        expect(controller.isChanged, false);
        expect(notified, 1);

        controller.fileControllers.first.codeController.text = 'changed';

        expect(controller.isChanged, true);
        expect(notified, 2);

        controller.fileControllers.first.codeController.text = 'changed2';

        expect(controller.isChanged, true);
        expect(notified, 2);

        controller.fileControllers.first.codeController.text =
            examplePython1.files.first.content;

        expect(controller.isChanged, false);
        expect(notified, 3);
      });

      test('Changes when changing pipelineOptions, notifies once', () {
        controller.setExample(exampleGo5PipelineOptions);
        controller.pipelineOptions = exampleGo5PipelineOptions.pipelineOptions;

        expect(controller.isChanged, false);
        expect(notified, 1);

        controller.pipelineOptions = 'changed';

        expect(controller.isChanged, true);
        expect(notified, 2);

        controller.pipelineOptions = 'changed2';

        expect(controller.isChanged, true);
        expect(notified, 2);

        controller.pipelineOptions = exampleGo5PipelineOptions.pipelineOptions;

        expect(controller.isChanged, false);
        expect(notified, 3);
      });
    });

    group('Files.', () {
      test('activeFileController, activateFileControllerByName', () {
        expect(controller.activeFileController, null);

        controller.setExample(exampleGo4Multifile);

        expect(
          controller.activeFileController?.getFile().content,
          exampleGo4Multifile.files[1].content,
        );

        controller.activateFileControllerByName(
          exampleGo4Multifile.files[0].name,
        );
        expect(
          controller.activeFileController?.getFile().content,
          exampleGo4Multifile.files[0].content,
        );

        controller.activateFileControllerByName('nonexistent');
        expect(controller.activeFileController, null);
      });

      test('getFileControllerByName', () {
        controller.setExample(exampleGo4Multifile);

        expect(
          controller
              .getFileControllerByName(exampleGo4Multifile.files[0].name)
              ?.savedFile
              .content,
          exampleGo4Multifile.files[0].content,
        );
        expect(
          controller
              .getFileControllerByName(exampleGo4Multifile.files[1].name)
              ?.savedFile
              .content,
          exampleGo4Multifile.files[1].content,
        );
        expect(
          controller.getFileControllerByName('nonexistent'),
          null,
        );
      });
    });

    group('Descriptors.', () {
      test('Returns the original descriptor if unchanged', () {
        controller.setExample(
          examplePython1,
          descriptor: standardDescriptor1,
        );

        final descriptor = controller.getLoadingDescriptor();

        expect(descriptor, standardDescriptor1);
      });

      test('Returns a ContentExampleLoadingDescriptor if changed', () {
        controller.setExample(
          examplePython1,
          descriptor: standardDescriptor1,
        );

        controller.fileControllers.first.codeController.value =
            const TextEditingValue(text: 'ex4');
        final descriptor = controller.getLoadingDescriptor();

        const expected = ContentExampleLoadingDescriptor(
          complexity: Complexity.basic,
          files: [SnippetFile(content: 'ex4', isMain: true, name: '')],
          name: 'Example X1',
          sdk: Sdk.python,
        );

        expect(descriptor, expected);
      });

      test('Returns a ContentExampleLoadingDescriptor if no descriptor', () {
        controller.setExample(examplePython1, descriptor: null);

        controller.setExample(examplePython2, descriptor: null);
        final descriptor = controller.getLoadingDescriptor();

        const expected = ContentExampleLoadingDescriptor(
          complexity: Complexity.basic,
          files: [SnippetFile(content: 'ex2', isMain: true, name: '')],
          name: 'Kata',
          sdk: Sdk.python,
        );

        expect(descriptor, expected);
      });
    });

    group('EventSnippetContext.', () {
      test('Uninitialized', () {
        expect(
          controller.eventSnippetContext,
          const EventSnippetContext(
            originalSnippet: null,
            sdk: Sdk.python,
            snippet: null,
          ),
        );
      });

      test('Unchanged, changed', () {
        final descriptors = <ExampleLoadingDescriptor>[
          HttpExampleLoadingDescriptor(
            sdk: Sdk.go,
            uri: Uri.parse('https://example.com'),
          ),
          const StandardExampleLoadingDescriptor(
            sdk: Sdk.go,
            path: 'path',
          ),
          const UserSharedExampleLoadingDescriptor(
            sdk: Sdk.python,
            snippetId: 'id',
          ),
        ];

        for (final descriptor in descriptors) {
          controller.setExample(examplePython1, descriptor: descriptor);

          expect(
            controller.eventSnippetContext,
            EventSnippetContext(
              originalSnippet: descriptor.token,
              sdk: controller.sdk,
              snippet: descriptor.token,
            ),
          );

          controller.activeFileController!.codeController.text += 'changed';

          expect(
            controller.eventSnippetContext,
            EventSnippetContext(
              originalSnippet: descriptor.token,
              sdk: controller.sdk,
              snippet: null,
            ),
          );
        }
      });
    });
  });
}
