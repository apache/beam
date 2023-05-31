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
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:playground_components/playground_components.dart';

import '../common/descriptors.dart';
import '../common/examples.dart';
import 'playground_controller_test.mocks.dart';

@GenerateMocks([ExamplesLoader, ExampleCache])
Future<void> main() async {
  TestWidgetsFlutterBinding.ensureInitialized();
  await PlaygroundComponents.ensureInitialized();

  late PlaygroundController controller;
  final mockExamplesLoader = MockExamplesLoader();

  when(mockExamplesLoader.loadIfNew(any)).thenAnswer((_) async => 1);

  setUp(() {
    controller = PlaygroundController(
      examplesLoader: MockExamplesLoader(),
      exampleCache: MockExampleCache(),
    );
  });

  group('PlaygroundController', () {
    test('sdk is initially null, set sdk', () {
      expect(controller.sdk, null);
      controller.setSdk(Sdk.go);
      expect(controller.sdk, Sdk.go);
    });

    test('Initial value of examplesTitle', () {
      expect(controller.examplesTitle, 'examples.defaultTitle');
    });

    test('Initial value of isCodeRunning should be false', () {
      expect(controller.codeRunner.isCodeRunning, false);
    });

    test('Initial value of pipelineOptions should be empty string', () {
      controller.setSdk(Sdk.go);
      expect(controller.codeRunner.pipelineOptions, '');
    });

    test('source', () {
      expect(controller.source, null);
      controller.setSdk(Sdk.go);
      expect(controller.source, null);
      controller.snippetEditingController!.setExample(exampleGo4Multifile);
      expect(controller.source, exampleGo4Multifile.files[1].content);
    });

    group('isExampleChanged Tests', () {
      test(
        'If example source is changed, value of isExampleChanged should be true',
        () {
          controller.setExample(
            examplePython1,
            descriptor: emptyDescriptor,
            setCurrentSdk: true,
          );
          expect(controller.codeRunner.isExampleChanged, false);
          controller.snippetEditingController?.fileControllers.first
              .codeController.text = 'test';
          expect(controller.codeRunner.isExampleChanged, true);
        },
      );

      test(
        'If pipelineOptions is changed, value of isExampleChanged should be true',
        () {
          controller.setExample(
            examplePython1,
            descriptor: emptyDescriptor,
            setCurrentSdk: true,
          );
          expect(controller.codeRunner.isExampleChanged, false);
          controller.setPipelineOptions('test options');
          expect(controller.codeRunner.isExampleChanged, true);
        },
      );
    });

    test(
      'If selected example type is not test and SDK is java or python, graph should be available',
      () {
        controller.setExample(
          examplePython1,
          descriptor: emptyDescriptor,
          setCurrentSdk: true,
        );
        expect(controller.graphAvailable, true);
      },
    );

    test(
      'Playground state setExample should update source and example and notify all listeners',
      () {
        controller.addListener(() {
          expect(controller.sdk, Sdk.go);
          expect(controller.source, exampleGo6.files.first.content);
          expect(controller.selectedExample, exampleGo6);
        });
        controller.setExample(
          exampleGo6,
          descriptor: emptyDescriptor,
          setCurrentSdk: true,
        );
      },
    );

    test('Playground state should notify all listeners about sdk change', () {
      controller.addListener(() {
        expect(controller.sdk, Sdk.go);
      });
      controller.setSdk(Sdk.go);
    });

    test(
        'Playground state reset should reset source to example notify all listeners',
        () {
      controller.setExample(
        examplePython1,
        descriptor: emptyDescriptor,
        setCurrentSdk: true,
      );
      controller.snippetEditingController?.fileControllers.first.codeController
          .text = 'source';
      controller.addListener(() {
        expect(controller.source, examplePython1.files.first.content);
      });
      controller.reset();
    });

    test(
      'If Playground state result is empty, then resetError should break the execution',
      () {
        controller.resetErrorMessageText();
        expect(controller.codeRunner.result, null);
      },
    );

    test(
      'Playground state should notify all listeners about pipeline options change',
      () {
        controller.setSdk(Sdk.go);
        controller.addListener(() {
          expect(controller.codeRunner.pipelineOptions, 'test options');
        });
        controller.setPipelineOptions('test options');
      },
    );

    test('getLoadingDescriptor()', () {
      controller.setExample(
        examplePython2,
        descriptor: standardDescriptor2,
        setCurrentSdk: true,
      );
      controller.setExample(
        exampleGo6,
        descriptor: standardGoDescriptor,
        setCurrentSdk: false,
      );

      final descriptor = controller.getLoadingDescriptor();

      expect(
        descriptor,
        ExamplesLoadingDescriptor(
          descriptors: [
            standardDescriptor2,
            standardGoDescriptor,
          ],
          initialSdk: Sdk.python,
        ),
      );
    });

    test('setEmptyIfNoSdk', () {
      controller.setEmptyIfNoSdk(Sdk.go);

      expect(controller.sdk, Sdk.go);
      expect(
        controller.snippetEditingController?.fileControllers.first
            .codeController.fullText,
        '',
      );

      controller.setEmptyIfNoSdk(Sdk.python);

      expect(controller.sdk, Sdk.go);
    });

    group('setEmptyIfNotExists', () {
      test('setCurrentSdk = false', () {
        controller.setEmptyIfNotExists(Sdk.go, setCurrentSdk: false);

        expect(controller.sdk, null);
      });

      test('setCurrentSdk = true', () {
        const text = 'test';
        controller.setEmptyIfNotExists(Sdk.go, setCurrentSdk: true);

        expect(controller.sdk, Sdk.go);

        controller.snippetEditingController?.fileControllers.first
            .codeController.text = text;
        controller.setEmptyIfNotExists(Sdk.go, setCurrentSdk: true);

        expect(
          controller.snippetEditingController?.fileControllers.first
              .codeController.fullText,
          text,
        );
      });
    });
  });
}
