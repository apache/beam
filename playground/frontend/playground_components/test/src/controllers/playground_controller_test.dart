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

  when(mockExamplesLoader.load(any)).thenAnswer((_) async => 1);

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

    test('Initial value of examplesTitle should be equal to kTitle', () {
      expect(controller.examplesTitle, kTitle);
    });

    test('Initial value of isCodeRunning should be false', () {
      expect(controller.isCodeRunning, false);
    });

    test('Initial value of pipelineOptions should be empty string', () {
      expect(controller.pipelineOptions, null);
      controller.setSdk(Sdk.go);
      expect(controller.pipelineOptions, '');
    });

    test('Initial value of source should be empty string', () {
      expect(controller.source, null);
      controller.setSdk(Sdk.go);
      expect(controller.source, '');
    });

    group('isExampleChanged Tests', () {
      test(
        'If example source is changed, value of isExampleChanged should be true',
        () {
          controller.setExample(
            exampleMock1,
            descriptor: emptyDescriptor,
            setCurrentSdk: true,
          );
          expect(controller.isExampleChanged, false);
          controller.setSource('test');
          expect(controller.isExampleChanged, true);
        },
      );

      test(
        'If pipelineOptions is changed, value of isExampleChanged should be true',
        () {
          controller.setExample(
            exampleMock1,
            descriptor: emptyDescriptor,
            setCurrentSdk: true,
          );
          expect(controller.isExampleChanged, false);
          controller.setPipelineOptions('test options');
          expect(controller.isExampleChanged, true);
        },
      );
    });

    test(
      'If selected example type is not test and SDK is java or python, graph should be available',
      () {
        controller.setExample(
          exampleMock1,
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
          expect(controller.source, exampleMockGo.source);
          expect(controller.selectedExample, exampleMockGo);
        });
        controller.setExample(
          exampleMockGo,
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
        exampleMock1,
        descriptor: emptyDescriptor,
        setCurrentSdk: true,
      );
      controller.setSource('source');
      controller.addListener(() {
        expect(controller.source, exampleMock1.source);
      });
      controller.reset();
    });

    test(
      'If Playground state result is empty, then resetError should break the execution',
      () {
        controller.resetError();
        expect(controller.result, null);
      },
    );

    test(
      'Playground state should notify all listeners about pipeline options change',
      () {
        controller.setSdk(Sdk.go);
        controller.addListener(() {
          expect(controller.pipelineOptions, 'test options');
        });
        controller.setPipelineOptions('test options');
      },
    );

    test('getLoadingDescriptor()', () {
      controller.setExample(
        exampleMock2,
        descriptor: standardDescriptor2,
        setCurrentSdk: true,
      );
      controller.setExample(
        exampleMockGo,
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
        controller.requireSnippetEditingController().codeController.text,
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

        controller.requireSnippetEditingController().setSource(text);
        controller.setEmptyIfNotExists(Sdk.go, setCurrentSdk: true);

        expect(
          controller.requireSnippetEditingController().codeController.text,
          text,
        );
      });
    });
  });
}
