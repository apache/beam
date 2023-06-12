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

import 'common.dart';
import 'examples_loader_test.mocks.dart';

@GenerateMocks([PlaygroundController, ExampleCache])
void main() async {
  late ExamplesLoader examplesLoader;
  late MockPlaygroundController playgroundController;
  final setExampleTrue = <String>[];
  final setExampleFalse = <String>[];
  final setEmptyIfNotExistsTrue = <Sdk>[];
  final setEmptyIfNotExistsFalse = <Sdk>[];

  await PlaygroundComponents.ensureInitialized();

  setUp(() {
    setExampleTrue.clear();
    setExampleFalse.clear();
    setEmptyIfNotExistsTrue.clear();
    setEmptyIfNotExistsFalse.clear();

    playgroundController = MockPlaygroundController();

    when(
      playgroundController.setExample(
        any,
        descriptor: anyNamed('descriptor'),
        setCurrentSdk: true,
      ),
    ).thenAnswer((realInvocation) {
      final example = realInvocation.positionalArguments[0] as Example;
      setExampleTrue.add(example.name);
    });
    when(
      playgroundController.setExample(
        any,
        descriptor: anyNamed('descriptor'),
        setCurrentSdk: false,
      ),
    ).thenAnswer((realInvocation) {
      final example = realInvocation.positionalArguments[0] as Example;
      setExampleFalse.add(example.name);
    });
    when(playgroundController.setEmptyIfNotExists(any, setCurrentSdk: true))
        .thenAnswer((realInvocation) {
      final sdk = realInvocation.positionalArguments[0] as Sdk;
      setEmptyIfNotExistsTrue.add(sdk);
    });
    when(playgroundController.setEmptyIfNotExists(any, setCurrentSdk: false))
        .thenAnswer((realInvocation) {
      final sdk = realInvocation.positionalArguments[0] as Sdk;
      setEmptyIfNotExistsFalse.add(sdk);
    });

    final exampleCache = MockExampleCache();
    when(playgroundController.exampleCache).thenReturn(exampleCache);

    examplesLoader = ExamplesLoader();
    examplesLoader.setPlaygroundController(playgroundController);
    TestExampleLoader.register(examplesLoader.defaultFactory);
  });

  group('ExamplesLoader.', () {
    group('load.', () {
      group('Success.', () {
        test('Race to set current SDK if not set', () async {
          final descriptor = ExamplesLoadingDescriptor(
            descriptors: const [
              TestExampleLoadingDescriptor(Sdk.go),
              TestExampleLoadingDescriptor(Sdk.python),
            ],
            lazyLoadDescriptors: {
              Sdk.scio: const [TestExampleLoadingDescriptor(Sdk.scio)],
            },
          );

          await examplesLoader.loadIfNew(descriptor);

          expect(setExampleTrue, [Sdk.go.id, Sdk.python.id]);
          expect(setExampleFalse, []);
        });

        test('Example with initialSdk sets the current SDK', () async {
          final descriptor = ExamplesLoadingDescriptor(
            descriptors: const [
              TestExampleLoadingDescriptor(Sdk.go),
              TestExampleLoadingDescriptor(Sdk.python),
            ],
            lazyLoadDescriptors: {
              Sdk.scio: const [TestExampleLoadingDescriptor(Sdk.scio)],
            },
            initialSdk: Sdk.python,
          );

          await examplesLoader.loadIfNew(descriptor);

          expect(setExampleTrue, [Sdk.python.id]);
          expect(setExampleFalse, [Sdk.go.id]);
        });
      });

      group('Error.', () {
        test('Load empty example instead', () async {
          const descriptor = ExamplesLoadingDescriptor(
            descriptors: [
              TestExampleLoadingDescriptor(Sdk.go, succeed: false),
              TestExampleLoadingDescriptor(Sdk.python, succeed: false),
            ],
            initialSdk: Sdk.python,
          );

          await examplesLoader.loadIfNew(descriptor);

          expect(setEmptyIfNotExistsTrue, [Sdk.python]);
          expect(setEmptyIfNotExistsFalse, [Sdk.go]);
        });
      });

      // TODO(alexeyinkin): Test lazy loading, https://github.com/apache/beam/issues/24351
    });
  });
}
