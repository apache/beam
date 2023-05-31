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

import 'common.dart';

void main() {
  group('ExamplesLoadingDescriptor', () {
    test('Empty map -> null', () {
      final parsed = ExamplesLoadingDescriptor.tryParse(
        const {},
        singleDescriptorFactory: _singleDescriptorFactory,
      );

      expect(parsed, null);
    });

    test('Empty instant descriptors -> null', () {
      // ignore: use_named_constants
      const descriptor = ExamplesLoadingDescriptor(
        descriptors: [],
      );

      final parsed = ExamplesLoadingDescriptor.tryParse(
        descriptor.toJson(),
        singleDescriptorFactory: _singleDescriptorFactory,
      );

      expect(parsed, null);
    });

    test('Multiple instant descriptors', () {
      const descriptor = ExamplesLoadingDescriptor(
        descriptors: [_d1, _d2, _unparsed],
      );

      final parsed = ExamplesLoadingDescriptor.tryParse(
        descriptor.toJson(),
        singleDescriptorFactory: _singleDescriptorFactory,
      );

      expect(
        parsed,
        const ExamplesLoadingDescriptor(
          descriptors: [_d1, _d2],
          initialSdk: null,
          lazyLoadDescriptors: {},
        ),
      );
    });

    test('Multiple instant and lazy descriptors', () {
      final descriptor = ExamplesLoadingDescriptor(
        descriptors: const [_d1, _d2, _unparsed],
        initialSdk: Sdk.python,
        lazyLoadDescriptors: {
          Sdk.scio: const [_d2],
          Sdk.python: const [_unparsed],
        },
      );

      final parsed = ExamplesLoadingDescriptor.tryParse(
        descriptor.toJson(),
        singleDescriptorFactory: _singleDescriptorFactory,
      );

      expect(
        parsed,
        ExamplesLoadingDescriptor(
          descriptors: const [_d1, _d2],
          initialSdk: Sdk.python,
          lazyLoadDescriptors: {
            Sdk.scio: const [_d2],
            Sdk.python: const [],
          },
        ),
      );
    });

    test('copyWithMissingLazy', () {
      final descriptor = ExamplesLoadingDescriptor(
        descriptors: const [_d1],
        initialSdk: Sdk.python,
        lazyLoadDescriptors: {
          Sdk.scio: const [_d2],
          Sdk.python: const [_unparsed],
        },
      );

      final result = descriptor.copyWithMissingLazy({
        Sdk.scio: const [_d1],
        Sdk.go: const [_d2],
      });

      expect(
        result,
        ExamplesLoadingDescriptor(
          descriptors: const [_d1],
          initialSdk: Sdk.python,
          lazyLoadDescriptors: {
            Sdk.scio: const [_d2], //         Original, not overwritten.
            Sdk.python: const [_unparsed], // Original.
            Sdk.go: const [_d2], //           Added.
          },
        ),
      );
    });

    test('copyWithoutViewOptions', () {
      final descriptorWithOptions = ExamplesLoadingDescriptor(
        descriptors: const [
          CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.go,
            viewOptions: viewOptions,
          ),
        ],
        initialSdk: Sdk.python,
        lazyLoadDescriptors: {
          Sdk.scio: const [
            StandardExampleLoadingDescriptor(
              sdk: Sdk.scio,
              path: 'path',
              viewOptions: viewOptions,
            ),
          ],
        },
      );

      final expected = ExamplesLoadingDescriptor(
        descriptors: const [_d1],
        initialSdk: Sdk.python,
        lazyLoadDescriptors: {
          Sdk.scio: const [_d2],
        },
      );

      expect(
        descriptorWithOptions.copyWithoutViewOptions(),
        expected,
      );
    });

    group('initialSnippetSdk, initialSnippetToken.', () {
      test('No descriptors -> null', () {
        final descriptor = ExamplesLoadingDescriptor(
          descriptors: const [],
          initialSdk: Sdk.go,
          lazyLoadDescriptors: {
            Sdk.scio: const [_d1],
          },
        );

        expect(descriptor.initialSnippetSdk, null);
        expect(descriptor.initialSnippetToken, null);
      });

      test('No initialSdk, multiple descriptors -> null', () {
        final descriptor = ExamplesLoadingDescriptor(
          descriptors: const [_d1, _d2],
          initialSdk: null,
          lazyLoadDescriptors: {
            Sdk.scio: const [_d1],
          },
        );

        expect(descriptor.initialSnippetSdk, null);
        expect(descriptor.initialSnippetToken, null);
      });

      test('No initialSdk, single descriptor -> from it', () {
        final descriptor = ExamplesLoadingDescriptor(
          descriptors: const [_d2],
          initialSdk: null,
          lazyLoadDescriptors: {
            Sdk.scio: const [_d1],
          },
        );

        expect(descriptor.initialSnippetSdk, _d2.sdk);
        expect(descriptor.initialSnippetToken, _d2.token);
      });

      test('initialSdk, multiple descriptors -> initialSdk', () {
        final descriptor = ExamplesLoadingDescriptor(
          descriptors: const [_d1, _d2],
          initialSdk: _d2.sdk,
          lazyLoadDescriptors: {
            Sdk.scio: const [_d1],
          },
        );

        expect(descriptor.initialSnippetSdk, _d2.sdk);
        expect(descriptor.initialSnippetToken, _d2.token);
      });
    });
  });
}

ExampleLoadingDescriptor? _singleDescriptorFactory(Map<String, dynamic> map) {
  return CatalogDefaultExampleLoadingDescriptor.tryParse(map) ??
      StandardExampleLoadingDescriptor.tryParse(map);
}

const _d1 = CatalogDefaultExampleLoadingDescriptor(sdk: Sdk.go);
const _d2 = StandardExampleLoadingDescriptor(sdk: Sdk.scio, path: 'path');
const _unparsed = EmptyExampleLoadingDescriptor(sdk: Sdk.python);
