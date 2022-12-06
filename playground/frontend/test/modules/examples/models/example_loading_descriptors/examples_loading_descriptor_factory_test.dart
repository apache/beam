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

import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:playground/constants/params.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'package:playground_components/playground_components.dart';

const _viewOptionsMap = {
  'readonly': 'readonly1,readonly2',
  'show': 'show1,show2',
  'unfold': 'unfold1,unfold2',
};

const _viewOptions = ExampleViewOptions(
  foldCommentAtLineZero: true,
  foldImports: true,
  readOnlySectionNames: ['readonly1', 'readonly2'],
  showSectionNames: ['show1', 'show2'],
  unfoldSectionNames: ['unfold1', 'unfold2'],
);

void main() {
  final lazy = ExamplesLoadingDescriptorFactory.defaultLazyLoadDescriptors;

  group('ExamplesLoadingDescriptorFactory', () {
    test('defaultLazyLoadDescriptors', () {
      final expected = {
        Sdk.go: [
          const CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.go,
          )
        ],
        Sdk.java: [
          const CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.java,
          )
        ],
        Sdk.python: [
          const CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.python,
          )
        ],
        Sdk.scio: [
          const CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.scio,
          )
        ],
      };

      expect(lazy, expected);
    });

    group('fromUriParts', () {
      void testExamples(Iterable<_Example> examples) {
        for (final example in examples) {
          final result = ExamplesLoadingDescriptorFactory.fromStandaloneParams(
            example.params,
          );

          expect(result, example.expected);
        }
      }

      test('ContentExampleLoadingDescriptor', () {
        testExamples([
          _Example(
            params: {
              kExamplesParam: jsonEncode(
                [
                  {
                    'sdk': 'go',
                    'content': 'go_content',
                  },
                  {
                    'sdk': 'python',
                    'content': 'python_content',
                    ..._viewOptionsMap,
                  },
                ],
              ),
            },
            expected: ExamplesLoadingDescriptor(
              descriptors: const [
                ContentExampleLoadingDescriptor(
                  content: 'go_content',
                  sdk: Sdk.go,
                ),
                ContentExampleLoadingDescriptor(
                  content: 'python_content',
                  sdk: Sdk.python,
                  viewOptions: _viewOptions,
                ),
              ],
              lazyLoadDescriptors: lazy,
            ),
          ),
        ]);
      });

      test('HttpExampleLoadingDescriptor', () {
        testExamples([
          _Example(
            params: {
              kExamplesParam: jsonEncode(
                [
                  {
                    'sdk': 'go',
                    'url': 'http://',
                  },
                  {
                    'sdk': 'python',
                    'url': 'https://',
                    ..._viewOptionsMap,
                  },
                ],
              ),
            },
            expected: ExamplesLoadingDescriptor(
              descriptors: [
                HttpExampleLoadingDescriptor(
                  sdk: Sdk.go,
                  uri: Uri.parse('http://'),
                ),
                HttpExampleLoadingDescriptor(
                  sdk: Sdk.python,
                  uri: Uri.parse('https://'),
                  viewOptions: _viewOptions,
                ),
              ],
              lazyLoadDescriptors: lazy,
            ),
          ),
        ]);
      });

      test('StandardExampleLoadingDescriptor', () {
        testExamples([
          _Example(
            params: {
              kExamplesParam: jsonEncode(
                [
                  {'sdk': 'go', 'path': 'SDK_GO'},
                  {
                    'sdk': 'python',
                    'path': 'SDK_PYTHON/something',
                    ..._viewOptionsMap,
                  },
                ],
              ),
            },
            expected: ExamplesLoadingDescriptor(
              descriptors: const [
                StandardExampleLoadingDescriptor(
                  sdk: Sdk.go,
                  path: 'SDK_GO',
                ),
                StandardExampleLoadingDescriptor(
                  sdk: Sdk.python,
                  path: 'SDK_PYTHON/something',
                  viewOptions: _viewOptions,
                ),
              ],
              lazyLoadDescriptors: lazy,
            ),
          ),
        ]);
      });

      test('UserSharedExampleLoadingDescriptor', () {
        testExamples([
          _Example(
            params: {
              kExamplesParam: jsonEncode(
                [
                  {'sdk': 'go', 'shared': ''},
                  {'sdk': 'python', 'shared': '123'},
                  {'sdk': 'scio', 'shared': 'abc', ..._viewOptionsMap},
                ],
              ),
            },
            expected: ExamplesLoadingDescriptor(
              descriptors: const [
                UserSharedExampleLoadingDescriptor(
                  sdk: Sdk.go,
                  snippetId: '',
                ),
                UserSharedExampleLoadingDescriptor(
                  sdk: Sdk.python,
                  snippetId: '123',
                ),
                UserSharedExampleLoadingDescriptor(
                  sdk: Sdk.scio,
                  snippetId: 'abc',
                  viewOptions: _viewOptions,
                ),
              ],
              lazyLoadDescriptors: lazy,
            ),
          ),
        ]);
      });
    });
  });
}

class _Example {
  final Map<String, dynamic> params;
  final ExamplesLoadingDescriptor expected;

  const _Example({
    required this.params,
    required this.expected,
  });
}
