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
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'package:playground/modules/messages/models/set_content_message.dart';
import 'package:playground_components/playground_components.dart';

const _content1 = 'my_code1';
const _content2 = 'my_code2';
const _sdk = Sdk.python;

void main() {
  group('SetContentMessage.tryParse', () {
    test(
      'returns null for other types',
      () {
        const map = {'type': 'any-other'};

        final parsed = SetContentMessage.tryParse(map);

        expect(parsed, null);
      },
    );

    test(
      'parses a single empty Java descriptor when without descriptor',
      () {
        const map = {'type': SetContentMessage.type};

        final parsed = SetContentMessage.tryParse(map);

        expect(
          parsed,
          SetContentMessage(
            descriptor: ExamplesLoadingDescriptor(
              descriptors: const [EmptyExampleLoadingDescriptor(sdk: Sdk.java)],
              lazyLoadDescriptors:
                  ExamplesLoadingDescriptorFactory.defaultLazyLoadDescriptors,
            ),
          ),
        );
      },
    );

    test(
      'parses a single empty Java descriptor with an invalid descriptor',
      () {
        const map = {
          'type': SetContentMessage.type,
          'descriptor': 123,
        };

        final parsed = SetContentMessage.tryParse(map);

        expect(
          parsed,
          SetContentMessage(
            descriptor: ExamplesLoadingDescriptor(
              descriptors: const [EmptyExampleLoadingDescriptor(sdk: Sdk.java)],
              lazyLoadDescriptors:
                  ExamplesLoadingDescriptorFactory.defaultLazyLoadDescriptors,
            ),
          ),
        );
      },
    );

    test(
      'parses messages',
      () {
        final map = {
          'type': SetContentMessage.type,
          'descriptor': {
            'examples': [
              null,
              1,
              1.0,
              'string',
              [],
              {'type': 'any-other'},
              {
                'files': [
                  {'content': _content1, 'isMain': false, 'name': '1'},
                  {'content': _content2, 'isMain': true, 'name': '2'},
                ],
                'name': 'name',
                'sdk': _sdk.id,
                'complexity': 'basic',
              },
              {
                'files': [
                  {'content': _content1, 'isMain': false, 'name': '1'}
                ],
                'name': null,
                'sdk': _sdk.id,
                'complexity': 'medium',
              },
              {
                'files': [
                  {'content': _content1, 'isMain': false, 'name': '1'}
                ],
                'sdk': _sdk.id,
                'complexity': 'advanced',
              },
            ],
          },
        };

        final parsed = SetContentMessage.tryParse(map);

        expect(
          parsed,
          SetContentMessage(
            descriptor: ExamplesLoadingDescriptor(
              descriptors: const [
                ContentExampleLoadingDescriptor(
                  complexity: Complexity.basic,
                  files: [
                    SnippetFile(content: _content1, isMain: false, name: '1'),
                    SnippetFile(content: _content2, isMain: true, name: '2'),
                  ],
                  name: 'name',
                  sdk: _sdk,
                ),
                ContentExampleLoadingDescriptor(
                  complexity: Complexity.medium,
                  files: [
                    SnippetFile(content: _content1, isMain: false, name: '1'),
                  ],
                  name: null,
                  sdk: _sdk,
                ),
                ContentExampleLoadingDescriptor(
                  complexity: Complexity.advanced,
                  files: [
                    SnippetFile(content: _content1, isMain: false, name: '1'),
                  ],
                  name: null,
                  sdk: _sdk,
                ),
              ],
              lazyLoadDescriptors:
                  ExamplesLoadingDescriptorFactory.defaultLazyLoadDescriptors,
            ),
          ),
        );
      },
    );
  });
}
