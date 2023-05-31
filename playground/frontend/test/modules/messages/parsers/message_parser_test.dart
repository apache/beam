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
import 'package:playground/modules/messages/models/set_content_message.dart';
import 'package:playground/modules/messages/models/set_sdk_message.dart';
import 'package:playground/modules/messages/parsers/messages_parser.dart';
import 'package:playground_components/playground_components.dart';

const _sdk = Sdk.python;

void main() {
  group('MessageParser.parse returns null for invalid inputs', () {
    test(
      'MessageParser.parse returns null for null',
      () {
        const value = null;

        final parsed = MessagesParser().tryParse(value);

        expect(parsed, null);
      },
    );

    test(
      'MessageParser.parse returns null for non-string and non-map',
      () {
        final value = DateTime(2022);

        final parsed = MessagesParser().tryParse(value);

        expect(parsed, null);
      },
    );

    test(
      'MessageParser.parse returns null for a non-JSON string',
      () {
        const value = 'non-JSON string';

        final parsed = MessagesParser().tryParse(value);

        expect(parsed, null);
      },
    );

    test(
      'MessageParser.parse returns null for a JSON scalar',
      () {
        const value = '123';

        final parsed = MessagesParser().tryParse(value);

        expect(parsed, null);
      },
    );

    test(
      'MessageParser.parse returns null for an alien map',
      () {
        const value = {'key': 'value'};

        final parsed = MessagesParser().tryParse(value);

        expect(parsed, null);
      },
    );
  });

  group('MessageParser.parse parses messages', () {
    test(
      'MessageParser.parse parses SetContentMessage',
      () {
        const value = {'type': SetContentMessage.type};

        final parsed = MessagesParser().tryParse(value);

        expect(
          parsed,
          SetContentMessage(
            descriptor: ExamplesLoadingDescriptor(
              descriptors: const [EmptyExampleLoadingDescriptor(sdk: Sdk.java)],
              lazyLoadDescriptors: {
                Sdk.go: const [
                  CatalogDefaultExampleLoadingDescriptor(sdk: Sdk.go),
                ],
                Sdk.java: const [
                  CatalogDefaultExampleLoadingDescriptor(sdk: Sdk.java),
                ],
                Sdk.python: const [
                  CatalogDefaultExampleLoadingDescriptor(sdk: Sdk.python),
                ],
                Sdk.scio: const [
                  CatalogDefaultExampleLoadingDescriptor(sdk: Sdk.scio),
                ],
              },
            ),
          ),
        );
      },
    );

    test(
      'MessageParser.parse parses SetSdkMessage',
      () {
        final value = {'type': SetSdkMessage.type, 'sdk': _sdk.id};

        final parsed = MessagesParser().tryParse(value);

        expect(parsed, const SetSdkMessage(sdk: _sdk));
      },
    );
  });
}
