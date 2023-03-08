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
import 'package:playground/modules/messages/models/set_sdk_message.dart';
import 'package:playground_components/playground_components.dart';

const _sdk = Sdk.python;

void main() {
  group('SetSdkMessage.tryParse', () {
    test(
      'returns null for other types',
      () {
        const map = {'type': 'any-other'};

        final parsed = SetSdkMessage.tryParse(map);

        expect(parsed, null);
      },
    );

    test(
      'returns null for an empty message',
      () {
        const map = {'type': SetSdkMessage.type};

        final parsed = SetSdkMessage.tryParse(map);

        expect(parsed, null);
      },
    );

    test(
      'parses an SDK',
      () {
        final map = {'type': SetSdkMessage.type, 'sdk': _sdk.id};

        final parsed = SetSdkMessage.tryParse(map);

        expect(parsed, const SetSdkMessage(sdk: _sdk));
      },
    );

    test(
      'returns null for unknown SDK',
      () {
        const map = {'type': SetSdkMessage.type, 'sdk': 'not-existing'};

        final parsed = SetSdkMessage.tryParse(map);

        expect(parsed, null);
      },
    );
  });
}
