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
import 'package:playground_components/playground_components.dart';

import 'common.dart';

void main() {
  const snippetId = 'snippetId';

  group('UserSharedExampleLoadingDescriptor', () {
    const descriptor = UserSharedExampleLoadingDescriptor(
      sdk: Sdk.go,
      snippetId: snippetId,
      viewOptions: viewOptions,
    );

    test('toJson -> tryParse', () {
      final map = descriptor.toJson();
      final parsed = UserSharedExampleLoadingDescriptor.tryParse(map);

      expect(parsed, descriptor);
    });

    test('copyWithoutViewOptions', () {
      expect(
        descriptor.copyWithoutViewOptions(),
        UserSharedExampleLoadingDescriptor(
          sdk: descriptor.sdk,
          snippetId: descriptor.snippetId,
        ),
      );
    });

    test('token', () {
      expect(descriptor.token, snippetId);
    });
  });
}
