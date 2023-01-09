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
import 'package:playground_components/src/controllers/example_loaders/user_shared_example_loader.dart';

import '../../common/example_cache.dart';

void main() async {
  TestWidgetsFlutterBinding.ensureInitialized();

  group('UserSharedExampleLoader', () {
    testWidgets('non-existent', (WidgetTester wt) async {
      Exception? thrown;
      final loader = UserSharedExampleLoader(
        descriptor: const UserSharedExampleLoadingDescriptor(
          sdk: Sdk.go,
          snippetId: 'non-existent',
        ),
        exampleCache: createFailingExampleCache(),
      );

      try {
        await loader.future;
      } on Exception catch (ex) {
        thrown = ex;
      }

      expect(thrown, isA<Exception>());
    });
  });
}
