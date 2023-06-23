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
import 'package:integration_test/integration_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/examples.dart';
import 'common/widget_tester.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  testWidgets('Default examples', (WidgetTester wt) async {
    await init(wt);

    final playgroundController = wt.findPlaygroundController();

    for (final entry in defaultExamples.entries) {
      final sdk = entry.key;
      final example = entry.value;

      print('${sdk.id}: ${example.path}');

      if (sdk != playgroundController.sdk) {
        await wt.changeSdk(sdk);
      }

      await _expectExample(example, wt);
      await wt.runExpectCached(example);
      await wt.modifyRunExpectReal(example);
    }
  });
}

Future<void> _expectExample(ExampleDescriptor example, WidgetTester wt) async {
  final visibleText = await example.getVisibleText();

  expectSdk(example.sdk, wt);
  expectVisibleTextIfDeployed(visibleText, wt);

  if (example.hasGraphTab) {
    expect(find.graphTab(), findsOneWidget);
  }

  expect(find.resultTab(), findsOneWidget);
  expect(wt.findOutputTabController().currentKey, OutputTabEnum.result);
}
