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
import 'package:playground/modules/shortcuts/constants/global_shortcuts.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'Shortcuts using test',
    (WidgetTester wt) async {
      await init(wt);

      final controller = wt.findPlaygroundController();

      await _checkResetShortcut(wt, controller);
      await _checkRunAndClearShortcuts(wt, controller);
    },
  );
}

Future<void> _checkResetShortcut(
  WidgetTester wt,
  PlaygroundController controller,
) async {
  final startSource = controller.source;
  await wt.enterCodeFieldText('print("Hello World!');

  expect(controller.source, isNot(startSource));

  await wt.runShortcut(controller.resetShortcut);
  await wt.pumpAndSettle();

  expect(startSource, controller.source);
}

Future<void> _checkRunAndClearShortcuts(
  WidgetTester wt,
  PlaygroundController controller,
) async {
  final oldOutput = controller.codeRunner.resultLogOutput;
  final runShortcuts = [
    BeamMainRunShortcut(onInvoke: () {}),
    BeamNumpadRunShortcut(onInvoke: () {}),
  ];

  for (final shortcut in runShortcuts) {
    await wt.runShortcut(shortcut);
    await wt.pumpAndSettle();

    expect(controller.codeRunner.resultLogOutput, isNot(oldOutput));

    expect(controller.codeRunner.resultLogOutput, isNotEmpty);
    expect(controller.codeRunner.resultLogOutput, isNotNull);

    await wt.runShortcut(kClearOutputShortcut);
    await wt.pumpAndSettle();

    expect(controller.codeRunner.resultLogOutput, isEmpty);
  }
}
