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

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/common_finders.dart';
import 'standalone_cancel_running_example_test.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'Changing pipeline options and press run',
    (WidgetTester wt) async {
      await init(wt);

      await wt.tapAndSettle(find.pipelineOptionsDropdown());

      await _addTwoOptions(wt);

      await wt.tapAndSettle(find.pipelineOptionsRawTab());

      _expectRawText('--some test --some2 test2');

      await wt.tapAndSettle(find.pipelineOptionsListTab());

      await wt.tap(_getDeleteIconByIndex(1, wt));

      await wt.tapAndSettle(find.pipelineOptionsRawTab());

      _expectRawText('--some test');

      await wt.tapAndSettle(find.pipelineOptionsSaveAndCloseButton());

      await wt.tap(find.runOrCancelButton());
      await Future.delayed(const Duration(milliseconds: 300));

      // Cancel execution just for test speed.
      await wt.tapAndSettle(find.runOrCancelButton());

      final playgroundController = wt.findPlaygroundController();
      expect(
        playgroundController.codeRunner.resultLogOutput,
        contains('--some test'),
      );
      expect(
        playgroundController.codeRunner.resultLogOutput,
        contains(kExecutionCancelledText),
      );
    },
  );
}

Future<void> _addTwoOptions(WidgetTester wt) async {
  await wt.enterText(
    find.byType(PipelineOptionsTextField).horizontallyAt(0, wt),
    'some',
  );
  await wt.enterText(
    find.byType(PipelineOptionsTextField).horizontallyAt(1, wt),
    'test',
  );

  await wt
      .tapAndSettle(find.byKey(PipelineOptionsDropdownBody.addOptionButtonKey));

  await wt.enterText(
    find
        .byType(PipelineOptionsRow)
        .verticallyAt(1, wt)
        .getChildrenByType(PipelineOptionsTextField)
        .horizontallyAt(0, wt),
    'some2',
  );

  await wt.enterText(
    find
        .byType(PipelineOptionsRow)
        .verticallyAt(1, wt)
        .getChildrenByType(PipelineOptionsTextField)
        .horizontallyAt(1, wt),
    'test2',
  );
}

Finder _getDeleteIconByIndex(int index, WidgetTester wt) {
  return find.descendant(
    of: find.byType(PipelineOptionsRow).verticallyAt(index, wt),
    matching: find.byIcon(Icons.delete_outlined),
  );
}

void _expectRawText(String text) {
  expect(
    find.descendant(
        of: find.byKey(PipelineOptionsDropdownInput.textFieldKey),
        matching: find.text(text)),
    findsOneWidget,
  );
}
