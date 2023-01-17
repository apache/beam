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

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_body.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_input.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/common_finders.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'Changing pipeline options and press run',
    (WidgetTester wt) async {
      await init(wt);

      await wt.tapAndSettle(find.appDropdownButtonWithText('Pipeline Options'));

      await _addTwoOptions(wt);

      await wt.tapAndSettle(find.pipelineOptionsRawTab());

      _checkIfRawTextCorrect('--some test --some2 test2');

      await wt.tapAndSettle(find.pipelineOptionsOptionsTab());

      await wt.tap(find.byKey(const ValueKey('PipelineOptionsDelete1')));

      await wt.tapAndSettle(find.pipelineOptionsRawTab());

      _checkIfRawTextCorrect('--some test');

      await wt.tapAndSettle(find.pipelineOptionsSaveAndCloseButton());

      await wt.tap(find.runOrCancelButton());
      await Future.delayed(const Duration(milliseconds: 300));

      await wt.tapAndSettle(find.runOrCancelButton());

      final playgroundController = wt.findPlaygroundController();
      expect(playgroundController.outputResult, contains('Pipeline cancelled'));
    },
  );
}

Future<void> _addTwoOptions(WidgetTester wt) async {
  await wt.enterText(
    find.byKey(const ValueKey('PipelineOptionsKey0')),
    'some',
  );
  await wt.enterText(
    find.byKey(const ValueKey('PipelineOptionsValue0')),
    'test',
  );
  
  await wt.tap(find.byKey(PipelineOptionsDropdownBody.addOptionButtonKey));
  await wt.pumpAndSettle();
  
  await wt.enterText(
    find.byKey(const ValueKey('PipelineOptionsKey1')),
    'some2',
  );
  await wt.enterText(
    find.byKey(const ValueKey('PipelineOptionsValue1')),
    'test2',
  );
}

void _checkIfRawTextCorrect(String text) {
  expect(
    find.descendant(
        of: find.byKey(PipelineOptionsDropdownInput.textFieldKey),
        matching: find.text(text)),
    findsOneWidget,
  );
}
