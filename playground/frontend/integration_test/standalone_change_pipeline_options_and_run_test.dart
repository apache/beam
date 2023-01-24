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
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_body.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_input.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_text_field.dart';
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

      await wt.tapAndSettle(find.pipelineOptionsListTab());

      await wt.tap(_getBottomDeleteIcon(wt));

      await wt.tapAndSettle(find.pipelineOptionsRawTab());

      _checkIfRawTextCorrect('--some test');

      await wt.tapAndSettle(find.pipelineOptionsSaveAndCloseButton());

      await wt.tap(find.runOrCancelButton());
      await Future.delayed(const Duration(milliseconds: 300));

      await wt.tapAndSettle(find.runOrCancelButton());

      final playgroundController = wt.findPlaygroundController();
      expect(playgroundController.outputResult, contains('--some test'));
      expect(playgroundController.outputResult, contains('Pipeline cancelled'));
    },
  );
}

Future<void> _addTwoOptions(WidgetTester wt) async {
  await wt.enterText(
    _getPipelineOptionsTextField(wt, _Placement.topLeft),
    'some',
  );
  await wt.enterText(
    _getPipelineOptionsTextField(wt, _Placement.topRight),
    'test',
  );

  await wt.tap(find.byKey(PipelineOptionsDropdownBody.addOptionButtonKey));
  await wt.pumpAndSettle();

  await wt.enterText(
    _getPipelineOptionsTextField(wt, _Placement.bottomLeft),
    'some2',
  );
  await wt.enterText(
    _getPipelineOptionsTextField(wt, _Placement.bottomRight),
    'test2',
  );
}

Finder _getPipelineOptionsTextField(WidgetTester wt, _Placement placement) {
  final fields = find.byType(PipelineOptionsTextField);
  final positions = <_Point>[];
  for (var i = 0; i < fields.evaluate().length; i++) {
    final element = fields.at(i);
    final position = wt.getCenter(element);
    positions.add(_Point(position.dx, position.dy));
  }

  late _Point Function(_Point, _Point) reduceFunc;
  switch (placement) {
    case _Placement.topLeft:
      reduceFunc = (a, b) => a.x <= b.x && a.y <= b.y ? a : b;
      break;
    case _Placement.topRight:
      reduceFunc = (a, b) => a.x >= b.x && a.y <= b.y ? a : b;
      break;
    case _Placement.bottomLeft:
      reduceFunc = (a, b) => a.x <= b.x && a.y >= b.y ? a : b;
      break;
    case _Placement.bottomRight:
      reduceFunc = (a, b) => a.x >= b.x && a.y >= b.y ? a : b;
      break;
  }
  final position = positions.reduce(reduceFunc);
  return fields.at(positions.indexOf(position));
}

enum _Placement { topLeft, topRight, bottomLeft, bottomRight }

class _Point {
  final double x;
  final double y;

  _Point(this.x, this.y);
}

Finder _getBottomDeleteIcon(WidgetTester wt) {
  Finder deleteIcons = find.byIcon(Icons.delete_outlined);

  Finder bottomIcon =
      wt.getCenter(deleteIcons.at(0)).dy > wt.getCenter(deleteIcons.at(1)).dy
          ? deleteIcons.at(0)
          : deleteIcons.at(1);
  return bottomIcon;
}

void _checkIfRawTextCorrect(String text) {
  expect(
    find.descendant(
        of: find.byKey(PipelineOptionsDropdownInput.textFieldKey),
        matching: find.text(text)),
    findsOneWidget,
  );
}
