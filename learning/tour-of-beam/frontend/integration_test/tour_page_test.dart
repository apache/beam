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

// ignore_for_file: avoid_print

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:get_it/get_it.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';
import 'package:tour_of_beam/cache/content_tree.dart';
import 'package:tour_of_beam/cache/sdk.dart';
import 'package:tour_of_beam/components/builders/content_tree.dart';
import 'package:tour_of_beam/models/module.dart';
import 'package:tour_of_beam/models/parent_node.dart';
import 'package:tour_of_beam/models/unit.dart';
import 'package:tour_of_beam/pages/tour/screen.dart';
import 'package:tour_of_beam/pages/tour/state.dart';
import 'package:tour_of_beam/pages/tour/widgets/playground.dart';
import 'package:tour_of_beam/pages/tour/widgets/unit.dart';
import 'package:tour_of_beam/pages/tour/widgets/unit_content.dart';

import 'common/common.dart';
import 'common/common_finders.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  testWidgets(
    'ToB miscellaneous ui',
    (wt) async {
      await init(wt);

      final sdkCache = GetIt.instance.get<SdkCache>();
      final sdks = sdkCache.getSdks();
      await wt.tapAndSettle(find.text(sdks.first.title));
      await wt.tapAndSettle(find.startTourButton());

      await _checkContentTreeBuildsProperly(wt);
      await _checkHighlightsSelectedUnit(wt);
      await _checkRunCodeWorks(wt);
      await _checkResizeUnitContent(wt);

      expect(
        ExamplesLoader.failedToLoadExamples,
        isEmpty,
        reason: 'Failed to load some examples.',
      );
    },
  );
}

Future<void> _checkContentTreeBuildsProperly(WidgetTester wt) async {
  final modules = _getModules(wt);

  for (final module in modules) {
    await _checkParent(module, wt);
  }
}

List<ModuleModel> _getModules(WidgetTester wt) {
  final contentTreeCache = GetIt.instance.get<ContentTreeCache>();
  final controller = getContentTreeController(wt);
  final contentTree = contentTreeCache.getContentTree(controller.sdk);
  return contentTree?.nodes ?? (throw Exception('Cannot load modules'));
}

Future<void> _checkParent(ParentNodeModel node, WidgetTester wt) async {
  if (!_getExpandedIds(wt).contains(node.id)) {
    await wt.ensureVisible(find.byKey(Key(node.id)));
    await wt.tapAndSettle(find.byKey(Key(node.id)));
  }

  for (final child in node.nodes) {
    if (child is UnitModel) {
      await _checkNode(child, wt);
    }
    if (child is ParentNodeModel) {
      await _checkParent(child, wt);
    }
  }
}

Future<void> _checkNode(UnitModel node, WidgetTester wt) async {
  final finder = find.byKey(Key(node.id));
  expect(finder, findsOneWidget, reason: node.id);

  await wt.ensureVisible(finder);
  expect(
    find.descendant(
      of: find.byType(ContentTreeBuilder),
      matching: find.text(node.title),
    ),
    findsAtLeastNWidgets(1),
  );

  await _checkUnitContentLoadsProperly(node, wt);
}

Future<void> _checkUnitContentLoadsProperly(
  UnitModel unit,
  WidgetTester wt,
) async {
  await wt.tapAndSettle(find.byKey(Key(unit.id)));

  final hasSnippet = _getTourNotifier(wt).isUnitContainsSnippet;

  expect(
    find.byType(PlaygroundWidget),
    hasSnippet ? findsOneWidget : findsNothing,
  );

  expect(
    find.descendant(
      of: find.byType(UnitContentWidget),
      matching: find.text(unit.title),
    ),
    findsAtLeastNWidgets(1),
  );
}

Future<void> _checkHighlightsSelectedUnit(WidgetTester wt) async {
  final controller = getContentTreeController(wt);
  final selectedUnit = controller.currentNode;

  if (selectedUnit == null) {
    fail('No unit selected');
  }

  final selectedUnitText = find.descendant(
    of: find.byKey(Key(selectedUnit.id)),
    matching: find.text(selectedUnit.title),
    skipOffstage: false,
  );

  final selectedUnitContainer = find.ancestor(
    of: selectedUnitText,
    matching: find.byKey(UnitWidget.containerKey),
  );

  final context = wt.element(selectedUnitText);

  expect(
    (wt.widget<Container>(selectedUnitContainer).decoration as BoxDecoration?)
        ?.color,
    Theme.of(context).selectedRowColor,
  );
}

Future<void> _checkRunCodeWorks(WidgetTester wt) async {
  const text = 'OK';
  const code = '''
public class MyClass {
  public static void main(String[] args) {
    System.out.print("$text");
  }
}
''';

  await _selectExampleWithSnippet(wt);
  await wt.pumpAndSettle();

  await wt.enterText(find.snippetCodeField(), code);
  await wt.pumpAndSettle();

  await _runAndCancelExample(wt, const Duration(milliseconds: 300));

  await wt.tapAndSettle(find.runOrCancelButton());

  final playgroundController = _getPlaygroundController(wt);
  expect(
    playgroundController.codeRunner.resultLogOutput,
    contains(text),
  );
}

Future<void> _runAndCancelExample(WidgetTester wt, Duration duration) async {
  await wt.tap(find.runOrCancelButton());

  await wt.pumpAndSettleNoException(timeout: duration);
  await wt.tapAndSettle(find.runOrCancelButton());

  final playgroundController = _getPlaygroundController(wt);
  expect(
    playgroundController.codeRunner.resultLogOutput,
    contains('Pipeline cancelled'),
  );
}

Future<void> _checkResizeUnitContent(WidgetTester wt) async {
  var dragHandleFinder = find.byKey(TourScreen.dragHandleKey);

  final startHandlePosition = wt.getCenter(dragHandleFinder);

  await wt.drag(dragHandleFinder, const Offset(100, 0));
  await wt.pumpAndSettle();

  dragHandleFinder = find.byKey(TourScreen.dragHandleKey);

  final movedHandlePosition = wt.getCenter(dragHandleFinder);

  expectSimilar(startHandlePosition.dx, movedHandlePosition.dx - 100);
}

Future<void> _selectExampleWithSnippet(WidgetTester wt) async {
  final tourNotifier = _getTourNotifier(wt);
  final modules = _getModules(wt);

  for (final module in modules) {
    for (final node in module.nodes) {
      if (node is UnitModel) {
        await _checkNode(node, wt);
        if (tourNotifier.isUnitContainsSnippet) {
          return;
        }
      }
    }
  }
}

TourNotifier _getTourNotifier(WidgetTester wt) {
  return (wt.widget(find.byType(UnitContentWidget)) as UnitContentWidget)
      .tourNotifier;
}

PlaygroundController _getPlaygroundController(WidgetTester wt) {
  return _getTourNotifier(wt).playgroundController;
}

Set<String> _getExpandedIds(WidgetTester wt) {
  final controller = getContentTreeController(wt);
  return controller.expandedIds;
}
