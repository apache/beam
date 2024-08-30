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

import 'package:collection/collection.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:get_it/get_it.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';
import 'package:tour_of_beam/cache/content_tree.dart';
import 'package:tour_of_beam/cache/sdk.dart';
import 'package:tour_of_beam/cache/unit_content.dart';
import 'package:tour_of_beam/components/builders/content_tree.dart';
import 'package:tour_of_beam/models/content_tree.dart';
import 'package:tour_of_beam/models/module.dart';
import 'package:tour_of_beam/models/parent_node.dart';
import 'package:tour_of_beam/models/unit.dart';
import 'package:tour_of_beam/pages/tour/screen.dart';
import 'package:tour_of_beam/pages/tour/state.dart';
import 'package:tour_of_beam/pages/tour/widgets/playground.dart';
import 'package:tour_of_beam/pages/tour/widgets/unit.dart';
import 'package:tour_of_beam/pages/tour/widgets/unit_content.dart';
import 'package:tour_of_beam/state.dart';

import 'common/common.dart';
import 'common/common_finders.dart';

const _sdk = Sdk.java;

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  testWidgets(
    'ToB miscellaneous ui',
    (wt) async {
      await init(wt);
      await wt.tapAndSettle(find.outlinedButtonWithText(_sdk.title));
      await wt.tapAndSettle(find.startTourButton());

      await _checkContentTreeBuildsProperly(wt);
      await _checkHighlightsSelectedUnit(wt);
      await _checkRunCodeWorks(wt);
      await _checkResizeUnitContent(wt);
      await _checkSdkChanges(wt);

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
  final contentTree = contentTreeCache.getContentTree(_sdk);
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

  final hasSnippet = _getTourNotifier(wt).isPlaygroundShown;

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

  await _selectUnitWithSnippet(wt);
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

Future<void> _selectUnitWithSnippet(WidgetTester wt) async {
  final tourNotifier = _getTourNotifier(wt);
  final modules = _getModules(wt);

  for (final module in modules) {
    for (final node in module.nodes) {
      if (node is UnitModel) {
        await _checkNode(node, wt);
        if (tourNotifier.isPlaygroundShown) {
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

Future<void> _checkSdkChanges(WidgetTester wt) async {
  await _selectUnitWithSnippetsInAllSdks(wt);
  await _checkSnippetChangesOnSdkChanging(wt);
}

Future<void> _selectUnitWithSnippetsInAllSdks(WidgetTester wt) async {
  final unitWithSnippets = await _findUnitWithSnippetsInAllSdks(wt);

  if (unitWithSnippets == null) {
    fail('No unit with snippets in all sdks');
  }

  final controller = getContentTreeController(wt);
  controller.onNodePressed(unitWithSnippets);
  await wt.pumpAndSettle();
}

Future<UnitModel?> _findUnitWithSnippetsInAllSdks(WidgetTester wt) async {
  final commonUnits = await _getCommonUnitsInAllSdks(wt);
  for (final unit in commonUnits) {
    if (await _hasSnippetsInAllSdks(unit)) {
      return unit;
    }
  }
  return null;
}

Future<bool> _hasSnippetsInAllSdks(UnitModel unit) async {
  final unitContentCache = GetIt.instance.get<UnitContentCache>();
  final sdks = GetIt.instance.get<SdkCache>().getSdks();
  for (final sdk in sdks) {
    final unitContent = await unitContentCache.getUnitContent(sdk.id, unit.id);
    if (unitContent.taskSnippetId == null) {
      return false;
    }
  }
  return true;
}

Future<Set<UnitModel>> _getCommonUnitsInAllSdks(WidgetTester wt) async {
  final contentTrees = await _loadAllContentTrees(wt);
  final sdkUnits = List<Set<UnitModel>>.empty(growable: true);
  for (final tree in contentTrees) {
    sdkUnits.add(tree.getUnits().toSet());
  }

  // Identifies and stores the common units across all lists within
  // the 'sdkUnits' list by iteratively removing elements from the first list
  // that don't exist in the subsequent lists.
  final commonUnitTitles = sdkUnits.first;
  for (final units in sdkUnits.skip(1)) {
    commonUnitTitles.removeWhere((u) => !units.contains(u));
  }

  return commonUnitTitles;
}

Future<List<ContentTreeModel>> _loadAllContentTrees(WidgetTester wt) async {
  final sdkCache = GetIt.instance.get<SdkCache>();
  final contentTreeCache = GetIt.instance.get<ContentTreeCache>();
  final sdks = sdkCache.getSdks();
  final nullableTrees = await Future.wait(
    sdks.map((sdk) async => contentTreeCache.getContentTree(sdk)),
  );

  return nullableTrees.whereNotNull().toList(growable: false);
}

Future<void> _checkSnippetChangesOnSdkChanging(WidgetTester wt) async {
  final defaultSdk = _getTourNotifier(wt).playgroundController.sdk;
  final sdkCache = GetIt.instance.get<SdkCache>();

  for (final sdk in sdkCache.getSdks()) {
    if (sdk == defaultSdk) {
      continue;
    }

    await _setSdk(sdk.title, wt);

    final selectedExample =
        _getTourNotifier(wt).playgroundController.selectedExample;
    final appNotifier = GetIt.instance.get<AppNotifier>();
    final actualSdk = selectedExample?.sdk;
    expect(actualSdk, appNotifier.sdk);
  }

  await _setSdk(defaultSdk!.title, wt);
}

Future<void> _setSdk(String title, WidgetTester wt) async {
  await wt.tapAndSettle(find.sdkDropdown());
  await wt.tapAndSettle(
    find.dropdownMenuItemWithText(title).first,
  );
}
