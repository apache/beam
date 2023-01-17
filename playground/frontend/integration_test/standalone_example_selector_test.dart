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
import 'package:playground/modules/examples/components/example_list/example_item_actions.dart';
import 'package:playground/modules/examples/components/filter/tag_bubble.dart';
import 'package:playground/modules/examples/components/filter/type_bubble.dart';
import 'package:playground/modules/examples/components/search_field/search_field.dart';
import 'package:playground/modules/examples/examples_dropdown_content.dart';
import 'package:playground/pages/standalone_playground/notifiers/example_selector_state.dart';
import 'package:playground_components_dev/playground_components_dev.dart';
import 'package:provider/provider.dart';

import 'common/common.dart';
import 'common/common_finders.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('Example selector test', (WidgetTester wt) async {
    await init(wt);
    await _checkFilteringExamplesByTags(wt);
    await _checkFilteringExamplesBySearchString(wt);
    await _checkViewDescription(wt);
  });
}

Future<void> _checkFilteringExamplesByTags(WidgetTester wt) async {
  await wt.tapAndSettle(find.exampleSelector());
  var allExamplesCount = _getExamplesCount(wt);
  await wt.tapAndSettle(find.byType(TypeBubble).last);
  var filteredExamplesCount = _getExamplesCount(wt);

  expect(allExamplesCount != filteredExamplesCount, true);

  await wt.tapAndSettle(find.exampleSelector());
  await wt.tapAndSettle(find.exampleSelector());

  allExamplesCount = _getExamplesCount(wt);
  await wt.tapAndSettle(find.byType(TagBubble).first);
  filteredExamplesCount = _getExamplesCount(wt);

  expect(allExamplesCount != filteredExamplesCount, true);

  await wt.tapAndSettle(find.byType(TagBubble).at(1));
  final nextFilteredExamplesCount = _getExamplesCount(wt);

  expect(filteredExamplesCount != nextFilteredExamplesCount, true);

  await wt.tapAndSettle(find.exampleSelector());
}

int _getExamplesCount(WidgetTester wt) {
  final state = wt
      .element(find.byType(ExamplesDropdownContent))
      .read<ExampleSelectorState>();

  if (state.categories.isEmpty) {
    return 0;
  }

  return state.categories
      .map((e) => e.examples.length)
      .reduce((value, element) => value + element);
}

Future<void> _checkFilteringExamplesBySearchString(WidgetTester wt) async {
  await wt.tapAndSettle(find.exampleSelector());
  final allExamplesCount = _getExamplesCount(wt);

  await wt.enterText(find.byType(SearchField), 'te');

  final filteredExamplesCount = _getExamplesCount(wt);

  expect(allExamplesCount != filteredExamplesCount, true);

  await wt.enterText(find.byType(SearchField), '');

  expect(_getExamplesCount(wt), allExamplesCount);

  await wt.tapAndSettle(find.exampleSelector());
}

Future<void> _checkViewDescription(WidgetTester wt) async {
  await wt.tapAndSettle(find.exampleSelector());

  expect(find.descriptionPopover(), findsNothing);

  await wt.tapAndSettle(
    find.descendant(
      of: find.byType(ExampleItemActions),
      matching: find.descriptionPopoverButton(),
    ).first,
  );

  expect(find.descriptionPopover(), findsOneWidget);

  await wt.tapAndSettle(find.exampleSelector());
}
