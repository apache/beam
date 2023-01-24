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
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';
import 'package:provider/provider.dart';

import 'common/common.dart';
import 'common/common_finders.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('Example selector test', (WidgetTester wt) async {
    await init(wt);
    await _checkFilteringExamplesByTypes(wt);
    await _checkFilteringExamplesByTags(wt);
    await _checkFilteringExamplesBySearchString(wt);
    await _checkViewDescription(wt);
  });
}

Future<void> _checkFilteringExamplesByTypes(WidgetTester wt) async {
  await wt.tapAndSettle(find.exampleSelector());
  final allExamplesCount = _getExamplesCount(wt);
  await wt.tapAndSettle(find.widgetWithText(TypeBubble, ExampleType.test.name));
  
  final filteredExamplesCount = _getExamplesCount(wt);

  expect(allExamplesCount, isNot(filteredExamplesCount));

  final categoriesWithExamples = _getCategoriesWithExamples(wt);
  for (final example in categoriesWithExamples.expand((e) => e.examples)) {
    expect(example.type, ExampleType.test);
  }

  await wt.tapAndSettle(find.exampleSelector());
}

Future<void> _checkFilteringExamplesByTags(WidgetTester wt) async {
  await wt.tapAndSettle(find.exampleSelector());

  final allExamplesCount = _getExamplesCount(wt);
  final sortedTags = _getSortedTags(wt);
  await wt.tapAndSettle(find.widgetWithText(TagBubble, sortedTags[0]));
  final filteredExamplesCount = _getExamplesCount(wt);

  expect(_areCategoriesContainsTag(wt, [sortedTags[0]]), isTrue);
  expect(allExamplesCount, isNot(filteredExamplesCount));

  await wt.tapAndSettle(find.widgetWithText(TagBubble, sortedTags[1]));
  final nextFilteredExamplesCount = _getExamplesCount(wt);

  expect(_areCategoriesContainsTag(wt, [sortedTags[0], sortedTags[1]]), isTrue);
  expect(filteredExamplesCount, isNot(nextFilteredExamplesCount));

  await wt.tapAndSettle(find.exampleSelector());
}

List<String> _getSortedTags(WidgetTester wt) {
  final categoriesWithExamples = _getCategoriesWithExamples(wt);
  final tags = categoriesWithExamples
      .expand((e) => e.examples)
      .expand((e) => e.tags);
  final tagsMap = <String, int>{};
  for (final tag in tags) {
    tagsMap[tag] = tagsMap[tag] == null ? 1 : tagsMap[tag]! + 1;
  }
  final tagsMapList = tagsMap.entries.toList()..sort((a, b) => b.value.compareTo(a.value));
  return tagsMapList.map((e) => e.key).toList();
}

bool _areCategoriesContainsTag(WidgetTester wt, List<String> tags) {
  final categoriesWithExamples = _getCategoriesWithExamples(wt);
  final examples = categoriesWithExamples.expand((e) => e.examples);
  
  if (examples.isEmpty) {
    return true;
  }
  
  for (final example in examples) {
    for (final tag in tags) {
      if (!example.tags.contains(tag)) {
        return false;
      }
    }
  }
  return true;
}

int _getExamplesCount(WidgetTester wt) {
  final categories = _getCategoriesWithExamples(wt);

  if (categories.isEmpty) {
    return 0;
  }

  return categories
      .map((e) => e.examples.length)
      .reduce((value, element) => value + element);
}

List<CategoryWithExamples> _getCategoriesWithExamples(WidgetTester wt) {
  return wt
      .element(find.byType(ExamplesDropdownContent))
      .read<ExampleSelectorState>()
      .categories;
}

Future<void> _checkFilteringExamplesBySearchString(WidgetTester wt) async {
  await wt.tapAndSettle(find.exampleSelector());
  final allExamplesCount = _getExamplesCount(wt);

  await wt.enterText(find.byType(SearchField), 'te');

  final filteredExamplesCount = _getExamplesCount(wt);

  expect(allExamplesCount != filteredExamplesCount, true);

  final categories = _getCategoriesWithExamples(wt);
  for (var category in categories) {
    for (var example in category.examples) {
      expect(example.name.toLowerCase(), contains('te'));
    }
  }

  await wt.enterText(find.byType(SearchField), '');

  expect(_getExamplesCount(wt), allExamplesCount);

  await wt.tapAndSettle(find.exampleSelector());
}

Future<void> _checkViewDescription(WidgetTester wt) async {
  await wt.tapAndSettle(find.exampleSelector());

  expect(find.descriptionPopover(), findsNothing);

  await wt.tapAndSettle(
    find
        .descendant(
          of: find.byType(ExampleItemActions),
          matching: find.descriptionPopoverButton(),
        )
        .first,
  );

  expect(find.descriptionPopover(), findsOneWidget);

  await wt.tapAndSettle(find.exampleSelector());
}
