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
import 'package:playground_components/playground_components.dart';

class ExampleSelectorState with ChangeNotifier {
  final PlaygroundController _playgroundController;
  ExampleType _selectedFilterType;
  String _searchText;
  List<CategoryWithExamples> categories;
  List<String> tags = [];
  List<String> selectedTags = [];

  ExampleSelectorState(
    this._playgroundController,
    this.categories, [
    this._selectedFilterType = ExampleType.all,
    this._searchText = '',
  ]) {
    tags = _getTagsSortedByPopularity(categories);
  }

  ExampleType get selectedFilterType => _selectedFilterType;

  String get searchText => _searchText;

  void setSelectedFilterType(ExampleType type) {
    _selectedFilterType = type;
    notifyListeners();
  }

  void addSelectedTag(String tag) {
    selectedTags.add(tag);
    notifyListeners();
  }

  void removeSelectedTag(String tag) {
    selectedTags.remove(tag);
    notifyListeners();
  }

  List<String> _getTagsSortedByPopularity(
    List<CategoryWithExamples> categories,
  ) {
    Map<String, int> tagsPopularity = {};
    for (var category in categories) {
      for (var example in category.examples) {
        for (var tag in example.tags) {
          if (tagsPopularity[tag] != null) {
            tagsPopularity[tag] = tagsPopularity[tag]! + 1;
          } else {
            tagsPopularity[tag] = 1;
          }
        }
      }
    }
    final tagEntries = tagsPopularity.entries.toList()
      ..sort((entry1, entry2) => entry2.value.compareTo(entry1.value));
    return tagEntries.map((entry) => entry.key).toList();
  }

  void setSearchText(String text) {
    _searchText = text;
    notifyListeners();
  }

  void setCategories(List<CategoryWithExamples> categories) {
    this.categories = categories;
    notifyListeners();
  }

  void filterCategoriesWithExamples() {
    final categories = _playgroundController.exampleCache.getCategories(
      _playgroundController.sdk,
    );
    final filteredCategories = categories
        .map((category) => CategoryWithExamples(
            title: category.title,
            examples: _filterExamples(category.examples)))
        .where((category) => category.examples.isNotEmpty)
        .toList();
    setCategories(filteredCategories);
  }

  List<ExampleBase> _filterExamples(List<ExampleBase> examples) {
    final byType = filterExamplesByType(examples, selectedFilterType);
    final byTags = filterExamplesByTags(byType);
    final byName = filterExamplesByName(byTags);
    return byName;
  }

  List<ExampleBase> filterExamplesByTags(List<ExampleBase> examples) {
    if (selectedTags.isEmpty) {
      return examples;
    }
    List<ExampleBase> sorted = [];
    for (var example in examples) {
      if (example.tags.toSet().containsAll(selectedTags)) {
        sorted.add(example);
      }
    }
    return sorted;
  }

  List<ExampleBase> filterExamplesByType(
    List<ExampleBase> examples,
    ExampleType type,
  ) {
    if (type == ExampleType.all) {
      return examples;
    }
    return examples.where((element) => element.type == type).toList();
  }

  List<ExampleBase> filterExamplesByName(List<ExampleBase> examples) {
    if (searchText.isEmpty) {
      return examples;
    }
    return examples
        .where((example) =>
            example.name.toLowerCase().contains(searchText.toLowerCase()))
        .toList();
  }
}
