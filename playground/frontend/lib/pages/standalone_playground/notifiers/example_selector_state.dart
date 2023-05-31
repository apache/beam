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
  late final Map<String, int> tagsFrequencyMap;
  List<String> selectedTags = [];

  ExampleSelectorState(
    this._playgroundController,
    this.categories, [
    this._selectedFilterType = ExampleType.all,
    this._searchText = '',
  ]) {
    tagsFrequencyMap = _buildTagsFrequencyMap(categories);
    tags = _getTagsSortedByExampleCount(categories);
  }

  ExampleType get selectedFilterType => _selectedFilterType;

  String get searchText => _searchText;

  void setSelectedFilterType(ExampleType type) {
    _selectedFilterType = type;
    notifyListeners();
  }

  void addSelectedTag(String tag) {
    selectedTags.add(tag);
    tags.sort(_compareTags);
    notifyListeners();
  }

  void removeSelectedTag(String tag) {
    selectedTags.remove(tag);
    tags.sort(_compareTags);
    notifyListeners();
  }

  /// First selected, then most frequent, alphabetically for equal frequency.
  int _compareTags(String a, String b) {
    if (selectedTags.contains(a) && !selectedTags.contains(b)) {
      return -1;
    } else if (!selectedTags.contains(a) && selectedTags.contains(b)) {
      return 1;
    } else {
      final aFreq = tagsFrequencyMap[a] ?? -1;
      final bFreq = tagsFrequencyMap[b] ?? -1;
      if (aFreq > bFreq) {
        return -1;
      } else if (aFreq < bFreq) {
        return 1;
      } else {
        return a.compareTo(b);
      }
    }
  }

  List<String> _getTagsSortedByExampleCount(
    List<CategoryWithExamples> categories,
  ) {
    final tagEntries = tagsFrequencyMap.entries.toList()
      ..sort((entry1, entry2) => _compareTags(entry1.key, entry2.key));
    return tagEntries.map((entry) => entry.key).toList();
  }

  Map<String, int> _buildTagsFrequencyMap(
    List<CategoryWithExamples> categories,
  ) {
    final result = <String, int>{};
    for (final category in categories) {
      for (final example in category.examples) {
        for (final tag in example.tags) {
          result[tag] = (result[tag] ?? 0) + 1;
        }
      }
    }
    return result;
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
    final filteredCategories = _getAllCategories()
        .where((category) => category.examples.isNotEmpty)
        .toList();
    setCategories(filteredCategories);
  }

  Iterable<CategoryWithExamples> _getAllCategories() {
    final categories = _playgroundController.exampleCache.getCategories(
      _playgroundController.sdk,
    );
    return categories.map(
      (category) => CategoryWithExamples(
        title: category.title,
        examples: _filterExamples(category.examples),
      ),
    );
  }

  List<ExampleBase> _filterExamples(List<ExampleBase> examples) {
    final byType = filterExamplesByType(examples, selectedFilterType);
    final byTags = filterExamplesByTags(byType);
    final byName = filterExamplesByName(byTags);
    return byName;
  }

  @visibleForTesting
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

  @visibleForTesting
  List<ExampleBase> filterExamplesByType(
    List<ExampleBase> examples,
    ExampleType type,
  ) {
    if (type == ExampleType.all) {
      return examples;
    }
    return examples.where((element) => element.type == type).toList();
  }

  @visibleForTesting
  List<ExampleBase> filterExamplesByName(List<ExampleBase> examples) {
    if (_searchText.isEmpty) {
      return examples;
    }
    return examples
        .where((example) =>
            example.name.toLowerCase().contains(_searchText.toLowerCase()))
        .toList();
  }
}
