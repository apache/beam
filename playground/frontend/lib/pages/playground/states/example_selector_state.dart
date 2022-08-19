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
import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/pages/playground/states/playground_state.dart';

class ExampleSelectorState with ChangeNotifier {
  final PlaygroundState _playgroundState;
  ExampleType _selectedFilterType;
  String _filterText;
  List<CategoryModel> categories;

  ExampleSelectorState(
    this._playgroundState,
    this.categories, [
    this._selectedFilterType = ExampleType.all,
    this._filterText = '',
  ]);

  ExampleType get selectedFilterType => _selectedFilterType;

  String get filterText => _filterText;

  void setSelectedFilterType(ExampleType type) {
    _selectedFilterType = type;
    notifyListeners();
  }

  void setFilterText(String text) {
    _filterText = text;
    notifyListeners();
  }

  void setCategories(List<CategoryModel>? categories) {
    this.categories = categories ?? [];
    notifyListeners();
  }

  void sortCategories() {
    final categories = _playgroundState.exampleState.getCategories(
      _playgroundState.sdk,
    )!;

    final sortedCategories = categories
        .map((category) => CategoryModel(
            name: category.name,
            examples: _sortCategoryExamples(category.examples)))
        .where((category) => category.examples.isNotEmpty)
        .toList();
    setCategories(sortedCategories);
  }

  List<ExampleModel> _sortCategoryExamples(List<ExampleModel> examples) {
    final isAllFilterType = selectedFilterType == ExampleType.all;
    final isFilterTextEmpty = filterText.isEmpty;
    if (isAllFilterType && isFilterTextEmpty) {
      return examples;
    }
    if (!isAllFilterType && isFilterTextEmpty) {
      return sortExamplesByType(
        examples,
        selectedFilterType,
      );
    }
    if (isAllFilterType && !isFilterTextEmpty) {
      return sortExamplesByName(examples, filterText);
    }
    final sorted = sortExamplesByType(
      examples,
      selectedFilterType,
    );
    return sortExamplesByName(sorted, filterText);
  }

  List<ExampleModel> sortExamplesByType(
    List<ExampleModel> examples,
    ExampleType type,
  ) {
    return examples.where((element) => element.type == type).toList();
  }

  List<ExampleModel> sortExamplesByName(
    List<ExampleModel> examples,
    String name,
  ) {
    return examples
        .where((example) =>
            example.name.toLowerCase().contains(name.toLowerCase()))
        .toList();
  }
}
