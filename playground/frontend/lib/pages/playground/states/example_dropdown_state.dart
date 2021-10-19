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

import 'examples_state.dart';

class ExampleDropdownState with ChangeNotifier {
  final ExampleState _exampleState;
  ExampleType _selectedFilterType;
  String _filterText;
  List<CategoryModel> categories;

  ExampleDropdownState(
    this._exampleState,
    this.categories, [
    this._selectedFilterType = ExampleType.all,
    this._filterText = '',
  ]);

  ExampleType get selectedFilterType => _selectedFilterType;

  String get filterText => _filterText;

  setSelectedFilterType(ExampleType type) {
    _selectedFilterType = type;
    notifyListeners();
  }

  setFilterText(String text) {
    _filterText = text;
    notifyListeners();
  }

  setCategories(List<CategoryModel>? categories) {
    this.categories = categories ?? [];
    notifyListeners();
  }

  sortCategories() {
    List<CategoryModel>? sorted;
    if (selectedFilterType == ExampleType.all && filterText == '') {
      sorted = _exampleState.categories!;
    } else if (selectedFilterType != ExampleType.all && filterText == '') {
      sorted = sortExamplesByType(
        _exampleState.categories!,
        selectedFilterType,
      );
    } else if (selectedFilterType == ExampleType.all && filterText != '') {
      sorted = sortExamplesByName(_exampleState.categories!, filterText);
    } else {
      sorted = sortExamplesByType(
        _exampleState.categories!,
        selectedFilterType,
      );
      sorted = sortExamplesByName(sorted, filterText);
    }
    setCategories(sorted);
  }

  List<CategoryModel> sortExamplesByType(
      List<CategoryModel> unsorted, ExampleType type) {
    List<CategoryModel> sorted = [];
    for (CategoryModel category in unsorted) {
      if (category.examples.any((element) => element.type == type)) {
        CategoryModel sortedCategory = CategoryModel(
          category.name,
          category.examples.where((element) => element.type == type).toList(),
        );
        sorted.add(sortedCategory);
      }
    }
    return sorted;
  }

  List<CategoryModel> sortExamplesByName(
      List<CategoryModel> unsorted, String name) {
    List<CategoryModel> sorted = [];
    for (CategoryModel category in unsorted) {
      if (category.examples.any(
        (element) => element.name.toLowerCase().contains(name.toLowerCase()),
      )) {
        CategoryModel sortedCategory = CategoryModel(
          category.name,
          category.examples
              .where((element) =>
                  element.name.toLowerCase().contains(name.toLowerCase()))
              .toList(),
        );
        sorted.add(sortedCategory);
      }
    }
    return sorted;
  }
}
