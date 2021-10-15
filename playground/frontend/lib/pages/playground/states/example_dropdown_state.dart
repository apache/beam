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
  ExampleType _selectedCategory;
  List<CategoryModel> categories;

  ExampleDropdownState(
    this._exampleState,
    this.categories, [
    this._selectedCategory = ExampleType.all,
  ]);

  ExampleType get selectedCategory => _selectedCategory;

  setSelectedCategory(ExampleType type) async {
    _selectedCategory = type;
    notifyListeners();
  }

  setCategories(List<CategoryModel>? categories) {
    this.categories = categories ?? [];
    notifyListeners();
  }

  sortExamplesByType(ExampleType type) {
    if (type == ExampleType.all) {
      setCategories(_exampleState.categories!);
    } else {
      List<CategoryModel> unsorted = [..._exampleState.categories!];
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
      setCategories(sorted);
    }
  }

  sortExamplesByName(String name) {
    if (name.isEmpty) {
      setCategories(_exampleState.categories!);
    } else {
      List<CategoryModel> unsorted = [..._exampleState.categories!];
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
      setCategories(sorted);
    }
  }
}
