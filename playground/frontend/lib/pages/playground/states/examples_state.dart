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
import 'package:playground/modules/examples/models/outputs_model.dart';
import 'package:playground/modules/examples/repositories/example_repository.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class ExampleState with ChangeNotifier {
  final ExampleRepository _exampleRepository;
  Map<SDK, List<CategoryModel>>? categoriesBySdkMap;
  Map<SDK, ExampleModel>? defaultExamples;
  bool isSelectorOpened = false;

  ExampleState(this._exampleRepository) {
    _loadCategories();
    _loadDefaultExamples();
  }

  List<CategoryModel>? getCategories(SDK sdk) {
    return categoriesBySdkMap?[sdk] ?? [];
  }

  Future<OutputsModel> getPrecompiledOutputs(int id) async {
    OutputsModel outputs = await _exampleRepository.getPrecompiledOutputs(id);
    return outputs;
  }

  Future<String> getSource(int id) async {
    String source = await _exampleRepository.getExampleSource(id);
    return source;
  }

  _loadCategories() async {
    categoriesBySdkMap = await _exampleRepository.getListOfExamples();
    notifyListeners();
  }

  _loadDefaultExamples() async {
    defaultExamples = await _exampleRepository.getDefaultExamples();
    notifyListeners();
  }

  changeSelectorVisibility() {
    isSelectorOpened = !isSelectorOpened;
    notifyListeners();
  }
}
