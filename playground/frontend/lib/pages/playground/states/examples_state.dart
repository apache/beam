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
import 'package:playground/modules/examples/repositories/example_repository.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class ExampleState with ChangeNotifier {
  final ExampleRepository _exampleRepository;
  Map<SDK, List<CategoryModel>>? sdkCategories;
  Map<SDK, ExampleModel>? defaultExamplesMap;
  bool isSelectorOpened = false;

  ExampleState(this._exampleRepository);

  init() {
    _loadCategories();
  }

  List<CategoryModel>? getCategories(SDK sdk) {
    return sdkCategories?[sdk] ?? [];
  }

  Future<String> getExampleOutput(String id, SDK sdk) async {
    return await _exampleRepository.getExampleOutput(
      GetExampleRequestWrapper(id, sdk),
    );
  }

  Future<String> getExampleSource(String id, SDK sdk) async {
    return await _exampleRepository.getExampleSource(
      GetExampleRequestWrapper(id, sdk),
    );
  }

  Future<String> getExampleLogs(String id, SDK sdk) async {
    return await _exampleRepository.getExampleLogs(
      GetExampleRequestWrapper(id, sdk),
    );
  }

  Future<ExampleModel> loadExampleInfo(ExampleModel example, SDK sdk) async {
    if (example.isInfoFetched()) {
      return example;
    }
    final exampleData = await Future.wait([
      getExampleSource(example.path, sdk),
      getExampleOutput(example.path, sdk),
      getExampleLogs(example.path, sdk)
    ]);
    example.setSource(exampleData[0]);
    example.setOutputs(exampleData[1]);
    example.setLogs(exampleData[2]);
    return example;
  }

  _loadCategories() async {
    sdkCategories = await _exampleRepository.getListOfExamples(
      GetListOfExamplesRequestWrapper(sdk: null, category: null),
    );
    await _loadDefaultExamples(sdkCategories);
    notifyListeners();
  }

  changeSelectorVisibility() {
    isSelectorOpened = !isSelectorOpened;
    notifyListeners();
  }

  _loadDefaultExamples(sdkCategories) async {
    defaultExamplesMap = {};
    List<MapEntry<SDK, ExampleModel>> entries = [];
    for (SDK sdk in SDK.values) {
      ExampleModel? defaultExample = sdkCategories![sdk]?.first.examples.first;
      if (defaultExample != null) {
        // load source and output async
        loadExampleInfo(defaultExample, sdk);
        entries.add(MapEntry(sdk, defaultExample));
      }
    }
    defaultExamplesMap?.addEntries(entries);
    notifyListeners();
  }
}
