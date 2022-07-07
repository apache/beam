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
import 'package:playground/constants/params.dart';
import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/examples/repositories/example_repository.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class ExampleState with ChangeNotifier {
  final ExampleRepository _exampleRepository;
  Map<SDK, List<CategoryModel>>? sdkCategories;
  Map<SDK, ExampleModel> defaultExamplesMap = {};
  ExampleModel? defaultExample;
  bool isSelectorOpened = false;

  ExampleState(this._exampleRepository);

  init() {
    if (!Uri.base.toString().contains(kIsEmbedded)) {
      _loadCategories();
    }
  }

  setSdkCategories(Map<SDK, List<CategoryModel>> map) {
    sdkCategories = map;
    notifyListeners();
  }

  List<CategoryModel>? getCategories(SDK sdk) {
    return sdkCategories?[sdk] ?? [];
  }

  Future<String> getExampleOutput(String id, SDK sdk) async {
    return _exampleRepository.getExampleOutput(
      GetExampleRequestWrapper(id, sdk),
    );
  }

  Future<String> getExampleSource(String id, SDK sdk) async {
    return _exampleRepository.getExampleSource(
      GetExampleRequestWrapper(id, sdk),
    );
  }

  Future<ExampleModel> getExample(String path, SDK sdk) async {
    return _exampleRepository.getExample(
      GetExampleRequestWrapper(path, sdk),
    );
  }

  Future<String> getExampleLogs(String id, SDK sdk) async {
    return _exampleRepository.getExampleLogs(
      GetExampleRequestWrapper(id, sdk),
    );
  }

  Future<String> getExampleGraph(String id, SDK sdk) async {
    return _exampleRepository.getExampleGraph(
      GetExampleRequestWrapper(id, sdk),
    );
  }

  Future<ExampleModel> loadExampleInfo(ExampleModel example, SDK sdk) async {
    if (example.isInfoFetched()) {
      return example;
    }

    //GRPC GetPrecompiledGraph errors hotfix
    if (example.name == 'MinimalWordCount' &&
        (sdk == SDK.go || sdk == SDK.scio)) {
      final exampleData = await Future.wait([
        getExampleSource(example.path, sdk),
        getExampleOutput(example.path, sdk),
        getExampleLogs(example.path, sdk),
      ]);
      example.setSource(exampleData[0]);
      example.setOutputs(exampleData[1]);
      example.setLogs(exampleData[2]);
      return example;
    }

    final exampleData = await Future.wait([
      getExampleSource(example.path, sdk),
      getExampleOutput(example.path, sdk),
      getExampleLogs(example.path, sdk),
      getExampleGraph(example.path, sdk)
    ]);
    example.setSource(exampleData[0]);
    example.setOutputs(exampleData[1]);
    example.setLogs(exampleData[2]);
    example.setGraph(exampleData[3]);
    return example;
  }

  _loadCategories() {
    _exampleRepository
        .getListOfExamples(
          GetListOfExamplesRequestWrapper(sdk: null, category: null),
        )
        .then((map) => setSdkCategories(map));
  }

  changeSelectorVisibility() {
    isSelectorOpened = !isSelectorOpened;
    notifyListeners();
  }

  loadDefaultExamples() async {
    if (defaultExamplesMap.isNotEmpty) {
      return;
    }

    List<MapEntry<SDK, ExampleModel>> defaultExamples = [];

    for (var value in SDK.values) {
      defaultExamples.add(
        MapEntry(
          value,
          await _exampleRepository.getDefaultExample(
            // First parameter is an empty string, because we don't need path to get the default example.
            GetExampleRequestWrapper('', value),
          ),
        ),
      );
    }

    defaultExamplesMap.addEntries(defaultExamples);
    for (var entry in defaultExamplesMap.entries) {
      loadExampleInfo(entry.value, entry.key)
          .then((value) => defaultExamplesMap[entry.key] = value);
    }
    notifyListeners();
  }
}
