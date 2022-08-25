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

import 'dart:async';

import 'package:collection/collection.dart';
import 'package:flutter/material.dart';
import 'package:playground/constants/params.dart';
import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/examples/repositories/example_repository.dart';
import 'package:playground/modules/examples/repositories/models/get_snippet_request.dart';
import 'package:playground/modules/examples/repositories/models/get_snippet_response.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/examples/repositories/models/save_snippet_request.dart';
import 'package:playground/modules/examples/repositories/models/shared_file_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class ExampleState with ChangeNotifier {
  final ExampleRepository _exampleRepository;
  Map<SDK, List<CategoryModel>>? sdkCategories;
  Map<SDK, ExampleModel> defaultExamplesMap = {};
  ExampleModel? defaultExample;
  bool isSelectorOpened = false;

  final _allExamplesCompleter = Completer<void>();

  Future<void> get allExamplesFuture => _allExamplesCompleter.future;

  bool get hasExampleCatalog => !isEmbedded();

  ExampleState(this._exampleRepository);

  Future<void> init() async {
    if (hasExampleCatalog) {
      await Future.wait([
        _loadCategories(),
        loadDefaultExamplesIfNot(),
      ]);
    }
  }

  void setSdkCategories(Map<SDK, List<CategoryModel>> map) {
    sdkCategories = map;
    _allExamplesCompleter.complete();
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

  Future<ExampleModel> loadSharedExample(String id) async {
    GetSnippetResponse result = await _exampleRepository.getSnippet(
      GetSnippetRequestWrapper(id: id),
    );
    return ExampleModel(
      sdk: result.sdk,
      name: result.files.first.name,
      path: id,
      description: '',
      type: ExampleType.example,
      source: result.files.first.code,
      pipelineOptions: result.pipelineOptions,
    );
  }

  Future<String> getSnippetId({
    required List<SharedFile> files,
    required SDK sdk,
    required String pipelineOptions,
  }) async {
    String id = await _exampleRepository.saveSnippet(SaveSnippetRequestWrapper(
      files: files,
      sdk: sdk,
      pipelineOptions: pipelineOptions,
    ));
    return id;
  }

  Future<ExampleModel> loadExampleInfo(ExampleModel example) async {
    if (example.isInfoFetched()) {
      return example;
    }

    //GRPC GetPrecompiledGraph errors hotfix
    if (example.name == 'MinimalWordCount' &&
        (example.sdk == SDK.go || example.sdk == SDK.scio)) {
      final exampleData = await Future.wait([
        getExampleSource(example.path, example.sdk),
        getExampleOutput(example.path, example.sdk),
        getExampleLogs(example.path, example.sdk),
      ]);
      example.setSource(exampleData[0]);
      example.setOutputs(exampleData[1]);
      example.setLogs(exampleData[2]);
      return example;
    }

    final exampleData = await Future.wait([
      getExampleSource(example.path, example.sdk),
      getExampleOutput(example.path, example.sdk),
      getExampleLogs(example.path, example.sdk),
      getExampleGraph(example.path, example.sdk)
    ]);
    example.setSource(exampleData[0]);
    example.setOutputs(exampleData[1]);
    example.setLogs(exampleData[2]);
    example.setGraph(exampleData[3]);
    return example;
  }

  Future<void> _loadCategories() {
    return _exampleRepository
        .getListOfExamples(
          GetListOfExamplesRequestWrapper(sdk: null, category: null),
        )
        .then((map) => setSdkCategories(map));
  }

  void changeSelectorVisibility() {
    isSelectorOpened = !isSelectorOpened;
    notifyListeners();
  }

  Future<void> loadDefaultExamples() async {
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
    final futures = <Future<void>>[];

    for (var entry in defaultExamplesMap.entries) {
      final exampleFuture = loadExampleInfo(entry.value)
          .then((value) => defaultExamplesMap[entry.key] = value);
      futures.add(exampleFuture);
    }
    notifyListeners();

    await Future.wait(futures);
  }

  Future<void> loadDefaultExamplesIfNot() async {
    if (defaultExamplesMap.isNotEmpty) {
      return;
    }

    await loadDefaultExamples();
  }

  Future<ExampleModel?> getCatalogExampleByPath(String path) async {
    await allExamplesFuture;

    final allExamples = sdkCategories?.values
        .expand((sdkCategory) => sdkCategory.map((e) => e.examples))
        .expand((element) => element);

    return allExamples?.firstWhereOrNull(
      (e) => e.path == path,
    );
  }
}
