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

import '../models/category_with_examples.dart';
import '../models/example.dart';
import '../models/example_base.dart';
import '../models/sdk.dart';
import '../repositories/example_repository.dart';
import '../repositories/models/get_default_precompiled_object_request.dart';
import '../repositories/models/get_precompiled_object_request.dart';
import '../repositories/models/get_precompiled_objects_request.dart';
import '../repositories/models/get_snippet_request.dart';
import '../repositories/models/save_snippet_request.dart';
import '../repositories/models/shared_file.dart';

/// A runtime cache for examples fetched from a repository.
class ExampleCache extends ChangeNotifier {
  /// If set, then categories and default examples are enabled.
  /// Otherwise examples can only be queried by paths.
  final bool hasCatalog;

  final ExampleRepository _exampleRepository;
  final categoryListsBySdk = <Sdk, List<CategoryWithExamples>>{};

  final Map<Sdk, Example> defaultExamplesBySdk = {};

  // TODO(alexeyinkin): Extract, https://github.com/apache/beam/issues/23249
  bool isSelectorOpened = false;

  final _allExamplesCompleter = Completer<void>();

  Future<void> get allExamplesFuture => _allExamplesCompleter.future;

  ExampleCache({
    required ExampleRepository exampleRepository,
    required this.hasCatalog,
  }) : _exampleRepository = exampleRepository;

  Future<void> init() async {
    if (hasCatalog) {
      await Future.wait([
        _loadCategories(),
        loadDefaultExamplesIfNot(),
      ]);
    }
  }

  void setSdkCategories(Map<Sdk, List<CategoryWithExamples>> map) {
    categoryListsBySdk.addAll(map);
    _allExamplesCompleter.complete();
  }

  List<CategoryWithExamples> getCategories(Sdk? sdk) {
    return categoryListsBySdk[sdk] ?? [];
  }

  Future<String> getExampleOutput(String path, Sdk sdk) async {
    return _exampleRepository.getExampleOutput(
      GetPrecompiledObjectRequest(path: path, sdk: sdk),
    );
  }

  Future<String> getExampleSource(String path, Sdk sdk) async {
    return _exampleRepository.getExampleSource(
      GetPrecompiledObjectRequest(path: path, sdk: sdk),
    );
  }

  Future<ExampleBase> getExample(String path, Sdk sdk) async {
    return _exampleRepository.getExample(
      GetPrecompiledObjectRequest(path: path, sdk: sdk),
    );
  }

  Future<String> getExampleLogs(String path, Sdk sdk) async {
    return _exampleRepository.getExampleLogs(
      GetPrecompiledObjectRequest(path: path, sdk: sdk),
    );
  }

  Future<String> getExampleGraph(String id, Sdk sdk) async {
    return _exampleRepository.getExampleGraph(
      GetPrecompiledObjectRequest(path: id, sdk: sdk),
    );
  }

  Future<Example> loadSharedExample(String id) async {
    final result = await _exampleRepository.getSnippet(
      GetSnippetRequest(id: id),
    );

    return Example(
      sdk: result.sdk,
      name: result.files.first.name,
      path: id,
      description: '',
      type: ExampleType.example,
      source: result.files.first.code,
      pipelineOptions: result.pipelineOptions,
      complexity: result.complexity,
    );
  }

  Future<String> getSnippetId({
    required List<SharedFile> files,
    required Sdk sdk,
    required String pipelineOptions,
  }) async {
    final id = await _exampleRepository.saveSnippet(
      SaveSnippetRequest(
        files: files,
        sdk: sdk,
        pipelineOptions: pipelineOptions,
      ),
    );
    return id;
  }

  Future<Example> loadExampleInfo(ExampleBase example) async {
    if (example is Example) {
      return example;
    }

    //GRPC GetPrecompiledGraph errors hotfix
    if (example.name == 'MinimalWordCount' &&
        (example.sdk == Sdk.go || example.sdk == Sdk.scio)) {
      final exampleData = await Future.wait([
        getExampleSource(example.path, example.sdk),
        getExampleOutput(example.path, example.sdk),
        getExampleLogs(example.path, example.sdk),
      ]);

      return Example.fromBase(
        example,
        source: exampleData[0],
        outputs: exampleData[1],
        logs: exampleData[2],
      );
    }

    final exampleData = await Future.wait([
      getExampleSource(example.path, example.sdk),
      getExampleOutput(example.path, example.sdk),
      getExampleLogs(example.path, example.sdk),
      getExampleGraph(example.path, example.sdk)
    ]);

    return Example.fromBase(
      example,
      source: exampleData[0],
      outputs: exampleData[1],
      logs: exampleData[2],
      graph: exampleData[3],
    );
  }

  Future<void> _loadCategories() async {
    final result = await _exampleRepository.getListOfExamples(
      const GetPrecompiledObjectsRequest(
        sdk: null,
        category: null,
      ),
    );

    setSdkCategories(result);
  }

  void changeSelectorVisibility() {
    isSelectorOpened = !isSelectorOpened;
    notifyListeners();
  }

  Future<void> loadDefaultExamples() async {
    if (defaultExamplesBySdk.isNotEmpty) {
      return;
    }

    try {
      await Future.wait(Sdk.known.map(_loadDefaultExample));
    } catch (ex) {
      if (defaultExamplesBySdk.isEmpty) {
        rethrow;
      }
      // As long as any of the examples is loaded, continue.
      print(ex);
      // TODO: Log.
    }

    notifyListeners();
  }

  Future<void> _loadDefaultExample(Sdk sdk) async {
    final exampleWithoutInfo = await _exampleRepository.getDefaultExample(
      GetDefaultPrecompiledObjectRequest(sdk: sdk),
    );

    defaultExamplesBySdk[sdk] = await loadExampleInfo(exampleWithoutInfo);
  }

  Future<void> loadDefaultExamplesIfNot() async {
    if (defaultExamplesBySdk.isNotEmpty) {
      return;
    }

    await loadDefaultExamples();
  }

  Future<ExampleBase?> getCatalogExampleByPath(String path) async {
    await allExamplesFuture;

    final allExamples = categoryListsBySdk.values
        .expand((categories) => categories.map((c) => c.examples))
        .expand((examples) => examples);

    return allExamples.firstWhereOrNull(
      (e) => e.path == path,
    );
  }
}
