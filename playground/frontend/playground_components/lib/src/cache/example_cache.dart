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
import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/foundation.dart';

import '../exceptions/catalog_loading_exception.dart';
import '../exceptions/example_loading_exception.dart';
import '../exceptions/snippet_saving_exception.dart';
import '../models/category_with_examples.dart';
import '../models/example.dart';
import '../models/example_base.dart';
import '../models/example_view_options.dart';
import '../models/loading_status.dart';
import '../models/sdk.dart';
import '../models/snippet_file.dart';
import '../repositories/example_repository.dart';
import '../repositories/models/get_default_precompiled_object_request.dart';
import '../repositories/models/get_precompiled_object_request.dart';
import '../repositories/models/get_precompiled_objects_request.dart';
import '../repositories/models/get_snippet_request.dart';
import '../repositories/models/save_snippet_request.dart';

/// A runtime cache for examples fetched from a repository.
class ExampleCache extends ChangeNotifier {
  final ExampleRepository _exampleRepository;
  final categoryListsBySdk = <Sdk, List<CategoryWithExamples>>{};

  @visibleForTesting
  final Map<Sdk, Example> defaultExamplesBySdk = {};

  // TODO(alexeyinkin): Extract, https://github.com/apache/beam/issues/23249
  bool isSelectorOpened = false;

  final _allExamplesCompleter = Completer<void>();

  Future<void> get allExamplesFuture => _allExamplesCompleter.future;

  Future<void>? _allPrecompiledObjectsAttemptFuture;

  ExampleCache({
    required ExampleRepository exampleRepository,
  }) : _exampleRepository = exampleRepository;

  Future<void> loadAllPrecompiledObjectsIfNot() async {
    if (_allPrecompiledObjectsAttemptFuture != null) {
      return await _allPrecompiledObjectsAttemptFuture;
    }

    try {
      _allPrecompiledObjectsAttemptFuture = _loadAllPrecompiledObjects();
      await _allPrecompiledObjectsAttemptFuture!;
    } on Exception catch (ex) {
      _allPrecompiledObjectsAttemptFuture = null;
      throw CatalogLoadingException(ex);
    }
  }

  LoadingStatus get catalogStatus {
    if (_allPrecompiledObjectsAttemptFuture == null) {
      return LoadingStatus.error;
    }

    if (categoryListsBySdk.isEmpty) {
      return LoadingStatus.loading;
    }

    return LoadingStatus.done;
  }

  List<CategoryWithExamples> getCategories(Sdk? sdk) {
    return categoryListsBySdk[sdk] ?? [];
  }

  Future<ExampleBase> getPrecompiledObject(String path, Sdk sdk) {
    return _exampleRepository.getPrecompiledObject(
      GetPrecompiledObjectRequest(path: path, sdk: sdk),
    );
  }

  Future<String?> _getPrecompiledObjectOutput(ExampleBase example) async {
    if (example.alwaysRun) {
      return null;
    }

    return _exampleRepository.getPrecompiledObjectOutput(
      GetPrecompiledObjectRequest(path: example.path, sdk: example.sdk),
    );
  }

  Future<List<SnippetFile>> _getPrecompiledObjectCode(ExampleBase example) {
    return _exampleRepository.getPrecompiledObjectCode(
      GetPrecompiledObjectRequest(path: example.path, sdk: example.sdk),
    );
  }

  Future<String> _getPrecompiledObjectLogs(ExampleBase example) {
    return _exampleRepository.getPrecompiledObjectLogs(
      GetPrecompiledObjectRequest(path: example.path, sdk: example.sdk),
    );
  }

  Future<String> _getPrecompiledObjectGraph(ExampleBase example) {
    return _exampleRepository.getPrecompiledObjectGraph(
      GetPrecompiledObjectRequest(path: example.path, sdk: example.sdk),
    );
  }

  Future<Example> loadSharedExample(
    String id, {
    required ExampleViewOptions viewOptions,
  }) async {
    final result = await _exampleRepository.getSnippet(
      GetSnippetRequest(id: id),
    );

    return Example(
      complexity: result.complexity,
      files: result.files,
      name: 'examples.userSharedName'.tr(),
      isMultiFile: result.files.length > 1,
      path: id,
      sdk: result.sdk,
      pipelineOptions: result.pipelineOptions,
      type: ExampleType.example,
      viewOptions: viewOptions,
    );
  }

  Future<String> saveSnippet({
    required List<SnippetFile> files,
    required Sdk sdk,
    required String pipelineOptions,
  }) async {
    try {
      final id = await _exampleRepository.saveSnippet(
        SaveSnippetRequest(
          files: files,
          sdk: sdk,
          pipelineOptions: pipelineOptions,
        ),
      );
      return id;
    } on Exception catch (ex) {
      throw SnippetSavingException(ex);
    }
  }

  Future<Example> loadExampleInfo(ExampleBase example) async {
    if (example is Example) {
      return example;
    }

    //GRPC GetPrecompiledGraph errors hotfix
    // TODO(alexeyinkin): Remove this special case, https://github.com/apache/beam/issues/24002
    if (example.name == 'MinimalWordCount' &&
        (example.sdk == Sdk.go || example.sdk == Sdk.scio)) {
      final exampleData = await Future.wait([
        _getPrecompiledObjectCode(example),
        _getPrecompiledObjectOutput(example),
        _getPrecompiledObjectLogs(example),
      ]);

      return Example.fromBase(
        example,
        files: exampleData[0]! as List<SnippetFile>,
        outputs: exampleData[1] as String?,
        logs: exampleData[2]! as String,
      );
    }

    // TODO(alexeyinkin): Load in a single request, https://github.com/apache/beam/issues/24305
    final exampleData = await Future.wait([
      _getPrecompiledObjectCode(example),
      _getPrecompiledObjectOutput(example),
      _getPrecompiledObjectLogs(example),
      _getPrecompiledObjectGraph(example)
    ]);

    return Example.fromBase(
      example,
      files: exampleData[0]! as List<SnippetFile>,
      outputs: exampleData[1] as String?,
      logs: exampleData[2]! as String,
      graph: exampleData[3]! as String,
    );
  }

  Future<void> _loadAllPrecompiledObjects() async {
    final result = await _exampleRepository.getPrecompiledObjects(
      const GetPrecompiledObjectsRequest(
        sdk: null,
        category: null,
      ),
    );

    categoryListsBySdk.addAll(result);
    _allExamplesCompleter.complete();
    notifyListeners();
  }

  void setSelectorOpened(bool value) {
    isSelectorOpened = value;
    notifyListeners();
  }

  Future<Example?> getDefaultExampleBySdk(Sdk sdk) async {
    await Future.wait([
      loadAllPrecompiledObjectsIfNot(),
      loadDefaultPrecompiledObjectsIfNot(),
    ]);

    return defaultExamplesBySdk[sdk];
  }

  Future<void> loadDefaultPrecompiledObjects() async {
    if (defaultExamplesBySdk.isNotEmpty) {
      return;
    }

    try {
      await Future.wait(Sdk.known.map(_loadDefaultPrecompiledObject));
    } on Exception catch (ex) {
      if (defaultExamplesBySdk.isEmpty) {
        throw ExampleLoadingException(ex);
      }
      // As long as any of the examples is loaded, continue.
      print(ex);
      // TODO: Log.
    }

    notifyListeners();
  }

  Future<void> _loadDefaultPrecompiledObject(Sdk sdk) async {
    final exampleWithoutInfo =
        await _exampleRepository.getDefaultPrecompiledObject(
      GetDefaultPrecompiledObjectRequest(sdk: sdk),
    );

    defaultExamplesBySdk[sdk] = await loadExampleInfo(exampleWithoutInfo);
  }

  Future<void> loadDefaultPrecompiledObjectsIfNot() async {
    if (defaultExamplesBySdk.isNotEmpty) {
      return;
    }

    await loadDefaultPrecompiledObjects();
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
