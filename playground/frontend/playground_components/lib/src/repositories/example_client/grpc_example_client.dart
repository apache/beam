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

import 'package:grpc/grpc.dart';

import '../../api/iis_workaround_channel.dart';
import '../../api/v1/api.pbgrpc.dart' as grpc;
import '../../models/category_with_examples.dart';
import '../../models/example_base.dart';
import '../../models/sdk.dart';
import '../../models/snippet_file.dart';
import '../complexity_grpc_extension.dart';
import '../dataset_grpc_extension.dart';
import '../models/get_default_precompiled_object_request.dart';
import '../models/get_precompiled_object_code_response.dart';
import '../models/get_precompiled_object_request.dart';
import '../models/get_precompiled_object_response.dart';
import '../models/get_precompiled_objects_request.dart';
import '../models/get_precompiled_objects_response.dart';
import '../models/get_snippet_request.dart';
import '../models/get_snippet_response.dart';
import '../models/output_response.dart';
import '../models/save_snippet_request.dart';
import '../models/save_snippet_response.dart';
import '../models/snippet_file_grpc_extension.dart';
import '../sdk_grpc_extension.dart';
import 'example_client.dart';

class GrpcExampleClient implements ExampleClient {
  final grpc.PlaygroundServiceClient _defaultClient;

  factory GrpcExampleClient({
    required Uri url,
  }) {
    final channel = IisWorkaroundChannel.xhr(url);

    return GrpcExampleClient._(
      client: grpc.PlaygroundServiceClient(channel),
    );
  }

  GrpcExampleClient._({
    required grpc.PlaygroundServiceClient client,
  }) : _defaultClient = client;

  @override
  Future<grpc.GetMetadataResponse> getMetadata() async {
    return _runSafely(
      () => _defaultClient.getMetadata(
        grpc.GetMetadataRequest(),
      ),
    );
  }

  @override
  Future<GetPrecompiledObjectsResponse> getPrecompiledObjects(
    GetPrecompiledObjectsRequest request,
  ) async {
    final response = await _runSafely(
      () => _defaultClient.getPrecompiledObjects(
        _grpcGetPrecompiledObjectsRequest(request),
      ),
    );
    return GetPrecompiledObjectsResponse(
      categories: _toClientCategories(response.sdkCategories),
    );
  }

  @override
  Future<GetPrecompiledObjectResponse> getDefaultPrecompiledObject(
    GetDefaultPrecompiledObjectRequest request,
  ) async {
    final response = await _runSafely(
      () => _defaultClient.getDefaultPrecompiledObject(
        _grpcGetDefaultPrecompiledObjectRequest(request),
      ),
    );

    return GetPrecompiledObjectResponse(
      example: _toExampleModel(
        request.sdk,
        response.precompiledObject,
      ),
    );
  }

  @override
  Future<GetPrecompiledObjectResponse> getPrecompiledObject(
    GetPrecompiledObjectRequest request,
  ) async {
    final response = await _runSafely(
      () => _defaultClient.getPrecompiledObject(
        grpc.GetPrecompiledObjectRequest()..cloudPath = request.path,
      ),
    );

    return GetPrecompiledObjectResponse(
      example: _toExampleModel(
        request.sdk,
        response.precompiledObject,
      ),
    );
  }

  @override
  Future<GetPrecompiledObjectCodeResponse> getPrecompiledObjectCode(
    GetPrecompiledObjectRequest request,
  ) async {
    final response = await _runSafely(
      () => _defaultClient.getPrecompiledObjectCode(
        _grpcGetPrecompiledObjectRequest(request),
      ),
    );

    return GetPrecompiledObjectCodeResponse(
      files: response.files.map((f) => f.model).toList(growable: false),
    );
  }

  @override
  Future<OutputResponse> getPrecompiledObjectOutput(
    GetPrecompiledObjectRequest request,
  ) async {
    try {
      final response = await _runSafely(
        () => _defaultClient.getPrecompiledObjectOutput(
          _grpcGetPrecompiledObjectOutputRequest(request),
        ),
      );

      return OutputResponse(output: response.output);
    } catch (ex) {
      print(ex);
      return OutputResponse(
        output: '',
      );
    }
  }

  @override
  Future<OutputResponse> getPrecompiledObjectLogs(
    GetPrecompiledObjectRequest request,
  ) async {
    try {
      final response = await _runSafely(
        () => _defaultClient.getPrecompiledObjectLogs(
          _grpcGetPrecompiledObjectLogRequest(request),
        ),
      );

      return OutputResponse(output: response.output);
    } catch (ex) {
      print(ex);
      return OutputResponse(
        output: '',
      );
    }
  }

  @override
  Future<OutputResponse> getPrecompiledObjectGraph(
    GetPrecompiledObjectRequest request,
  ) async {
    try {
      final response = await _runSafely(
        () => _defaultClient.getPrecompiledObjectGraph(
          _grpcGetPrecompiledGraphRequest(request),
        ),
      );

      return OutputResponse(
        output: response.graph,
      );
    } catch (ex) {
      print(ex);
      return OutputResponse(
        output: '',
      );
    }
  }

  @override
  Future<GetSnippetResponse> getSnippet(
    GetSnippetRequest request,
  ) async {
    final response = await _runSafely(
      () => _defaultClient.getSnippet(
        _grpcGetSnippetRequest(request),
      ),
    );

    return GetSnippetResponse(
      files: _convertToSharedFileList(response.files),
      sdk: response.sdk.model,
      pipelineOptions: response.pipelineOptions,
      complexity: response.complexity.model,
    );
  }

  @override
  Future<SaveSnippetResponse> saveSnippet(
    SaveSnippetRequest request,
  ) async {
    final response = await _runSafely(
      () => _defaultClient.saveSnippet(
        _grpcSaveSnippetRequest(request),
      ),
    );

    return SaveSnippetResponse(
      id: response.id,
    );
  }

  Future<T> _runSafely<T>(Future<T> Function() invoke) async {
    try {
      return await invoke();
    } on GrpcError catch (error) {
      throw Exception(error.message);
    }
  }

  grpc.GetPrecompiledObjectsRequest _grpcGetPrecompiledObjectsRequest(
    GetPrecompiledObjectsRequest request,
  ) {
    return grpc.GetPrecompiledObjectsRequest()
      ..category = request.category ?? ''
      ..sdk = request.sdk?.grpc ?? grpc.Sdk.SDK_UNSPECIFIED;
  }

  grpc.GetDefaultPrecompiledObjectRequest
      _grpcGetDefaultPrecompiledObjectRequest(
    GetDefaultPrecompiledObjectRequest request,
  ) {
    return grpc.GetDefaultPrecompiledObjectRequest()..sdk = request.sdk.grpc;
  }

  grpc.GetPrecompiledObjectCodeRequest _grpcGetPrecompiledObjectRequest(
    GetPrecompiledObjectRequest request,
  ) {
    return grpc.GetPrecompiledObjectCodeRequest()..cloudPath = request.path;
  }

  grpc.GetPrecompiledObjectOutputRequest _grpcGetPrecompiledObjectOutputRequest(
    GetPrecompiledObjectRequest request,
  ) {
    return grpc.GetPrecompiledObjectOutputRequest()..cloudPath = request.path;
  }

  grpc.GetPrecompiledObjectLogsRequest _grpcGetPrecompiledObjectLogRequest(
    GetPrecompiledObjectRequest request,
  ) {
    return grpc.GetPrecompiledObjectLogsRequest()..cloudPath = request.path;
  }

  grpc.GetPrecompiledObjectGraphRequest _grpcGetPrecompiledGraphRequest(
    GetPrecompiledObjectRequest request,
  ) {
    return grpc.GetPrecompiledObjectGraphRequest()..cloudPath = request.path;
  }

  grpc.GetSnippetRequest _grpcGetSnippetRequest(
    GetSnippetRequest request,
  ) {
    return grpc.GetSnippetRequest()..id = request.id;
  }

  grpc.SaveSnippetRequest _grpcSaveSnippetRequest(
    SaveSnippetRequest request,
  ) {
    return grpc.SaveSnippetRequest()
      ..sdk = request.sdk.grpc
      ..pipelineOptions = request.pipelineOptions
      ..files.addAll(_convertToSnippetFileList(request.files));
  }

  ExampleType _exampleTypeFromString(grpc.PrecompiledObjectType type) {
    switch (type) {
      case grpc.PrecompiledObjectType.PRECOMPILED_OBJECT_TYPE_EXAMPLE:
        return ExampleType.example;
      case grpc.PrecompiledObjectType.PRECOMPILED_OBJECT_TYPE_KATA:
        return ExampleType.kata;
      case grpc.PrecompiledObjectType.PRECOMPILED_OBJECT_TYPE_UNIT_TEST:
        return ExampleType.test;
      case grpc.PrecompiledObjectType.PRECOMPILED_OBJECT_TYPE_UNSPECIFIED:
        return ExampleType.all;
    }

    return ExampleType.example;
  }

  Map<Sdk, List<CategoryWithExamples>> _toClientCategories(
    List<grpc.Categories> response,
  ) {
    final result = <Sdk, List<CategoryWithExamples>>{};

    for (final sdkMap in response) {
      final sdk = sdkMap.sdk.model;
      final categoriesForSdk = <CategoryWithExamples>[];

      for (final category in sdkMap.categories) {
        final examples = category.precompiledObjects
            .map((example) => _toExampleModel(sdk, example))
            .toList(growable: false)
          ..sort();

        categoriesForSdk.add(
          CategoryWithExamples(
            title: category.categoryName,
            examples: examples,
          ),
        );
      }

      result[sdk] = categoriesForSdk..sort();
    }

    return result;
  }

  ExampleBase _toExampleModel(Sdk sdk, grpc.PrecompiledObject example) {
    return ExampleBase(
      alwaysRun: example.alwaysRun,
      complexity: example.complexity.model,
      contextLine: example.contextLine,
      description: example.description,
      datasets: example.datasets.map((e) => e.model).toList(growable: false),
      isMultiFile: example.multifile,
      name: example.name,
      path: example.cloudPath,
      pipelineOptions: example.pipelineOptions,
      sdk: sdk,
      tags: example.tags,
      type: _exampleTypeFromString(example.type),
      urlNotebook: example.urlNotebook,
      urlVcs: example.urlVcs,
    );
  }

  List<SnippetFile> _convertToSharedFileList(
    List<grpc.SnippetFile> snippetFileList,
  ) {
    final sharedFilesList = <SnippetFile>[];

    for (final item in snippetFileList) {
      sharedFilesList.add(
        SnippetFile(
          content: item.content,
          isMain: item.isMain,
          name: item.name,
        ),
      );
    }

    return sharedFilesList;
  }

  List<grpc.SnippetFile> _convertToSnippetFileList(
    List<SnippetFile> sharedFilesList,
  ) {
    final snippetFileList = <grpc.SnippetFile>[];

    for (final item in sharedFilesList) {
      snippetFileList.add(
        grpc.SnippetFile(
          content: item.content,
          isMain: true,
          name: item.name,
        ),
      );
    }

    return snippetFileList;
  }
}
