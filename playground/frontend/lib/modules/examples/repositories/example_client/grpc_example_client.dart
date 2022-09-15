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

import 'package:grpc/grpc_web.dart';
import 'package:playground/api/iis_workaround_channel.dart';
import 'package:playground/api/v1/api.pbgrpc.dart' as grpc;
import 'package:playground/config.g.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/output_response.dart';
import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/examples/repositories/example_client/example_client.dart';
import 'package:playground/modules/examples/repositories/models/get_example_code_response.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_example_response.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_response.dart';
import 'package:playground/modules/examples/repositories/models/get_snippet_request.dart';
import 'package:playground/modules/examples/repositories/models/get_snippet_response.dart';
import 'package:playground/modules/examples/repositories/models/save_snippet_request.dart';
import 'package:playground/modules/examples/repositories/models/save_snippet_response.dart';
import 'package:playground/modules/examples/repositories/models/shared_file_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/utils/replace_incorrect_symbols.dart';

class GrpcExampleClient implements ExampleClient {
  late final grpc.PlaygroundServiceClient _defaultClient;

  GrpcExampleClient() {
    final channel = IisWorkaroundChannel.xhr(
      Uri.parse(kApiClientURL),
    );
    _defaultClient = grpc.PlaygroundServiceClient(channel);
  }

  @override
  Future<GetListOfExampleResponse> getListOfExamples(
    GetListOfExamplesRequestWrapper request,
  ) {
    return _runSafely(
      () => _defaultClient
          .getPrecompiledObjects(
              _getListOfExamplesRequestToGrpcRequest(request))
          .then((response) => GetListOfExampleResponse(
              _toClientCategories(response.sdkCategories))),
    );
  }

  @override
  Future<GetExampleResponse> getDefaultExample(
    GetExampleRequestWrapper request,
  ) {
    return _runSafely(
      () => _defaultClient
          .getDefaultPrecompiledObject(
              _getDefaultExampleRequestToGrpcRequest(request))
          .then(
            (response) => GetExampleResponse(
              _toExampleModel(
                request.sdk,
                response.precompiledObject,
              ),
            ),
          ),
    );
  }

  @override
  Future<GetExampleResponse> getExample(
    GetExampleRequestWrapper request,
  ) {
    return _runSafely(
      () => _defaultClient
          .getPrecompiledObject(
              grpc.GetPrecompiledObjectRequest()..cloudPath = request.path)
          .then(
            (response) => GetExampleResponse(
              _toExampleModel(
                request.sdk,
                response.precompiledObject,
              ),
            ),
          ),
    );
  }

  @override
  Future<GetExampleCodeResponse> getExampleSource(
      GetExampleRequestWrapper request) {
    return _runSafely(
      () => _defaultClient
          .getPrecompiledObjectCode(
              _getExampleCodeRequestToGrpcRequest(request))
          .then((response) =>
              GetExampleCodeResponse(replaceIncorrectSymbols(response.code))),
    );
  }

  @override
  Future<OutputResponse> getExampleOutput(GetExampleRequestWrapper request) {
    return _runSafely(
      () => _defaultClient
          .getPrecompiledObjectOutput(
              _getExampleOutputRequestToGrpcRequest(request))
          .then((response) =>
              OutputResponse(replaceIncorrectSymbols(response.output)))
          .catchError((err) {
        print(err);
        return OutputResponse('');
      }),
    );
  }

  @override
  Future<OutputResponse> getExampleLogs(GetExampleRequestWrapper request) {
    return _runSafely(
      () => _defaultClient
          .getPrecompiledObjectLogs(_getExampleLogRequestToGrpcRequest(request))
          .then((response) =>
              OutputResponse(replaceIncorrectSymbols(response.output)))
          .catchError((err) {
        print(err);
        return OutputResponse('');
      }),
    );
  }

  @override
  Future<OutputResponse> getExampleGraph(GetExampleRequestWrapper request) {
    return _runSafely(
      () => _defaultClient
          .getPrecompiledObjectGraph(
              _getExampleGraphRequestToGrpcRequest(request))
          .then((response) => OutputResponse(response.graph))
          .catchError((err) {
        print(err);
        return OutputResponse('');
      }),
    );
  }

  @override
  Future<GetSnippetResponse> getSnippet(
    GetSnippetRequestWrapper request,
  ) {
    return _runSafely(
      () => _defaultClient
          .getSnippet(_getSnippetRequestToGrpcRequest(request))
          .then(
            (response) => GetSnippetResponse(
              files: _convertToSharedFileList(response.files),
              sdk: _getAppSdk(response.sdk),
              pipelineOptions: response.pipelineOptions,
            ),
          ),
    );
  }

  @override
  Future<SaveSnippetResponse> saveSnippet(
    SaveSnippetRequestWrapper request,
  ) {
    return _runSafely(
      () => _defaultClient
          .saveSnippet(_saveSnippetRequestToGrpcRequest(request))
          .then(
            (response) => SaveSnippetResponse(
              id: response.id,
            ),
          ),
    );
  }

  Future<T> _runSafely<T>(Future<T> Function() invoke) {
    try {
      return invoke();
    } on GrpcError catch (error) {
      throw Exception(error.message);
    }
  }

  grpc.GetPrecompiledObjectsRequest _getListOfExamplesRequestToGrpcRequest(
    GetListOfExamplesRequestWrapper request,
  ) {
    return grpc.GetPrecompiledObjectsRequest()
      ..category = request.category ?? ''
      ..sdk = request.sdk == null
          ? grpc.Sdk.SDK_UNSPECIFIED
          : _getGrpcSdk(request.sdk!);
  }

  grpc.GetDefaultPrecompiledObjectRequest
      _getDefaultExampleRequestToGrpcRequest(
    GetExampleRequestWrapper request,
  ) {
    return grpc.GetDefaultPrecompiledObjectRequest()
      ..sdk = _getGrpcSdk(request.sdk);
  }

  grpc.GetPrecompiledObjectCodeRequest _getExampleCodeRequestToGrpcRequest(
    GetExampleRequestWrapper request,
  ) {
    return grpc.GetPrecompiledObjectCodeRequest()..cloudPath = request.path;
  }

  grpc.GetPrecompiledObjectOutputRequest _getExampleOutputRequestToGrpcRequest(
    GetExampleRequestWrapper request,
  ) {
    return grpc.GetPrecompiledObjectOutputRequest()..cloudPath = request.path;
  }

  grpc.GetPrecompiledObjectLogsRequest _getExampleLogRequestToGrpcRequest(
    GetExampleRequestWrapper request,
  ) {
    return grpc.GetPrecompiledObjectLogsRequest()..cloudPath = request.path;
  }

  grpc.GetPrecompiledObjectGraphRequest _getExampleGraphRequestToGrpcRequest(
    GetExampleRequestWrapper request,
  ) {
    return grpc.GetPrecompiledObjectGraphRequest()..cloudPath = request.path;
  }

  grpc.GetSnippetRequest _getSnippetRequestToGrpcRequest(
    GetSnippetRequestWrapper request,
  ) {
    return grpc.GetSnippetRequest()..id = request.id;
  }

  grpc.SaveSnippetRequest _saveSnippetRequestToGrpcRequest(
    SaveSnippetRequestWrapper request,
  ) {
    return grpc.SaveSnippetRequest()
      ..sdk = _getGrpcSdk(request.sdk)
      ..pipelineOptions = request.pipelineOptions
      ..files.addAll(_convertToSnippetFileList(request.files));
  }

  grpc.Sdk _getGrpcSdk(SDK sdk) {
    switch (sdk) {
      case SDK.java:
        return grpc.Sdk.SDK_JAVA;
      case SDK.go:
        return grpc.Sdk.SDK_GO;
      case SDK.python:
        return grpc.Sdk.SDK_PYTHON;
      case SDK.scio:
        return grpc.Sdk.SDK_SCIO;
    }
  }

  SDK _getAppSdk(grpc.Sdk sdk) {
    switch (sdk) {
      case grpc.Sdk.SDK_JAVA:
        return SDK.java;
      case grpc.Sdk.SDK_GO:
        return SDK.go;
      case grpc.Sdk.SDK_PYTHON:
        return SDK.python;
      case grpc.Sdk.SDK_SCIO:
        return SDK.scio;
      default:
        return SDK.java;
    }
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
      default:
        return ExampleType.example;
    }
  }

  Map<SDK, List<CategoryModel>> _toClientCategories(
    List<grpc.Categories> response,
  ) {
    Map<SDK, List<CategoryModel>> sdkCategoriesMap = {};
    List<MapEntry<SDK, List<CategoryModel>>> entries = [];
    for (var sdkMap in response) {
      SDK sdk = _getAppSdk(sdkMap.sdk);
      List<CategoryModel> categoriesForSdk = [];
      for (var category in sdkMap.categories) {
        List<ExampleModel> examples = category.precompiledObjects
            .map((example) => _toExampleModel(sdk, example))
            .toList()
          ..sort();
        categoriesForSdk.add(CategoryModel(
          name: category.categoryName,
          examples: examples,
        ));
      }
      entries.add(MapEntry(sdk, categoriesForSdk..sort()));
    }
    sdkCategoriesMap.addEntries(entries);
    return sdkCategoriesMap;
  }

  ExampleModel _toExampleModel(SDK sdk, grpc.PrecompiledObject example) {
    return ExampleModel(
      sdk: sdk,
      name: example.name,
      description: example.description,
      type: _exampleTypeFromString(example.type),
      path: example.cloudPath,
      contextLine: example.contextLine,
      pipelineOptions: example.pipelineOptions,
      isMultiFile: example.multifile,
      link: example.link,
    );
  }

  List<SharedFile> _convertToSharedFileList(
    List<grpc.SnippetFile> snippetFileList,
  ) {
    final sharedFilesList = <SharedFile>[];

    for (grpc.SnippetFile item in snippetFileList) {
      sharedFilesList.add(SharedFile(
        code: item.content,
        isMain: item.isMain,
        name: item.name,
      ));
    }

    return sharedFilesList;
  }

  List<grpc.SnippetFile> _convertToSnippetFileList(
    List<SharedFile> sharedFilesList,
  ) {
    final snippetFileList = <grpc.SnippetFile>[];

    for (SharedFile item in sharedFilesList) {
      snippetFileList.add(
        grpc.SnippetFile()
          ..name = item.name
          ..isMain = true
          ..content = item.code,
      );
    }

    return snippetFileList;
  }
}
