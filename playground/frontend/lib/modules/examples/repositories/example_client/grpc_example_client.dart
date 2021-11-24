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
import 'package:playground/api/v1/api.pbgrpc.dart' as grpc;
import 'package:playground/modules/editor/repository/code_repository/code_client/output_response.dart';
import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/examples/repositories/example_client/example_client.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_example_response.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_response.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class GrpcExampleClient implements ExampleClient {
  grpc.PlaygroundServiceClient createClient(SDK? sdk) {
    String apiClientURL = SDK.java.getRoute;
    // if (sdk != null) {
    //   apiClientURL = sdk.getRoute;
    // }
    GrpcWebClientChannel channel = GrpcWebClientChannel.xhr(
      Uri.parse(apiClientURL),
    );
    return grpc.PlaygroundServiceClient(channel);
  }

  @override
  Future<GetListOfExampleResponse> getListOfExamples(
    GetListOfExamplesRequestWrapper request,
  ) {
    return _runSafely(
      () => createClient(request.sdk)
          .getPrecompiledObjects(
              _getListOfExamplesRequestToGrpcRequest(request))
          .then((response) => GetListOfExampleResponse(
              _toClientCategories(response.sdkCategories))),
    );
  }

  @override
  Future<GetExampleResponse> getExample(GetExampleRequestWrapper request) {
    return _runSafely(
      () => createClient(request.sdk)
          .getPrecompiledObjectCode(_getExampleRequestToGrpcRequest(request))
          .then((response) => GetExampleResponse(response.code)),
    );
  }

  @override
  Future<OutputResponse> getExampleOutput(GetExampleRequestWrapper request) {
    return _runSafely(
      () => createClient(request.sdk)
          .getPrecompiledObjectOutput(_getExampleRequestToGrpcRequest(request))
          .then((response) => OutputResponse(response.output)),
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

  grpc.GetPrecompiledObjectRequest _getExampleRequestToGrpcRequest(
    GetExampleRequestWrapper request,
  ) {
    return grpc.GetPrecompiledObjectRequest()..cloudPath = request.path;
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
            .map((e) => ExampleModel(
                  name: e.name,
                  description: e.description,
                  type: _exampleTypeFromString(e.type),
                  path: e.cloudPath,
                ))
            .toList();
        categoriesForSdk.add(CategoryModel(
          name: category.categoryName,
          examples: examples,
        ));
      }
      entries.add(MapEntry(
        sdk,
        categoriesForSdk,
      ));
    }
    sdkCategoriesMap.addEntries(entries);
    return sdkCategoriesMap;
  }
}
