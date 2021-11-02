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
import 'package:playground/constants/api.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/output_response.dart';
import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/modules/examples/repositories/example_client/example_client.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_example_response.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_response.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class GrpcExampleClient implements ExampleClient {
  late final GrpcWebClientChannel _channel;
  late final grpc.PlaygroundServiceClient _client;

  GrpcExampleClient() {
    _channel = GrpcWebClientChannel.xhr(
      Uri.parse(kApiClientURL),
    );
    _client = grpc.PlaygroundServiceClient(_channel);
  }

  @override
  Future<GetListOfExampleResponse> getListOfExamples(
    GetListOfExamplesRequestWrapper request,
  ) {
    return _runSafely(
      () => _client
          .getListOfExamples(_getListOfExamplesRequestToGrpcRequest(request))
          .then((response) => GetListOfExampleResponse(
              _toClientCategories(response.sdkCategories))),
    );
  }

  @override
  Future<GetExampleResponse> getExample(GetExampleRequestWrapper request) {
    return _runSafely(
      () => _client
          .getExample(_getExampleRequestToGrpcRequest(request))
          .then((response) => GetExampleResponse(response.code)),
    );
  }

  @override
  Future<OutputResponse> getExampleOutput(GetExampleRequestWrapper request) {
    return _runSafely(
      () => _client
          .getExampleOutput(_getExampleRequestToGrpcRequest(request))
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

  grpc.GetListOfExamplesRequest _getListOfExamplesRequestToGrpcRequest(
    GetListOfExamplesRequestWrapper request,
  ) {
    return grpc.GetListOfExamplesRequest()
      ..category = request.category ?? ''
      ..sdk = request.sdk == null
          ? grpc.Sdk.SDK_UNSPECIFIED
          : _getGrpcSdk(request.sdk!);
  }

  grpc.GetExampleRequest _getExampleRequestToGrpcRequest(
    GetExampleRequestWrapper request,
  ) {
    return grpc.GetExampleRequest()..exampleUuid = request.uuid;
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

  Map<SDK, List<CategoryModel>> _toClientCategories(
    Map<String, grpc.CategoryList> response,
  ) {
    Map<SDK, List<CategoryModel>> output = {};
    // for (SDK sdk in SDK.values) {
    //   final sdkName = sdk.displayName.toLowerCase();
    //   if (response.containsKey(sdkName)) {
    //         response[sdkName]!.categoryExamples.forEach((key, value) {
    //           output[sdk]!.add(CategoryModel(name: key, examples: value.example));
    //         });
    //   }
    // }
    return output;
  }
}
