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

import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/examples/repositories/example_client/example_client.dart';
import 'package:playground/modules/examples/repositories/models/get_snippet_request.dart';
import 'package:playground/modules/examples/repositories/models/get_snippet_response.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/examples/repositories/models/save_snippet_request.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class ExampleRepository {
  late final ExampleClient _client;

  ExampleRepository(ExampleClient client) {
    _client = client;
  }

  Future<Map<SDK, List<CategoryModel>>> getListOfExamples(
    GetListOfExamplesRequestWrapper request,
  ) async {
    final result = await _client.getListOfExamples(request);
    return result.categories;
  }

  Future<ExampleModel> getDefaultExample(
    GetExampleRequestWrapper request,
  ) async {
    final result = await _client.getDefaultExample(request);
    return result.example;
  }

  Future<String> getExampleSource(
    GetExampleRequestWrapper request,
  ) async {
    final result = await _client.getExampleSource(request);
    return result.code;
  }

  Future<String> getExampleOutput(
    GetExampleRequestWrapper request,
  ) async {
    final result = await _client.getExampleOutput(request);
    return result.output;
  }

  Future<String> getExampleLogs(
    GetExampleRequestWrapper request,
  ) async {
    final result = await _client.getExampleLogs(request);
    return result.output;
  }

  Future<String> getExampleGraph(
    GetExampleRequestWrapper request,
  ) async {
    final result = await _client.getExampleGraph(request);
    return result.output;
  }

  Future<ExampleModel> getExample(
    GetExampleRequestWrapper request,
  ) async {
    final result = await _client.getExample(request);
    return result.example;
  }

  Future<GetSnippetResponse> getSnippet(
    GetSnippetRequestWrapper request,
  ) async {
    final result = await _client.getSnippet(request);
    return result;
  }

  Future<String> saveSnippet(
    SaveSnippetRequestWrapper request,
  ) async {
    final result = await _client.saveSnippet(request);
    return result.id;
  }
}
