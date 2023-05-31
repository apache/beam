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

import '../models/category_with_examples.dart';
import '../models/example_base.dart';
import '../models/sdk.dart';
import '../models/snippet_file.dart';
import 'example_client/example_client.dart';
import 'models/get_default_precompiled_object_request.dart';
import 'models/get_precompiled_object_request.dart';
import 'models/get_precompiled_objects_request.dart';
import 'models/get_snippet_request.dart';
import 'models/get_snippet_response.dart';
import 'models/save_snippet_request.dart';

class ExampleRepository {
  final ExampleClient _client;

  ExampleRepository({
    required ExampleClient client,
  }) : _client = client;

  Future<Map<Sdk, List<CategoryWithExamples>>> getPrecompiledObjects(
    GetPrecompiledObjectsRequest request,
  ) async {
    final result = await _client.getPrecompiledObjects(request);
    return result.categories;
  }

  Future<ExampleBase> getDefaultPrecompiledObject(
    GetDefaultPrecompiledObjectRequest request,
  ) async {
    final result = await _client.getDefaultPrecompiledObject(request);
    return result.example;
  }

  Future<List<SnippetFile>> getPrecompiledObjectCode(
    GetPrecompiledObjectRequest request,
  ) async {
    final result = await _client.getPrecompiledObjectCode(request);
    return result.files;
  }

  Future<String> getPrecompiledObjectOutput(
    GetPrecompiledObjectRequest request,
  ) async {
    final result = await _client.getPrecompiledObjectOutput(request);
    return result.output;
  }

  Future<String> getPrecompiledObjectLogs(
    GetPrecompiledObjectRequest request,
  ) async {
    final result = await _client.getPrecompiledObjectLogs(request);
    return result.output;
  }

  Future<String> getPrecompiledObjectGraph(
    GetPrecompiledObjectRequest request,
  ) async {
    final result = await _client.getPrecompiledObjectGraph(request);
    return result.output;
  }

  Future<ExampleBase> getPrecompiledObject(
    GetPrecompiledObjectRequest request,
  ) async {
    final result = await _client.getPrecompiledObject(request);
    return result.example;
  }

  Future<GetSnippetResponse> getSnippet(
    GetSnippetRequest request,
  ) async {
    final result = await _client.getSnippet(request);
    return result;
  }

  Future<String> saveSnippet(
    SaveSnippetRequest request,
  ) async {
    final result = await _client.saveSnippet(request);
    return result.id;
  }
}
