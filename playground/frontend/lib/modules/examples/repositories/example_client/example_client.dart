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

import 'package:playground/modules/editor/repository/code_repository/code_client/output_response.dart';
import 'package:playground/modules/examples/repositories/models/get_snippet_request.dart';
import 'package:playground/modules/examples/repositories/models/get_snippet_response.dart';
import 'package:playground/modules/examples/repositories/models/get_example_code_response.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_example_response.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_response.dart';
import 'package:playground/modules/examples/repositories/models/save_snippet_request.dart';
import 'package:playground/modules/examples/repositories/models/save_snippet_response.dart';

abstract class ExampleClient {
  Future<GetListOfExampleResponse> getListOfExamples(
    GetListOfExamplesRequestWrapper request,
  );

  Future<GetExampleCodeResponse> getExampleSource(
    GetExampleRequestWrapper request,
  );

  Future<GetExampleResponse> getDefaultExample(
    GetExampleRequestWrapper request,
  );

  Future<GetExampleResponse> getExample(
    GetExampleRequestWrapper request,
  );

  Future<OutputResponse> getExampleOutput(
    GetExampleRequestWrapper request,
  );

  Future<OutputResponse> getExampleLogs(
    GetExampleRequestWrapper request,
  );

  Future<OutputResponse> getExampleGraph(
    GetExampleRequestWrapper request,
  );

  Future<GetSnippetResponse> getSnippet(
    GetSnippetRequestWrapper request,
  );

  Future<SaveSnippetResponse> saveSnippet(
    SaveSnippetRequestWrapper request,
  );
}
