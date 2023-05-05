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

import '../../api/v1/api.pb.dart' as grpc;
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

abstract class ExampleClient {
  Future<grpc.GetMetadataResponse> getMetadata();

  Future<GetPrecompiledObjectsResponse> getPrecompiledObjects(
    GetPrecompiledObjectsRequest request,
  );

  Future<GetPrecompiledObjectCodeResponse> getPrecompiledObjectCode(
    GetPrecompiledObjectRequest request,
  );

  Future<GetPrecompiledObjectResponse> getDefaultPrecompiledObject(
    GetDefaultPrecompiledObjectRequest request,
  );

  Future<GetPrecompiledObjectResponse> getPrecompiledObject(
    GetPrecompiledObjectRequest request,
  );

  Future<OutputResponse> getPrecompiledObjectOutput(
    GetPrecompiledObjectRequest request,
  );

  Future<OutputResponse> getPrecompiledObjectLogs(
    GetPrecompiledObjectRequest request,
  );

  Future<OutputResponse> getPrecompiledObjectGraph(
    GetPrecompiledObjectRequest request,
  );

  Future<GetSnippetResponse> getSnippet(
    GetSnippetRequest request,
  );

  Future<SaveSnippetResponse> saveSnippet(
    SaveSnippetRequest request,
  );
}
