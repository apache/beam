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

import 'package:playground_components/src/models/sdk.dart';
import 'package:playground_components/src/models/snippet_file.dart';
import 'package:playground_components/src/repositories/models/get_default_precompiled_object_request.dart';
import 'package:playground_components/src/repositories/models/get_precompiled_object_code_response.dart';
import 'package:playground_components/src/repositories/models/get_precompiled_object_request.dart';
import 'package:playground_components/src/repositories/models/get_precompiled_object_response.dart';
import 'package:playground_components/src/repositories/models/get_precompiled_objects_request.dart';
import 'package:playground_components/src/repositories/models/get_precompiled_objects_response.dart';
import 'package:playground_components/src/repositories/models/output_response.dart';

import 'categories.dart';
import 'examples.dart';

const kGetPrecompiledObjectsRequest = GetPrecompiledObjectsRequest(
  sdk: null,
  category: null,
);
final kGetPrecompiledObjectsResponse = GetPrecompiledObjectsResponse(
  categories: sdkCategoriesFromServerMock,
);

const kGetDefaultPrecompiledObjectRequest = GetDefaultPrecompiledObjectRequest(
  sdk: Sdk.java,
);
const kGetDefaultPrecompiledObjectResponse = GetPrecompiledObjectResponse(
  example: examplePython1,
);

const kGetPrecompiledObjectCodeResponse = GetPrecompiledObjectCodeResponse(
  files: [SnippetFile(content: 'test source', isMain: true)],
);
const kOutputResponse = OutputResponse(output: 'test outputs');

const kRequestForExampleInfo = GetPrecompiledObjectRequest(
  path: 'SDK_PYTHON/Category/Name',
  sdk: Sdk.python,
);
const kRequestDefaultExampleForJava = GetDefaultPrecompiledObjectRequest(
  sdk: Sdk.java,
);
const kRequestDefaultExampleForGo = GetDefaultPrecompiledObjectRequest(
  sdk: Sdk.go,
);
const kRequestDefaultExampleForPython = GetDefaultPrecompiledObjectRequest(
  sdk: Sdk.python,
);
const kRequestDefaultExampleForScio = GetDefaultPrecompiledObjectRequest(
  sdk: Sdk.scio,
);
