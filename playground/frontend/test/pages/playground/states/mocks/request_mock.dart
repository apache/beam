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
import 'package:playground/modules/examples/repositories/models/get_example_code_response.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_example_response.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_response.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

import 'categories_mock.dart';
import 'example_mock.dart';

final kGetListOfExamplesRequestMock =
    GetListOfExamplesRequestWrapper(sdk: null, category: null);
final kGetListOfExamplesResponseMock =
    GetListOfExampleResponse(sdkCategoriesFromServerMock);
final kGetExampleRequestMock = GetExampleRequestWrapper('', SDK.java);
final kGetExampleResponseMock = GetExampleResponse(exampleMock1);
final kGetExampleCodeResponseMock = GetExampleCodeResponse('test source');
final kOutputResponseMock = OutputResponse('test outputs');

final kRequestForExampleInfo =
    GetExampleRequestWrapper('SDK/Category/Name', SDK.java);
final kRequestDefaultExampleForJava = GetExampleRequestWrapper('', SDK.java);
final kRequestDefaultExampleForGo = GetExampleRequestWrapper('', SDK.go);
final kRequestDefaultExampleForPython =
    GetExampleRequestWrapper('', SDK.python);
final kRequestDefaultExampleForScio = GetExampleRequestWrapper('', SDK.scio);
