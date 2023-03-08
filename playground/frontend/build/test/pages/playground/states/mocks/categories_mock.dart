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
import 'package:playground/modules/sdk/models/sdk.dart';

import 'example_mock.dart';

final categoriesMock = [
  CategoryModel(name: 'Sorted', examples: [exampleMock1]),
  CategoryModel(name: 'Unsorted', examples: [exampleMock2]),
];

final List<CategoryModel> sortedCategories = [
  CategoryModel(name: 'Sorted', examples: [exampleMock1]),
];

final List<ExampleModel> unsortedExamples = [exampleMock1, exampleMock2];

final List<ExampleModel> examplesSortedByTypeMock = [exampleMock2];

final List<ExampleModel> examplesSortedByNameMock = [exampleMock1];

final sdkCategoriesFromServerMock = {
  SDK.java: categoriesMock,
  SDK.python: categoriesMock,
  SDK.go: categoriesMock,
  SDK.scio: categoriesMock,
};
