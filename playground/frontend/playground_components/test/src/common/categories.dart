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

import 'dart:collection';

import 'package:playground_components/src/models/category_with_examples.dart';
import 'package:playground_components/src/models/sdk.dart';

import 'examples.dart';

final categoriesMock = [
  CategoryWithExamples(title: 'Filtered', examples: [exampleMock1]),
  CategoryWithExamples(
    title: 'Unfiltered',
    // exampleMock2 is repeated to test that 'tag2' is more frequent than 'tag1'
    examples: [exampleMock1, exampleMock2, exampleMock2, exampleMock2],
  ),
];

final filteredCategories = [
  CategoryWithExamples(title: 'Filtered', examples: [exampleMock1]),
];

const filteredExamples = [exampleMock1, exampleMock2];

const examplesFilteredByTypeMock = [exampleMock2];

const examplesFilteredByTagsMock = [exampleMock2];

const examplesFilteredByNameMock = [exampleMock1];

final sdkCategoriesFromServerMock = UnmodifiableMapView({
  Sdk.java: categoriesMock,
  Sdk.python: categoriesMock,
  Sdk.go: categoriesMock,
  Sdk.scio: categoriesMock,
});
