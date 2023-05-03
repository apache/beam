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

import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:hive/hive.dart';
import 'package:hive_test/hive_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components/src/controllers/example_loaders/hive_example_loader.dart';

import '../../common/example_cache.mocks.dart';

const _example = Example(
  files: [],
  name: 'name',
  sdk: Sdk.go,
  type: ExampleType.example,
  path: 'path',
);

void main() {
  test('HiveExampleLoader loads locally stored example', () async {
    await setUpTestHive();

    const descriptor = HiveExampleLoadingDescriptor(
      sdk: Sdk.go,
      boxName: 'boxName',
      snippetId: 'snippetId',
    );

    final box = await Hive.openBox(descriptor.boxName);
    await box.put(descriptor.snippetId, jsonEncode(_example.toJson()));

    final loader = HiveExampleLoader(
      descriptor: descriptor,
      exampleCache: MockExampleCache(),
    );

    final exampleFromHive = await loader.future;

    expect(exampleFromHive.name, _example.name);
    expect(exampleFromHive.sdk.id, _example.sdk.id);

    await tearDownTestHive();
  });
}
