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

import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components/src/controllers/example_loaders/example_loader_factory.dart';

import '../../common/example_cache.mocks.dart';
import 'common.dart';

void main() {
  late ExampleLoaderFactory factory;
  final cache = MockExampleCache();

  setUp(() {
    factory = ExampleLoaderFactory();
    TestExampleLoader.register(factory);
  });

  group('ExampleLoaderFactory', () {
    test('creates loaders', () {
      const descriptor = TestExampleLoadingDescriptor(Sdk.go);
      final loader =
          factory.create(descriptor: descriptor, exampleCache: cache);

      expect(loader, isA<TestExampleLoader>());
    });

    test('null for unregistered type', () {
      final descriptor = _UnregisteredDescriptor();
      final loader =
          factory.create(descriptor: descriptor, exampleCache: cache);

      expect(loader, null);
    });
  });
}

class _UnregisteredDescriptor extends ExampleLoadingDescriptor {
  @override
  List<Object?> get props => [];

  @override
  ExampleLoadingDescriptor copyWithoutViewOptions() => this;

  @override
  Map<String, dynamic> toJson() => throw UnimplementedError();

  @override
  bool get isSerializableToUrl => true;
}
