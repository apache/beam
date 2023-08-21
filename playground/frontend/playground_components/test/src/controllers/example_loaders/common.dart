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

import 'package:playground_components/playground_components.dart';
import 'package:playground_components/src/controllers/example_loaders/example_loader.dart';
import 'package:playground_components/src/controllers/example_loaders/example_loader_factory.dart';

class TestExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  @override
  final Sdk? sdk;

  final bool succeed;

  const TestExampleLoadingDescriptor(
    this.sdk, {
    this.succeed = true,
  });

  @override
  List<Object?> get props => [sdk, succeed];

  @override
  ExampleLoadingDescriptor copyWithoutViewOptions() => this;

  @override
  Map<String, dynamic> toJson() => throw UnimplementedError();

  @override
  bool get isSerializableToUrl => true;
}

class TestExampleLoader extends ExampleLoader {
  @override
  final TestExampleLoadingDescriptor descriptor;

  final Example? example;

  TestExampleLoader(this.descriptor)
      : example = descriptor.sdk == null
            ? null
            : Example(
                files: [SnippetFile(content: descriptor.sdk!.id, isMain: true)],
                name: descriptor.sdk!.id,
                path: descriptor.sdk!.id,
                sdk: descriptor.sdk!,
                type: ExampleType.example,
              );

  @override
  Sdk? get sdk => descriptor.sdk;

  @override
  Future<Example> get future async {
    if (descriptor.succeed && example != null) {
      return example!;
    }

    throw Exception();
  }

  static void register(ExampleLoaderFactory factory) {
    factory.add(
      ({
        required TestExampleLoadingDescriptor descriptor,
        required ExampleCache exampleCache,
      }) =>
          TestExampleLoader(descriptor),
    );
  }
}
