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

import '../../cache/example_cache.dart';
import '../../models/example.dart';
import '../../models/example_loading_descriptors/empty_example_loading_descriptor.dart';
import '../../models/sdk.dart';
import 'example_loader.dart';

/// The [ExampleLoader] for [EmptyExampleLoadingDescriptor].
///
/// Empties the editor.
class EmptyExampleLoader extends ExampleLoader {
  @override
  final EmptyExampleLoadingDescriptor descriptor;

  final Example _example;

  EmptyExampleLoader({
    required this.descriptor,
    // TODO(alexeyinkin): Remove when this lands: https://github.com/dart-lang/language/issues/1813
    required ExampleCache exampleCache,
  }) : _example = Example.empty(descriptor.sdk);

  @override
  Sdk get sdk => _example.sdk;

  @override
  Future<Example> get future async => _example;
}
