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

import 'dart:async';

import '../../cache/example_cache.dart';
import '../../exceptions/multiple_exceptions.dart';
import '../../models/example.dart';
import '../../models/example_loading_descriptors/standard_example_loading_descriptor.dart';
import '../../models/sdk.dart';
import 'example_loader.dart';

/// Loads a given example from the local cache, then adds info from network.
///
/// This loader assumes that [ExampleCache] is loading all examples to
/// its cache. So it only completes if this is successful.
class StandardExampleLoader extends ExampleLoader {
  @override
  final StandardExampleLoadingDescriptor descriptor;

  final ExampleCache exampleCache;

  @override
  Sdk? get sdk => descriptor.sdk;

  @override
  late Future<Example> future = _load();

  StandardExampleLoader({
    required this.descriptor,
    required this.exampleCache,
  });

  Future<Example> _load() async {
    try {
      final exampleWithoutOptions = await exampleCache.getPrecompiledObject(
        descriptor.path,
        descriptor.sdk,
      );

      return exampleWithoutOptions.copyWith(
        viewOptions: descriptor.viewOptions,
      );
    } on Exception catch (ex, trace) {
      return _tryLoadSharedExample(
        previousExceptions: [ex],
        previousStackTraces: [trace],
      );
    }
  }

  Future<Example> _tryLoadSharedExample({
    required List<Exception> previousExceptions,
    required List<StackTrace> previousStackTraces,
  }) async {
    try {
      return await exampleCache.loadSharedExample(
        descriptor.path,
        viewOptions: descriptor.viewOptions,
      );
    } on Exception catch (ex, trace) {
      throw MultipleExceptions(
        'Cannot load example: ${descriptor.path}',
        exceptions: [...previousExceptions, ex],
        stackTraces: [...previousStackTraces, trace],
      );
    }
  }
}
