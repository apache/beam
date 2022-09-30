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
import '../../models/example.dart';
import '../../models/example_base.dart';
import '../../models/example_loading_descriptors/standard_example_loading_descriptor.dart';
import '../../models/sdk.dart';
import 'example_loader.dart';

/// Loads a given example from the local cache, then adds info from network.
///
/// This loader assumes that [ExampleState] is loading all examples to
/// its cache. So it only completes if this is successful.
class StandardExampleLoader extends ExampleLoader {
  final StandardExampleLoadingDescriptor descriptor;
  final ExampleCache exampleCache;
  final _completer = Completer<Example>();

  @override
  Future<Example> get future => _completer.future;

  StandardExampleLoader({
    required this.descriptor,
    required this.exampleCache,
  }) {
    _load();
  }

  Future<void> _load() async {
    final example = await _loadExampleWithoutInfo();

    if (example == null) {
      _completer.completeError('Example not found: $descriptor');
      return;
    }

    _completer.complete(
      exampleCache.loadExampleInfo(example),
    );
  }

  Future<ExampleBase?> _loadExampleWithoutInfo() {
    return exampleCache.hasCatalog
        ? exampleCache.getCatalogExampleByPath(descriptor.path)
        : _loadExampleFromRepository();
  }

  Future<ExampleBase?> _loadExampleFromRepository() async {
    final sdk = Sdk.tryParseExamplePath(descriptor.path);

    if (sdk == null) {
      return null;
    }

    return exampleCache.getExample(descriptor.path, sdk);
  }
}
