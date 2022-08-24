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

import 'package:playground/modules/examples/models/example_loading_descriptors/standard_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/states/example_loaders/example_loader.dart';
import 'package:playground/pages/playground/states/examples_state.dart';

/// Loads a given example from the local cache, then adds info from network.
///
/// This loader assumes that [ExampleState] is loading all examples to
/// its cache. So it only completes if this is successful.
class StandardExampleLoader extends ExampleLoader {
  final StandardExampleLoadingDescriptor descriptor;
  final ExampleState exampleState;
  final _completer = Completer<ExampleModel>();

  @override
  Future<ExampleModel> get future => _completer.future;

  StandardExampleLoader({
    required this.descriptor,
    required this.exampleState,
  }) {
    _load();
  }

  void _load() async {
    final example = await _loadExampleWithoutInfo();

    if (example == null) {
      _completer.completeError('Example not found: $descriptor');
      return;
    }

    _completer.complete(
      exampleState.loadExampleInfo(example),
    );
  }

  Future<ExampleModel?> _loadExampleWithoutInfo() {
    return exampleState.hasExampleCatalog
        ? exampleState.getCatalogExampleByPath(descriptor.path)
        : _loadExampleFromRepository();
  }

  Future<ExampleModel?> _loadExampleFromRepository() async {
    final sdk = SDK.tryParseExamplePath(descriptor.path);

    if (sdk == null) {
      return null;
    }

    return exampleState.getExample(descriptor.path, sdk);
  }
}
