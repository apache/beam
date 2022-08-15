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

import 'package:playground/modules/examples/models/example_loading_descriptors/catalog_default_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/empty_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/standard_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/user_shared_example_loading_descriptor.dart';
import 'package:playground/pages/playground/states/example_loaders/catalog_default_example_loader.dart';
import 'package:playground/pages/playground/states/example_loaders/empty_example_loader.dart';
import 'package:playground/pages/playground/states/example_loaders/example_loader.dart';
import 'package:playground/pages/playground/states/example_loaders/standard_example_loader.dart';
import 'package:playground/pages/playground/states/example_loaders/user_shared_example_loader.dart';
import 'package:playground/pages/playground/states/playground_state.dart';

class ExamplesLoader {
  PlaygroundState? _playgroundState;
  ExamplesLoadingDescriptor? _descriptor;

  void setPlaygroundState(PlaygroundState value) {
    _playgroundState = value;
  }

  Future<void> load(ExamplesLoadingDescriptor descriptor) async {
    if (_descriptor == descriptor) {
      return;
    }

    _descriptor = descriptor;
    await Future.wait(descriptor.descriptors.map(_loadOne));
  }

  Future<void> _loadOne(ExampleLoadingDescriptor descriptor) async {
    final example = await _getOneLoader(descriptor).future;
    _playgroundState!.setExample(example);
  }

  ExampleLoader _getOneLoader(ExampleLoadingDescriptor descriptor) {
    final exampleState = _playgroundState!.exampleState;

    if (descriptor is CatalogDefaultExampleLoadingDescriptor) {
      return CatalogDefaultExampleLoader(
        descriptor: descriptor,
        exampleState: exampleState,
      );
    }

    if (descriptor is EmptyExampleLoadingDescriptor) {
      return EmptyExampleLoader(
        descriptor: descriptor,
        exampleState: exampleState,
      );
    }

    if (descriptor is StandardExampleLoadingDescriptor) {
      return StandardExampleLoader(
        descriptor: descriptor,
        exampleState: exampleState,
      );
    }

    if (descriptor is UserSharedExampleLoadingDescriptor) {
      return UserSharedExampleLoader(
        descriptor: descriptor,
        exampleState: exampleState,
      );
    }

    throw Exception('Unknown example loading descriptor: $descriptor');
  }
}
