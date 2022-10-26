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

import 'package:collection/collection.dart';

import '../../models/example_loading_descriptors/example_loading_descriptor.dart';
import '../../models/example_loading_descriptors/examples_loading_descriptor.dart';
import '../../models/sdk.dart';
import '../playground_controller.dart';
import 'catalog_default_example_loader.dart';
import 'content_example_loader.dart';
import 'empty_example_loader.dart';
import 'example_loader_factory.dart';
import 'standard_example_loader.dart';
import 'user_shared_example_loader.dart';

class ExamplesLoader {
  final defaultFactory = ExampleLoaderFactory();
  PlaygroundController? _playgroundController;
  ExamplesLoadingDescriptor? _descriptor;

  ExamplesLoader() {
    defaultFactory.add(CatalogDefaultExampleLoader.new);
    defaultFactory.add(ContentExampleLoader.new);
    defaultFactory.add(EmptyExampleLoader.new);
    defaultFactory.add(StandardExampleLoader.new);
    defaultFactory.add(UserSharedExampleLoader.new);
  }

  void setPlaygroundController(PlaygroundController value) {
    _playgroundController = value;
  }

  Future<void> load(ExamplesLoadingDescriptor descriptor) async {
    if (_descriptor == descriptor) {
      return;
    }

    _descriptor = descriptor;
    await Future.wait(
      descriptor.descriptors.map(
        (one) => loadOne(group: descriptor, one: one),
      ),
    );

    final sdk = descriptor.initialSdk;
    if (sdk != null) {
      _playgroundController!.setSdk(sdk);
    }
  }

  Future<void> loadDefaultIfAny(Sdk sdk) async {
    final group = _descriptor;
    final one = group?.lazyLoadDescriptors[sdk]?.firstOrNull;

    if (group == null || one == null) {
      return;
    }

    return loadOne(
      group: group,
      one: one,
    );
  }

  Future<void> loadOne({
    required ExamplesLoadingDescriptor group,
    required ExampleLoadingDescriptor one,
  }) async {
    final loader = defaultFactory.create(
      descriptor: one,
      exampleCache: _playgroundController!.exampleCache,
    );

    if (loader == null) {
      // TODO: Log.
      print('Cannot create example loader for $one');
      return;
    }

    final example = await loader.future;
    _playgroundController!.setExample(
      example,
      setCurrentSdk:
          example.sdk == group.initialSdk || group.initialSdk == null,
    );
  }
}
