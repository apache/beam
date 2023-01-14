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
import '../../models/example_loading_descriptors/catalog_default_example_loading_descriptor.dart';
import '../../models/example_loading_descriptors/example_loading_descriptor.dart';
import '../../models/example_loading_descriptors/standard_example_loading_descriptor.dart';
import '../../models/sdk.dart';
import 'example_loader.dart';

/// The [ExampleLoader] for [CatalogDefaultExampleLoadingDescriptor].
///
/// Loads the default example for the [sdk].
class CatalogDefaultExampleLoader extends ExampleLoader {
  @override
  ExampleLoadingDescriptor get descriptor =>
      _standardDescriptor ?? _defaultDescriptor;

  final CatalogDefaultExampleLoadingDescriptor _defaultDescriptor;
  StandardExampleLoadingDescriptor? _standardDescriptor;

  final ExampleCache exampleCache;

  CatalogDefaultExampleLoader({
    required CatalogDefaultExampleLoadingDescriptor descriptor,
    required this.exampleCache,
  }) : _defaultDescriptor = descriptor;

  @override
  Sdk get sdk => _defaultDescriptor.sdk;

  @override
  Future<Example> get future async {
    final result =
        await exampleCache.getDefaultExampleBySdk(_defaultDescriptor.sdk);

    if (result == null) {
      throw Exception('Default example not found for $descriptor');
    }

    _standardDescriptor = StandardExampleLoadingDescriptor(
      path: result.path,
      sdk: result.sdk,
      viewOptions: _defaultDescriptor.viewOptions,
    );

    return result;
  }
}
