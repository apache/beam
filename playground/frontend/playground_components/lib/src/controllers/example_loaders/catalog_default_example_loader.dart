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
import 'example_loader.dart';

class CatalogDefaultExampleLoader extends ExampleLoader {
  final CatalogDefaultExampleLoadingDescriptor descriptor;
  final ExampleCache exampleCache;

  const CatalogDefaultExampleLoader({
    required this.descriptor,
    required this.exampleCache,
  });

  @override
  Future<Example> get future async {
    if (!exampleCache.hasCatalog) {
      throw Exception('Default example requires a catalog in ExampleState');
    }

    await exampleCache.loadDefaultExamplesIfNot();
    final result = exampleCache.defaultExamplesBySdk[descriptor.sdk];

    if (result == null) {
      throw Exception('Default example not found for $descriptor');
    }

    return result;
  }
}
