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
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'package:playground_components/playground_components.dart';

void main() {
  final lazy = ExamplesLoadingDescriptorFactory.defaultLazyLoadDescriptors;

  group('ExamplesLoadingDescriptorFactory', () {
    test('defaultLazyLoadDescriptors', () {
      final expected = {
        Sdk.go: [
          const CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.go,
          )
        ],
        Sdk.java: [
          const CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.java,
          )
        ],
        Sdk.python: [
          const CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.python,
          )
        ],
        Sdk.scio: [
          const CatalogDefaultExampleLoadingDescriptor(
            sdk: Sdk.scio,
          )
        ],
      };

      expect(lazy, expected);
    });

    test('emptyLazyLoadDescriptors', () {
      final expected = {
        Sdk.go: [
          const EmptyExampleLoadingDescriptor(
            sdk: Sdk.go,
          )
        ],
        Sdk.java: [
          const EmptyExampleLoadingDescriptor(
            sdk: Sdk.java,
          )
        ],
        Sdk.python: [
          const EmptyExampleLoadingDescriptor(
            sdk: Sdk.python,
          )
        ],
        Sdk.scio: [
          const EmptyExampleLoadingDescriptor(
            sdk: Sdk.scio,
          )
        ],
      };

      expect(ExamplesLoadingDescriptorFactory.emptyLazyLoadDescriptors, expected,);
    });
  });
}
