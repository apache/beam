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

import 'package:flutter/foundation.dart';
import 'package:playground_components/playground_components.dart';

import '../../../../constants/params.dart';

class ExamplesLoadingDescriptorFactory {
  static ExamplesLoadingDescriptor fromEmbeddedParams(
    Map<String, dynamic> params,
  ) {
    return (_tryParseOfMultipleExamples(params) ??
            _tryParseOfSingleExample(params) ??
            _getEmpty(params))
        .copyWithMissingLazy(_emptyLazyLoadDescriptors);
  }

  static ExamplesLoadingDescriptor fromStandaloneParams(
    Map<String, dynamic> params,
  ) {
    return (_tryParseOfMultipleExamples(params) ??
            _tryParseOfSingleExample(params) ??
            _parseOfCatalogDefaultExamples(params))
        .copyWithMissingLazy(defaultLazyLoadDescriptors);
  }

  static ExamplesLoadingDescriptor fromMap(dynamic map) {
    const empty = ExamplesLoadingDescriptor(
      descriptors: [EmptyExampleLoadingDescriptor(sdk: defaultSdk)],
    );

    if (map is! Map<String, dynamic>) {
      return empty;
    }

    return ExamplesLoadingDescriptor.tryParse(
          map,
          singleDescriptorFactory: _tryParseSingleExample,
        ) ??
        empty;
  }

  /// ?examples=[{"sdk":"go","path":"..."},...]
  /// &sdk=go
  static ExamplesLoadingDescriptor? _tryParseOfMultipleExamples(
    Map<String, dynamic> params,
  ) {
    return ExamplesLoadingDescriptor.tryParse(
      params,
      singleDescriptorFactory: _tryParseSingleExample,
    );
  }

  static ExampleLoadingDescriptor? _tryParseSingleExample(Object? map) {
    if (map is! Map<String, dynamic>) {
      return null;
    }

    // The order does not matter.
    return CatalogDefaultExampleLoadingDescriptor.tryParse(map) ??
        ContentExampleLoadingDescriptor.tryParse(map) ??
        EmptyExampleLoadingDescriptor.tryParse(map) ??
        HttpExampleLoadingDescriptor.tryParse(map) ??
        StandardExampleLoadingDescriptor.tryParse(map) ??
        UserSharedExampleLoadingDescriptor.tryParse(map);
  }

  /// ?path=... | shared=... | url=... etc.
  /// &sdk=go
  static ExamplesLoadingDescriptor? _tryParseOfSingleExample(
    Map<String, dynamic> params,
  ) {
    final single = _tryParseSingleExample(params);
    if (single == null) {
      return null;
    }

    return ExamplesLoadingDescriptor(
      descriptors: [single],
    );
  }

  static ExamplesLoadingDescriptor _parseOfCatalogDefaultExamples(
    Map<String, dynamic> params,
  ) {
    final sdk = Sdk.tryParse(params[kSdkParam]) ?? defaultSdk;

    return ExamplesLoadingDescriptor(
      descriptors: [
        CatalogDefaultExampleLoadingDescriptor(sdk: sdk),
      ],
    );
  }

  /// Optional ?sdk=...
  static ExamplesLoadingDescriptor _getEmpty(
    Map<String, dynamic> params,
  ) {
    return ExamplesLoadingDescriptor(
      descriptors: [
        EmptyExampleLoadingDescriptor(
          sdk: Sdk.tryParse(params[kSdkParam]) ?? defaultSdk,
        ),
      ],
    );
  }

  static Map<Sdk, List<ExampleLoadingDescriptor>>
      get _emptyLazyLoadDescriptors {
    return {
      for (final sdk in Sdk.known)
        sdk: [EmptyExampleLoadingDescriptor(sdk: sdk)],
    };
  }

  @visibleForTesting
  static Map<Sdk, List<ExampleLoadingDescriptor>>
      get defaultLazyLoadDescriptors {
    return {
      for (final sdk in Sdk.known)
        sdk: [CatalogDefaultExampleLoadingDescriptor(sdk: sdk)],
    };
  }
}
