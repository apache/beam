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

import 'package:playground_components/playground_components.dart';

import '../../../../constants/params.dart';

const _sdkParam = 'sdk';

/// Creates [ExampleLoadingDescriptor]s from query parameters and normalized
/// states.
class ExamplesLoadingDescriptorFactory {
  static ExamplesLoadingDescriptor fromMap(dynamic map) {
    const empty = ExamplesLoadingDescriptor(
      descriptors: [EmptyExampleLoadingDescriptor(sdk: defaultSdk)],
    );

    if (map is! Map<String, dynamic>) {
      return empty;
    }

    final parsed = ExamplesLoadingDescriptor.tryParse(
      map,
      singleDescriptorFactory: _tryParseSingleExample,
    );

    return parsed ?? empty;
  }

  /// Tries to parse [params] into an [ExamplesLoadingDescriptor] containing
  /// multiple examples to load at the same time.
  ///
  /// ?examples=[{"sdk":"go","path":"..."},...]
  /// &sdk=go
  static ExamplesLoadingDescriptor? tryParseOfMultipleExamples(
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

    // The order does not matter because parsing must not be ambiguous.
    return CatalogDefaultExampleLoadingDescriptor.tryParse(map) ??
        ContentExampleLoadingDescriptor.tryParse(map) ??
        EmptyExampleLoadingDescriptor.tryParse(map) ??
        HttpExampleLoadingDescriptor.tryParse(map) ??
        StandardExampleLoadingDescriptor.tryParse(map) ??
        UserSharedExampleLoadingDescriptor.tryParse(map);
  }

  /// Tries to parse [params] into a single [ExampleLoadingDescriptor]
  /// and to return an [ExamplesLoadingDescriptor] containing only it.
  ///
  /// ?path=... | shared=... | url=... etc.
  /// &sdk=go
  static ExamplesLoadingDescriptor? tryParseOfSingleExample(
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

  /// Creates an [ExamplesLoadingDescriptor] that loads the default example
  /// for each SDK.
  ///
  /// Optional ?sdk=...
  static ExamplesLoadingDescriptor parseOfCatalogDefaultExamples(
    Map<String, dynamic> params,
  ) {
    final sdk = Sdk.tryParse(params[_sdkParam]) ?? defaultSdk;

    return ExamplesLoadingDescriptor(
      descriptors: [
        CatalogDefaultExampleLoadingDescriptor(sdk: sdk),
      ],
    );
  }

  /// Creates an [ExamplesLoadingDescriptor] that loads an empty snippet
  /// for each SDK.
  ///
  /// Optional ?sdk=...
  static ExamplesLoadingDescriptor getEmptyForSdk(
    Map<String, dynamic> params,
  ) {
    return ExamplesLoadingDescriptor(
      descriptors: [
        EmptyExampleLoadingDescriptor(
          sdk: Sdk.tryParse(params[_sdkParam]) ?? defaultSdk,
        ),
      ],
    );
  }

  /// Empty [ExampleLoadingDescriptor]s for each of the known SDKs.
  static Map<Sdk, List<ExampleLoadingDescriptor>> get emptyLazyLoadDescriptors {
    return {
      for (final sdk in Sdk.known)
        sdk: [EmptyExampleLoadingDescriptor(sdk: sdk)],
    };
  }

  /// Default catalog [ExampleLoadingDescriptor]s for each of the known SDKs.
  static Map<Sdk, List<ExampleLoadingDescriptor>>
      get defaultLazyLoadDescriptors {
    return {
      for (final sdk in Sdk.known)
        sdk: [CatalogDefaultExampleLoadingDescriptor(sdk: sdk)],
    };
  }
}
