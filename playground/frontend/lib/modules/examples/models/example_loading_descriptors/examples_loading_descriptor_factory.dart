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

import 'dart:convert';

import 'package:playground/constants/params.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/catalog_default_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/empty_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/standard_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/user_shared_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_token_type.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class ExamplesLoadingDescriptorFactory {
  static const _defaultSdk = SDK.java;

  static ExamplesLoadingDescriptor fromUriParts({
    required String path,
    required Map<String, dynamic> params,
  }) {
    return _tryParseOfMultipleExamples(params) ??
        _tryParseOfSingleExample(params) ??
        _tryParseOfCatalogDefaultExamples() ??
        _getEmpty(params);
  }

  /// ?examples=[{"sdk":"go","example":"..."},...]
  /// &sdk=go
  static ExamplesLoadingDescriptor? _tryParseOfMultipleExamples(
    Map<String, dynamic> params,
  ) {
    try {
      final list = jsonDecode(params[kExamplesParam] ?? '');
      if (list is! List) {
        return null;
      }

      return ExamplesLoadingDescriptor(
        descriptors: _parseMultipleInstantExamples(list),
        lazyLoadDescriptors: _getLazyLoadDescriptors(),
        initialSdk: SDK.tryParse(params[kSdkParam]),
      );
    } catch (ex) {
      return null;
    }
  }

  static List<ExampleLoadingDescriptor> _parseMultipleInstantExamples(
    List list,
  ) {
    final result = <ExampleLoadingDescriptor>[];

    for (final map in list) {
      final parsed = _tryParseSingleMap(map);
      if (parsed != null) {
        result.add(parsed);
      }
    }

    return result.isEmpty
        ? const [EmptyExampleLoadingDescriptor(sdk: _defaultSdk)]
        : result;
  }

  static ExampleLoadingDescriptor? _tryParseSingleMap(Object? map) {
    if (map is! Map<String, dynamic>) {
      return null;
    }

    return _tryParseSingleExample(map);
  }

  /// ?example=...
  static ExamplesLoadingDescriptor? _tryParseOfSingleExample(
    Map<String, dynamic> params,
  ) {
    final single = _tryParseSingleExample(params);
    if (single == null) {
      return null;
    }

    return ExamplesLoadingDescriptor(
      descriptors: [single],
      lazyLoadDescriptors: _getLazyLoadDescriptors(),
    );
  }

  static ExampleLoadingDescriptor? _tryParseSingleExample(
      Map<String, dynamic> params) {
    final token = params[kExampleParam];
    if (token is! String) {
      return null;
    }

    return _parseSingleExample(token);
  }

  static ExamplesLoadingDescriptor? _tryParseOfCatalogDefaultExamples() {
    if (isEmbedded()) {
      return null;
    }

    return ExamplesLoadingDescriptor(
      descriptors: [
        const CatalogDefaultExampleLoadingDescriptor(sdk: _defaultSdk),
      ],
      lazyLoadDescriptors: _getLazyLoadDescriptors(),
    );
  }

  static ExampleLoadingDescriptor _parseSingleExample(String token) {
    final tokenType = ExampleTokenType.fromToken(token);

    switch (tokenType) {
      case ExampleTokenType.standard:
        return StandardExampleLoadingDescriptor(path: token);

      case ExampleTokenType.userShared:
        return UserSharedExampleLoadingDescriptor(snippetId: token);
    }
  }

  /// Optional ?sdk=...
  static ExamplesLoadingDescriptor _getEmpty(
    Map<String, dynamic> params,
  ) {
    return ExamplesLoadingDescriptor(
      descriptors: [
        EmptyExampleLoadingDescriptor(
          sdk: SDK.tryParse(params[kSdkParam]) ?? _defaultSdk,
        ),
      ],
      lazyLoadDescriptors: _emptyLazyLoadDescriptors,
    );
  }

  static Map<SDK, List<ExampleLoadingDescriptor>> _getLazyLoadDescriptors() {
    if (isEmbedded()) {
      return _emptyLazyLoadDescriptors;
    }

    return _defaultLazyLoadDescriptors;
  }

  static Map<SDK, List<ExampleLoadingDescriptor>>
      get _emptyLazyLoadDescriptors {
    return {
      for (final sdk in SDK.values)
        sdk: [EmptyExampleLoadingDescriptor(sdk: sdk)]
    };
  }

  static Map<SDK, List<ExampleLoadingDescriptor>>
      get _defaultLazyLoadDescriptors {
    return {
      for (final sdk in SDK.values)
        sdk: [CatalogDefaultExampleLoadingDescriptor(sdk: sdk)]
    };
  }
}
