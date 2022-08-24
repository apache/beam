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
  static ExamplesLoadingDescriptor fromUriParts({
    required String path,
    required Map<String, dynamic> params,
  }) {
    return _tryParseSingleExample(params) ??
        _tryParseCatalogDefaultExample() ??
        _oneEmptyWithDefaultSdk;
  }

  static ExamplesLoadingDescriptor? _tryParseSingleExample(
    Map<String, dynamic> params,
  ) {
    final token = params[kExampleParam];
    if (token is! String) {
      return null;
    }

    return ExamplesLoadingDescriptor(
      descriptors: [_parseSingleExample(token)],
    );
  }

  static ExamplesLoadingDescriptor? _tryParseCatalogDefaultExample() {
    if (isEmbedded()) {
      return null;
    }

    return const ExamplesLoadingDescriptor(
      descriptors: [CatalogDefaultExampleLoadingDescriptor(sdk: SDK.java)],
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

  static ExamplesLoadingDescriptor get _oneEmptyWithDefaultSdk {
    return const ExamplesLoadingDescriptor(
      descriptors: [EmptyExampleLoadingDescriptor(sdk: SDK.java)],
    );
  }
}
