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

import 'package:app_state/app_state.dart';
import 'package:playground_components/playground_components.dart';

import '../../modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import '../../constants/params.dart';
import '../../utils/bool.dart';
import 'page.dart';

const _basePath = '/embedded';

/// A path to load multiple examples immediately.
///
/// This object is parsed from a URL that loads multiple specific examples,
/// most likely one per SDK.
class EmbeddedPlaygroundMultiplePath extends PagePath {
  final ExamplesLoadingDescriptor descriptor;
  final bool isEditable;

  EmbeddedPlaygroundMultiplePath({
    required this.descriptor,
    required this.isEditable,
  }) : super(
          key: EmbeddedPlaygroundPage.classFactoryKey,
          state: {
            kDescriptorParam: descriptor.toJson(),
            EmbeddedPlaygroundPage.isEditableParam: isEditable.toIntString(),
          },
        );

  static EmbeddedPlaygroundMultiplePath? tryParse(Uri uri) {
    if (uri.path != _basePath) {
      return null;
    }

    final descriptor =
        ExamplesLoadingDescriptorFactory.tryParseOfMultipleExamples(
      uri.queryParameters,
    );

    if (descriptor == null) {
      return null;
    }

    return EmbeddedPlaygroundMultiplePath(
      descriptor: _addLazyLoad(descriptor),
      isEditable: _getIsEditable(uri.queryParameters),
    );
  }

  @override
  String get location {
    final params = descriptor.toJson();

    if (!isEditable) {
      params[EmbeddedPlaygroundPage.isEditableParam] = isEditable.toIntString();
    }

    return Uri(
      path: _basePath,
      queryParameters: params,
    ).toString();
  }
}

/// A path to load a single example.
///
/// This class is used to emit a URL of any currently selected example.
class EmbeddedPlaygroundSinglePath extends PagePath {
  final ExampleLoadingDescriptor descriptor;
  final bool isEditable;

  EmbeddedPlaygroundSinglePath({
    required this.descriptor,
    required this.isEditable,
    super.state,
  }) : super(
          factoryKey: EmbeddedPlaygroundPage.classFactoryKey,
        );

  @override
  String get location {
    final params = descriptor.toJson();

    if (!isEditable) {
      params[EmbeddedPlaygroundPage.isEditableParam] = isEditable.toIntString();
    }

    return Uri(
      path: _basePath,
      queryParameters: params.map((k, v) => MapEntry(k, v.toString())),
    ).toString();
  }
}

/// A path to load a single example and possibly others with lazy-load.
///
/// This object is parsed from a URL that loads a single specific example.
/// It adds instructions to lazily load default examples for other SDKs.
class EmbeddedPlaygroundSingleFirstPath extends EmbeddedPlaygroundSinglePath {
  final ExamplesLoadingDescriptor multipleDescriptor;

  EmbeddedPlaygroundSingleFirstPath({
    required super.descriptor,
    required super.isEditable,
    required this.multipleDescriptor,
  }) : super(
          state: {
            kDescriptorParam: multipleDescriptor.toJson(),
          },
        );

  factory EmbeddedPlaygroundSingleFirstPath.fromMultiple({
    required ExamplesLoadingDescriptor multipleDescriptor,
    required bool isEditable,
  }) {
    return EmbeddedPlaygroundSingleFirstPath(
      descriptor: multipleDescriptor.descriptors.first,
      isEditable: isEditable,
      multipleDescriptor: multipleDescriptor,
    );
  }

  static EmbeddedPlaygroundSinglePath? tryParse(Uri uri) {
    if (uri.path != _basePath) {
      return null;
    }

    final descriptor = ExamplesLoadingDescriptorFactory.tryParseOfSingleExample(
      uri.queryParameters,
    );

    if (descriptor == null) {
      return null;
    }

    return EmbeddedPlaygroundSingleFirstPath.fromMultiple(
      multipleDescriptor: _addLazyLoad(descriptor),
      isEditable: _getIsEditable(uri.queryParameters),
    );
  }
}

/// A path to load an empty example for each SDK.
class EmbeddedPlaygroundEmptyPath extends PagePath {
  static EmbeddedPlaygroundSingleFirstPath? tryParse(Uri uri) {
    if (uri.path != _basePath) {
      return null;
    }

    final descriptor = ExamplesLoadingDescriptorFactory.getEmptyForSdk(
      uri.queryParameters,
    );

    return EmbeddedPlaygroundSingleFirstPath.fromMultiple(
      multipleDescriptor: _addLazyLoad(descriptor),
      isEditable: _getIsEditable(uri.queryParameters),
    );
  }
}

bool _getIsEditable(Map<String, String> params) {
  final value = params[EmbeddedPlaygroundPage.isEditableParam];
  return BoolExtension.tryParseIntString(value) ?? true;
}

ExamplesLoadingDescriptor _addLazyLoad(ExamplesLoadingDescriptor descriptor) {
  return descriptor.copyWithMissingLazy(
    ExamplesLoadingDescriptorFactory.defaultLazyLoadDescriptors,
  );
}
