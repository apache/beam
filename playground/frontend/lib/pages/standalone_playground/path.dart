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

import '../../constants/params.dart';
import '../../modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'page.dart';

const _basePath = '/';

/// A path to load multiple examples immediately.
///
/// This object is parsed from a URL that loads multiple specific examples,
/// most likely one per SDK.
class StandalonePlaygroundMultiplePath extends PagePath {
  final ExamplesLoadingDescriptor descriptor;

  StandalonePlaygroundMultiplePath({
    required this.descriptor,
  }) : super(
          key: StandalonePlaygroundPage.classFactoryKey,
          state: {
            kDescriptorParam: descriptor.toJson(),
          },
        );

  static StandalonePlaygroundMultiplePath? tryParse(Uri uri) {
    final descriptor =
        ExamplesLoadingDescriptorFactory.tryParseOfMultipleExamples(
      uri.queryParameters,
    );

    if (descriptor == null) {
      return null;
    }

    return StandalonePlaygroundMultiplePath(
      descriptor: _addLazyLoad(descriptor),
    );
  }

  @override
  String get location {
    return Uri(
      path: _basePath,
      queryParameters: descriptor.toJson(),
    ).toString();
  }
}

/// A path to load a single example.
///
/// This class is used to emit a URL of any currently selected example.
class StandalonePlaygroundSinglePath extends PagePath {
  final ExampleLoadingDescriptor descriptor;

  StandalonePlaygroundSinglePath({
    required this.descriptor,
    super.state,
  }) : super(
          factoryKey: StandalonePlaygroundPage.classFactoryKey,
        );

  @override
  String get location {
    final params = descriptor.toJson();

    return Uri(
      path: _basePath,
      queryParameters: params.isEmpty
          ? null
          : params.map((k, v) => MapEntry(k, v.toString())), // num, bool...
    ).toString();
  }
}

/// A path to load a single example and possibly others with lazy-load.
///
/// This object is parsed from a URL that loads a single specific example.
/// It adds instructions to lazily load default examples for other SDKs.
class StandalonePlaygroundSingleFirstPath
    extends StandalonePlaygroundSinglePath {
  final ExamplesLoadingDescriptor multipleDescriptor;

  StandalonePlaygroundSingleFirstPath({
    required super.descriptor,
    required this.multipleDescriptor,
  }) : super(
          state: {
            kDescriptorParam: multipleDescriptor.toJson(),
          },
        );

  factory StandalonePlaygroundSingleFirstPath.fromMultiple({
    required ExamplesLoadingDescriptor multipleDescriptor,
  }) {
    return StandalonePlaygroundSingleFirstPath(
      descriptor: multipleDescriptor.descriptors.first,
      multipleDescriptor: multipleDescriptor,
    );
  }

  static StandalonePlaygroundSinglePath? tryParse(Uri uri) {
    final descriptor = ExamplesLoadingDescriptorFactory.tryParseOfSingleExample(
      uri.queryParameters,
    );

    if (descriptor == null) {
      return null;
    }

    return StandalonePlaygroundSingleFirstPath.fromMultiple(
      multipleDescriptor: _addLazyLoad(descriptor),
    );
  }
}

/// A path to not load anything and show infinite loading.
///
/// Can be used for a page that is about to receive a JavaScript message
/// to load its content.
class StandalonePlaygroundWaitPath extends PagePath {
  static const _location = '/wait';

  StandalonePlaygroundWaitPath()
      : super(
          factoryKey: StandalonePlaygroundPage.classFactoryKey,
        );

  static StandalonePlaygroundWaitPath? tryParse(Uri uri) {
    return uri.path == _location ? StandalonePlaygroundWaitPath() : null;
  }

  @override
  String get location => _location;
}

/// A path to load catalog default examples.
class StandalonePlaygroundDefaultPath extends PagePath {
  static StandalonePlaygroundSinglePath parse(Uri uri) {
    final descriptor =
        ExamplesLoadingDescriptorFactory.parseOfCatalogDefaultExamples(
      uri.queryParameters,
    );

    return StandalonePlaygroundSingleFirstPath.fromMultiple(
      multipleDescriptor: _addLazyLoad(descriptor),
    );
  }
}

ExamplesLoadingDescriptor _addLazyLoad(ExamplesLoadingDescriptor descriptor) {
  return descriptor.copyWithMissingLazy(
    ExamplesLoadingDescriptorFactory.defaultLazyLoadDescriptors,
  );
}
