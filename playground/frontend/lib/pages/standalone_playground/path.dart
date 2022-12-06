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
import 'package:flutter/widgets.dart';
import 'package:playground_components/playground_components.dart';

import '../../modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'page.dart';

const _basePath = '/';

class StandalonePlaygroundPath extends PagePath {
  final ExamplesLoadingDescriptor descriptor;
  final ExampleLoadingDescriptor? exampleDescriptor;

  StandalonePlaygroundPath({
    required this.descriptor,
    this.exampleDescriptor,
  }) : super(
          key: StandalonePlaygroundPage.classFactoryKey,
          state: {'descriptor': descriptor.toJson()},
        );

  static StandalonePlaygroundPath parse(RouteInformation ri) {
    final uri = Uri.parse(ri.location ?? '');
    final descriptor = ExamplesLoadingDescriptorFactory.fromStandaloneParams(
      uri.queryParameters,
    );

    return StandalonePlaygroundPath(
      descriptor: descriptor,
    );
  }

  @override
  String get location => _getSingleLocation() ?? _getMultipleLocation();

  String? _getSingleLocation() {
    final params = exampleDescriptor?.toJson();
    if (params == null) {
      return null;
    }

    return Uri(
      path: _basePath,
      queryParameters: params.isEmpty ? null : params,
    ).toString();
  }

  String _getMultipleLocation() {
    if (descriptor.descriptors.isEmpty) {
      return _basePath;
    }

    return Uri(
      path: _basePath,
      queryParameters: descriptor.toJson(),
    ).toString();
  }
}
