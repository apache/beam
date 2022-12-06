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
import '../../constants/params.dart';
import 'page.dart';

class EmbeddedPlaygroundPath extends PagePath {
  final ExamplesLoadingDescriptor descriptor;
  final bool isEditable;

  static const _location = '/embedded';

  EmbeddedPlaygroundPath({
    required this.descriptor,
    required this.isEditable,
  }) : super(
          key: EmbeddedPlaygroundPage.classFactoryKey,
          state: {'descriptor': descriptor.toJson()},
        );

  static EmbeddedPlaygroundPath? tryParse(RouteInformation ri) {
    final uri = Uri.parse(ri.location ?? '');
    if (uri.path != _location) {
      return null;
    }

    final descriptor = ExamplesLoadingDescriptorFactory.fromEmbeddedParams(
      uri.queryParameters,
    );

    return EmbeddedPlaygroundPath(
      descriptor: descriptor,
      isEditable: uri.queryParameters[kIsEditableParam] == '1',
    );
  }
}
