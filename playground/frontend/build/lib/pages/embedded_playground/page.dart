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
import 'package:flutter/foundation.dart';
import 'package:playground/constants/params.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'package:playground/utils/bool.dart';
import 'package:playground_components/playground_components.dart';

import 'screen.dart';
import 'state.dart';

class EmbeddedPlaygroundPage
    extends StatefulMaterialPage<void, EmbeddedPlaygroundNotifier> {
  static const classFactoryKey = 'EmbeddedPlayground';

  static const isEditableParam = 'editable';

  /// Called when navigating to the page programmatically.
  EmbeddedPlaygroundPage({
    required ExamplesLoadingDescriptor descriptor,
    required bool isEditable,
  }) : super(
          key: const ValueKey(classFactoryKey),
          state: EmbeddedPlaygroundNotifier(
            initialDescriptor: descriptor,
            isEditable: isEditable,
          ),
          createScreen: EmbeddedPlaygroundScreen.new,
        );

  /// Called when re-creating the page at a navigation intent.
  factory EmbeddedPlaygroundPage.fromStateMap(Map map) {
    return EmbeddedPlaygroundPage(
      descriptor: _descriptorFromMap(map[kDescriptorParam]),
      isEditable: BoolExtension.tryParseIntString(map[isEditableParam]) ?? true,
    );
  }

  static ExamplesLoadingDescriptor _descriptorFromMap(Map? map) {
    if (map == null) {
      return ExamplesLoadingDescriptor.empty;
    }

    return ExamplesLoadingDescriptorFactory.fromMap(map).copyWithMissingLazy(
      ExamplesLoadingDescriptorFactory.emptyLazyLoadDescriptors,
    );
  }
}
