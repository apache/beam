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

import 'screen.dart';
import 'state.dart';

class TourPage extends StatefulMaterialPage<void, TourNotifier> {
  static const classFactoryKey = 'Tour';

  /// Called when navigating to the page programmatically.
  TourPage({
    required Sdk sdk,
    List<String> treeIds = const [],
  }) : super(
          key: const ValueKey(classFactoryKey),
          state: TourNotifier(
            initialSdk: sdk,
            initialTreeIds: treeIds,
          ),
          createScreen: TourScreen.new,
        );

  /// Called when re-creating the page at a navigation intent.
  factory TourPage.fromStateMap(Map state) {
    final treeIds = state['treeIds'];

    return TourPage(
      sdk: Sdk.parseOrCreate(state['sdkId']),
      treeIds: treeIds is List ? treeIds.cast<String>() : const [],
    );
  }
}
