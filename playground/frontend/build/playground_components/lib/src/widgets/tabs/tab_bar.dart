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

import 'package:flutter/material.dart';
import 'package:keyed_collection_widgets/keyed_collection_widgets.dart';

import '../../constants/sizes.dart';

class BeamTabBar<K extends Object> extends StatelessWidget {
  const BeamTabBar({
    super.key,
    required this.tabs,
    this.hasPadding = false,
  });

  final bool hasPadding;
  final Map<K, Widget> tabs;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: hasPadding
          ? const EdgeInsets.symmetric(horizontal: BeamSizes.size16)
          : EdgeInsets.zero,
      child: SizedBox(
        height: BeamSizes.tabBarHeight,
        child: KeyedTabBar.withDefaultController<K>(
          isScrollable: true,
          tabs: {
            for (final key in tabs.keys) key: Tab(child: tabs[key]),
          },
        ),
      ),
    );
  }
}
