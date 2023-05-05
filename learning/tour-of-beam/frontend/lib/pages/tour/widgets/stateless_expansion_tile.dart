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
import 'package:playground_components/playground_components.dart';

import '../../../components/expansion_tile_wrapper.dart';

class StatelessExpansionTile extends StatelessWidget {
  final bool isExpanded;
  final ValueChanged<bool>? onExpansionChanged;
  final Widget title;
  final Widget child;

  const StatelessExpansionTile({
    required this.isExpanded,
    required this.onExpansionChanged,
    required this.title,
    required this.child,
  });

  @override
  Widget build(BuildContext context) {
    return ExpansionTileWrapper(
      ExpansionTile(
        key: ValueKey(isExpanded),
        initiallyExpanded: isExpanded,
        tilePadding: EdgeInsets.zero,
        onExpansionChanged: onExpansionChanged,
        title: title,
        childrenPadding: const EdgeInsets.only(
          left: BeamSizes.size12,
        ),
        children: [child],
      ),
    );
  }
}
