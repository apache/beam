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
import 'package:flutter_svg/svg.dart';
import 'package:split_view/split_view.dart';

import '../../../playground_components.dart';
import '../../constants/names.dart';
import '../../generated/assets.gen.dart';

class BeamSplitView extends StatelessWidget {
  final Axis direction;
  final List<Pan> pans;

  const BeamSplitView({
    required this.direction,
    required this.pans,
  });

  @override
  Widget build(BuildContext context) {
    return SplitView(
      gripSize: BeamSizes.splitViewSeparator,
      gripColor: ThemeColors.of(context).divider,
      gripColorActive: ThemeColors.of(context).divider,
      indicator: SvgPicture.asset(
        Assets.svg.drag,
        package: BeamNames.package,
      ),
      viewMode: direction == Axis.horizontal
          ? SplitViewMode.Horizontal
          : SplitViewMode.Vertical,
      controller: SplitViewController(
        limits: pans
            .map(
              (pan) => WeightLimit(
                min: pan.minWeight,
                max: pan.maxWeight,
              ),
            )
            .toList(growable: false),
      ),
      children: pans.map((e) => e.child).toList(growable: false),
    );
  }
}
