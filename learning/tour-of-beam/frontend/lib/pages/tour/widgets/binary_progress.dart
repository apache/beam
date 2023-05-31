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
import 'package:playground_components/playground_components.dart';

import '../../../assets/assets.gen.dart';

class BinaryProgressIndicator extends StatelessWidget {
  final bool isCompleted;
  final bool isSelected;

  const BinaryProgressIndicator({
    required this.isCompleted,
    required this.isSelected,
  });

  @override
  Widget build(BuildContext context) {
    final ext = Theme.of(context).extension<BeamThemeExtension>()!;
    final Color color;
    if (isCompleted) {
      color = BeamColors.green;
    } else if (isSelected) {
      color = ext.selectedProgressColor;
    } else {
      color = ext.unselectedProgressColor;
    }

    return Padding(
      padding: const EdgeInsets.only(
        left: BeamSizes.size4,
        right: BeamSizes.size8,
      ),
      child: SvgPicture.asset(
        isCompleted ? Assets.svg.unitProgress100 : Assets.svg.unitProgress0,
        colorFilter: ColorFilter.mode(
          color,
          BlendMode.srcIn,
        ),
      ),
    );
  }
}
