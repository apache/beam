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

import '../constants/sizes.dart';

class BubbleWidget extends StatelessWidget {
  final bool isSelected;
  final VoidCallback onTap;
  final String title;

  const BubbleWidget({
    super.key,
    required this.isSelected,
    required this.onTap,
    required this.title,
  });

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);
    final ext = themeData.extension<BeamThemeExtension>()!;

    return MouseRegion(
      cursor: SystemMouseCursors.click,
      child: Padding(
        padding: const EdgeInsets.only(right: BeamSizes.size8),
        child: GestureDetector(
          onTap: onTap,
          child: Container(
            height: BeamSizes.buttonHeight,
            padding: const EdgeInsets.symmetric(horizontal: BeamSizes.size16),
            decoration: BoxDecoration(
              color: isSelected ? themeData.primaryColor : ext.borderColor,
              borderRadius: BorderRadius.circular(BeamBorderRadius.infinite),
            ),
            child: Center(
              child: Text(
                title,
                style: TextStyle(
                  color: isSelected
                      ? ext.primaryBackgroundTextColor
                      : ext.lightGreyBackgroundTextColor,
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}
