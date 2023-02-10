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

class FractionProgressIndicator extends StatelessWidget {
  final double progress;

  const FractionProgressIndicator({
    required this.progress,
  });

  static const _diameter = 8.5;
  static const _thickness = 3.8;

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.only(
        left: BeamSizes.size7,
        right: BeamSizes.size10,
      ),
      height: _diameter,
      width: _diameter,
      child: CircularProgressIndicator(
        strokeWidth: _thickness,
        color: BeamColors.green,
        backgroundColor: Theme.of(context)
            .extension<BeamThemeExtension>()!
            .unselectedProgressColor,
        value: progress,
      ),
    );
  }
}
