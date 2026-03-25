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

import '../constants/colors.dart';
import '../constants/sizes.dart';
import '../enums/complexity.dart';

class ComplexityWidget extends StatelessWidget {
  final Complexity complexity;

  const ComplexityWidget({required this.complexity});

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      behavior: HitTestBehavior.translucent,
      onTap: () {},
      child: SizedBox.square(
        dimension: 24,
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: _dots[complexity]!,
        ),
      ),
    );
  }

  static const Map<Complexity, List<Widget>> _dots = {
    Complexity.basic: [_Dot.green, _Dot.grey, _Dot.grey],
    Complexity.medium: [_Dot.orange, _Dot.orange, _Dot.grey],
    Complexity.advanced: [_Dot.red, _Dot.red, _Dot.red],
  };
}

class _Dot extends StatelessWidget {
  final Color color;

  const _Dot({required this.color});

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.only(left: 1),
      width: BeamSizes.size4,
      height: BeamSizes.size4,
      decoration: BoxDecoration(
        shape: BoxShape.circle,
        color: color,
      ),
    );
  }

  static const grey = _Dot(color: BeamColors.grey2);
  static const green = _Dot(color: BeamColors.green);
  static const orange = _Dot(color: BeamColors.orange);
  static const red = _Dot(color: BeamColors.red);
}
