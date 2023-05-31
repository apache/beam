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

import '../assets/assets.gen.dart';
import '../playground_components.dart';

class DragHandle extends StatelessWidget {
  final Axis direction;

  const DragHandle({
    required this.direction,
    super.key,
  });

  @override
  Widget build(BuildContext context) {
    // TODO: Use a single file and just rotate it if needed.
    //  Currently a rotated widget gets blurred in HTML renderer. Find a fix.
    return SvgPicture.asset(
      direction == Axis.horizontal
          ? Assets.svg.dragHorizontal
          : Assets.svg.dragVertical,
      package: PlaygroundComponents.packageName,
    );
  }
}
