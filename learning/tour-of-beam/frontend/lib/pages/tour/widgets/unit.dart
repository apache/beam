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

import '../../../generated/assets.gen.dart';
import '../../../models/unit.dart';
import '../controllers/content_tree.dart';
import 'tour_progress_indicator.dart';

class UnitWidget extends StatelessWidget {
  final UnitModel unit;
  final ContentTreeController contentTreeController;

  const UnitWidget({
    required this.unit,
    required this.contentTreeController,
  });

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: contentTreeController,
      builder: (context, child) {
        final isSelected = contentTreeController.currentNode?.id == unit.id;

        return ClickableWidget(
          onTap: () => contentTreeController.openNode(unit),
          child: Container(
            decoration: BoxDecoration(
              color: isSelected ? Theme.of(context).selectedRowColor : null,
              borderRadius: BorderRadius.circular(BeamSizes.size3),
            ),
            padding: const EdgeInsets.symmetric(vertical: BeamSizes.size10),
            child: Row(
              children: [
                TourProgressIndicator(
                  assetPath: Assets.svg.unitProgress0,
                  isSelected: isSelected,
                ),
                Expanded(
                  child: Text(unit.title),
                ),
              ],
            ),
          ),
        );
      },
    );
  }
}
