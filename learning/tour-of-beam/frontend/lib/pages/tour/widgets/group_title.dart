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
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../../cache/unit_progress.dart';
import '../../../models/group.dart';
import '../../../models/node.dart';
import 'binary_progress.dart';
import 'fraction_progress.dart';

class GroupTitleWidget extends StatelessWidget {
  final GroupModel group;
  final VoidCallback onTap;

  const GroupTitleWidget({
    required this.group,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return ClickableWidget(
      onTap: onTap,
      child: Row(
        children: [
          _GroupProgressIndicator(group: group),
          Expanded(
            child: Text(
              group.title,
              style: Theme.of(context).textTheme.headlineMedium,
            ),
          ),
        ],
      ),
    );
  }
}

class _GroupProgressIndicator extends StatelessWidget {
  final GroupModel group;
  const _GroupProgressIndicator({required this.group});

  @override
  Widget build(BuildContext context) {
    final unitProgressCache = GetIt.instance.get<UnitProgressCache>();

    return AnimatedBuilder(
      animation: unitProgressCache,
      builder: (context, child) {
        final progress = _getGroupProgress(
          group.nodes,
          unitProgressCache.getCompletedUnits(),
        );

        if (progress == 1) {
          return const BinaryProgressIndicator(
            isCompleted: true,
            isSelected: false,
          );
        }

        return FractionProgressIndicator(progress: progress);
      },
    );
  }

  double _getGroupProgress(
    List<NodeModel> groupNodes,
    Set<String> completedUnits,
  ) {
    int completed = 0;
    int total = 0;

    void countNodes(List<NodeModel> nodes) {
      for (final node in nodes) {
        if (node is GroupModel) {
          countNodes(node.nodes);
        } else {
          total += 1;
          if (completedUnits.contains(node.id)) {
            completed += 1;
          }
        }
      }
    }

    countNodes(groupNodes);
    return completed / total;
  }
}
