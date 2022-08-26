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
import 'package:playground/modules/graph/graph_builder/painters/graph_painter.dart';
import 'package:playground/modules/output/components/graph.dart';
import 'package:playground/modules/output/components/output_result.dart';
import 'package:playground/modules/output/models/output_placement.dart';
import 'package:playground/modules/output/models/output_placement_state.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

class OutputArea extends StatelessWidget {
  final TabController tabController;
  final bool showGraph;

  const OutputArea({
    Key? key,
    required this.tabController,
    required this.showGraph,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      color: Theme.of(context).backgroundColor,
      child: Consumer2<PlaygroundState, OutputPlacementState>(
        builder: (context, playgroundState, placementState, child) {
          final sdk = playgroundState.sdk;

          return TabBarView(
            controller: tabController,
            physics: const NeverScrollableScrollPhysics(),
            children: <Widget>[
              OutputResult(
                text: playgroundState.outputResult,
                isSelected: tabController.index == 0,
              ),
              if (showGraph)
                sdk == null
                    ? Container()
                    : GraphTab(
                        graph: playgroundState.result?.graph ?? '',
                        sdk: sdk,
                        direction: _getGraphDirection(placementState.placement),
                      ),
            ],
          );
        },
      ),
    );
  }

  GraphDirection _getGraphDirection(OutputPlacement placement) {
    return placement == OutputPlacement.bottom
        ? GraphDirection.horizontal
        : GraphDirection.vertical;
  }
}
