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
import 'package:keyed_collection_widgets/keyed_collection_widgets.dart';

import '../../controllers/playground_controller.dart';
import '../../enums/output_tab.dart';
import '../tabs/tab_bar.dart';
import 'graph_tab.dart';
import 'graph_tab_content.dart';
import 'result_tab.dart';
import 'result_tab_content.dart';

class OutputWidget extends StatelessWidget {
  final PlaygroundController playgroundController;
  final Widget? trailing;
  final Axis graphDirection;

  const OutputWidget({
    required this.playgroundController,
    required this.graphDirection,
    this.trailing,
  });

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: playgroundController,
      builder: (context, child) {
        final keys = [...OutputTabEnum.values];

        if (!playgroundController.graphAvailable) {
          keys.remove(OutputTabEnum.graph);
        }

        return DefaultKeyedTabController<OutputTabEnum>.fromKeys(
          keys: keys,
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Expanded(
                    child: BeamTabBar(
                      hasPadding: true,
                      tabs: UnmodifiableOutputTabEnumMap(
                        result: ResultTab(
                          playgroundController: playgroundController,
                        ),
                        graph: GraphTab(
                          playgroundController: playgroundController,
                        ),
                      ),
                    ),
                  ),
                  if (trailing != null) trailing!,
                ],
              ),
              Expanded(
                child: KeyedTabBarView.withDefaultController(
                  children: UnmodifiableOutputTabEnumMap(
                    result: ResultTabContent(
                      playgroundController: playgroundController,
                    ),
                    graph: GraphTabContent(
                      direction: graphDirection,
                      playgroundController: playgroundController,
                    ),
                  ),
                ),
              ),
            ],
          ),
        );
      },
    );
  }
}
