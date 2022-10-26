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

import 'graph/graph.dart';
import 'output_result.dart';

class OutputArea extends StatelessWidget {
  final PlaygroundController playgroundController;
  final TabController tabController;
  final Axis graphDirection;

  const OutputArea({
    Key? key,
    required this.playgroundController,
    required this.tabController,
    required this.graphDirection,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final sdk = playgroundController.sdk;

    return Container(
      color: Theme.of(context).backgroundColor,
      child: TabBarView(
        controller: tabController,
        physics: const NeverScrollableScrollPhysics(),
        children: <Widget>[
          OutputResult(
            text: playgroundController.outputResult,
            isSelected: tabController.index == 0,
          ),
          if (playgroundController.graphAvailable)
            sdk == null
                ? Container()
                : GraphTab(
                    graph: playgroundController.result?.graph ?? '',
                    sdk: sdk,
                    direction: graphDirection,
                  ),
        ],
      ),
    );
  }
}
