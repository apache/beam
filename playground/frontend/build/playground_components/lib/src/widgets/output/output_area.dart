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

import '../../controllers/playground_controller.dart';
import '../../models/outputs.dart';
import 'graph/graph.dart';
import 'output_result.dart';

class OutputArea extends StatelessWidget {
  final PlaygroundController playgroundController;
  final TabController tabController;
  final Axis graphDirection;

  const OutputArea({
    super.key,
    required this.playgroundController,
    required this.tabController,
    required this.graphDirection,
  });

  String _getResultOutput() {
    final outputType =
        playgroundController.outputTypeController.outputFilterType;
    switch (outputType) {
      case OutputType.log:
        return playgroundController.codeRunner.resultLog;
      case OutputType.output:
        return playgroundController.codeRunner.resultOutput;
      case OutputType.all:
        return playgroundController.codeRunner.resultLogOutput;
    }
  }

  @override
  Widget build(BuildContext context) {
    final sdk = playgroundController.sdk;

    return AnimatedBuilder(
      animation: playgroundController.outputTypeController,
      builder: (context, child) => ColoredBox(
        color: Theme.of(context).backgroundColor,
        child: TabBarView(
          controller: tabController,
          physics: const NeverScrollableScrollPhysics(),
          children: <Widget>[
            OutputResult(
              text: _getResultOutput(),
              isSelected: tabController.index == 0,
            ),
            if (playgroundController.graphAvailable)
              sdk == null
                  ? Container()
                  : GraphTab(
                      graph:
                          playgroundController.codeRunner.result?.graph ?? '',
                      sdk: sdk,
                      direction: graphDirection,
                    ),
          ],
        ),
      ),
    );
  }
}
