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
import 'package:playground/modules/output/components/output_header/output_tab.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

class OutputTabs extends StatelessWidget {
  final TabController tabController;

  const OutputTabs({Key? key, required this.tabController}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundState>(builder: (context, state, child) {
      return SizedBox(
        width: 300,
        child: TabBar(
          controller: tabController,
          tabs: <Widget>[
            OutputTab(
              name: 'Output',
              isSelected: tabController.index == 0,
              value: state.result?.output ?? '',
            ),
            OutputTab(
              name: 'Log',
              isSelected: tabController.index == 1,
              value: state.result?.log ?? '',
            ),
            OutputTab(
              name: 'Graph',
              isSelected: tabController.index == 2,
              value: '',
            ),
          ],
        ),
      );
    });
  }
}
