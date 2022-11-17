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
import '../tab_header.dart';
import 'output_area.dart';
import 'output_tabs.dart';

const kTabsCount = 2;

class OutputWidget extends StatefulWidget {
  final PlaygroundController playgroundController;
  final Widget? trailing;
  final Axis graphDirection;

  OutputWidget({
    required this.playgroundController,
    required this.graphDirection,
    this.trailing,
  }) : super(
          key: ValueKey(
            '${playgroundController.sdk}_${playgroundController.selectedExample?.path}',
          ),
        );

  @override
  State<OutputWidget> createState() => _OutputWidgetState();
}

class _OutputWidgetState extends State<OutputWidget>
    with SingleTickerProviderStateMixin {
  late final TabController tabController;
  int selectedTab = 0;

  @override
  void initState() {
    final tabsCount = widget.playgroundController.graphAvailable
        ? kTabsCount
        : kTabsCount - 1;
    tabController = TabController(vsync: this, length: tabsCount);
    tabController.addListener(_onTabChange);
    super.initState();
  }

  @override
  void dispose() {
    tabController.removeListener(_onTabChange);
    tabController.dispose();
    super.dispose();
  }

  void _onTabChange() {
    setState(() {
      selectedTab = tabController.index;
    });
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            TabHeader(
              tabController: tabController,
              tabsWidget: OutputTabs(
                playgroundController: widget.playgroundController,
                tabController: tabController,
              ),
            ),
            if (widget.trailing != null) widget.trailing!,
          ],
        ),
        Expanded(
          child: OutputArea(
            playgroundController: widget.playgroundController,
            tabController: tabController,
            graphDirection: widget.graphDirection,
          ),
        ),
      ],
    );
  }
}
