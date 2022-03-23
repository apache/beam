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
import 'package:playground/modules/output/components/output_area.dart';
import 'package:playground/modules/output/components/output_header/output_header.dart';

const kTabsCount = 3;

class Output extends StatefulWidget {
  final bool isEmbedded;
  final bool showGraph;

  const Output({Key? key, required this.isEmbedded, required this.showGraph})
      : super(key: key);

  @override
  State<Output> createState() => _OutputState();
}

class _OutputState extends State<Output> with SingleTickerProviderStateMixin {
  late final TabController tabController;
  int selectedTab = 0;

  @override
  void initState() {
    final tabsCount = widget.showGraph ? kTabsCount : kTabsCount - 1;
    tabController = TabController(vsync: this, length: tabsCount);
    tabController.addListener(onTabChange);
    super.initState();
  }

  @override
  void dispose() {
    tabController.removeListener(onTabChange);
    tabController.dispose();
    super.dispose();
  }

  onTabChange() {
    setState(() {
      selectedTab = tabController.index;
    });
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        OutputHeader(
          tabController: tabController,
          showOutputPlacements: !widget.isEmbedded,
          showGraph: widget.showGraph,
        ),
        Expanded(
          child: OutputArea(
            tabController: tabController,
            showGraph: widget.showGraph,
          ),
        ),
      ],
    );
  }
}
