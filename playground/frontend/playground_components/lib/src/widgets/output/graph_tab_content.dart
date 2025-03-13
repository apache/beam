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

import '../../constants/sizes.dart';
import '../../controllers/playground_controller.dart';
import '../../enums/unread_entry.dart';
import '../scroll_2d.dart';
import '../unread/clearer.dart';
import 'graph/graph_builder/graph_builder.dart';
import 'graph/graph_builder/painters/graph_painter.dart';
import 'graph/painter.dart';

class GraphTabContent extends StatefulWidget {
  const GraphTabContent({
    super.key,
    required this.direction,
    required this.playgroundController,
  });

  final Axis direction;
  final PlaygroundController playgroundController;

  @override
  State<GraphTabContent> createState() => _GraphTabContentState();
}

class _GraphTabContentState extends State<GraphTabContent> {
  GraphPainter? _graphPainter;
  String _lastGraph = '';

  @override
  void initState() {
    super.initState();
    widget.playgroundController.addListener(_updateGraphPainterIfNeed);
    _updateGraphPainter();
  }

  void _updateGraphPainterIfNeed() {
    final graph = widget.playgroundController.codeRunner.result?.graph ?? '';
    if (graph == _lastGraph) {
      return;
    }

    _updateGraphPainter();
  }

  void _updateGraphPainter() {
    final codeRunner = widget.playgroundController.codeRunner;
    final result = codeRunner.result;
    final graph = result?.graph ?? '';
    final sdk = result?.sdk;

    _lastGraph = graph;
    _graphPainter = graph.isEmpty || sdk == null
        ? null
        : GraphBuilder.parseDot(graph, sdk)?.getPainter(widget.direction);
  }

  @override
  void dispose() {
    widget.playgroundController.removeListener(_updateGraphPainterIfNeed);
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final graphPainter = _graphPainter;
    if (graphPainter == null) {
      return Container();
    }

    return UnreadClearer(
      controller: widget.playgroundController.codeRunner.unreadController,
      unreadKey: UnreadEntryEnum.graph,
      child: AnimatedBuilder(
        animation: widget.playgroundController.codeRunner,
        builder: (context, child) => Padding(
          padding: const EdgeInsets.all(BeamSizes.size16),
          child: Scroll2DWidget(
            child: ClipRRect(
              child: CustomPaint(
                painter: GraphCustomPainter(graph: graphPainter),
                size: graphPainter.getSize(),
              ),
            ),
          ),
        ),
      ),
    );
  }
}
