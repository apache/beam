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

import 'graph_builder/canvas_drawer.dart';
import 'graph_builder/graph_builder.dart';
import 'graph_builder/painters/graph_painter.dart';

class GraphCustomPainter extends CustomPainter {
  final GraphPainter graph;

  GraphCustomPainter({required this.graph});

  @override
  void paint(Canvas canvas, Size size) {
    graph.paint(CanvasDrawer(canvas));
  }

  @override
  bool shouldRepaint(covariant CustomPainter oldDelegate) {
    return true;
  }
}

class GraphTab extends StatefulWidget {
  final String graph;
  final Sdk sdk;
  final Axis direction;

  const GraphTab({
    super.key,
    required this.graph,
    required this.sdk,
    required this.direction,
  });

  @override
  State<GraphTab> createState() => _GraphTabState();
}

class _GraphTabState extends State<GraphTab> {
  GraphPainter? graphPainter;

  @override
  void initState() {
    if (widget.graph.isNotEmpty) {
      graphPainter = GraphBuilder.parseDot(widget.graph, widget.sdk)
          ?.getPainter(widget.direction);
    }
    super.initState();
  }

  @override
  void didUpdateWidget(GraphTab oldWidget) {
    final graphChanged =
        widget.graph.isNotEmpty && oldWidget.graph != widget.graph;
    final directionChanged = widget.direction != oldWidget.direction;
    if (graphChanged || directionChanged) {
      graphPainter = GraphBuilder.parseDot(widget.graph, widget.sdk)
          ?.getPainter(widget.direction);
    }
    if (widget.graph.isEmpty) {
      graphPainter = null;
    }
    super.didUpdateWidget(oldWidget);
  }

  @override
  Widget build(BuildContext context) {
    if (graphPainter == null) {
      return Container();
    }
    return Padding(
      padding: const EdgeInsets.all(BeamSizes.size16),
      child: SingleChildScrollView(
        child: SingleChildScrollView(
          scrollDirection: Axis.horizontal,
          child: ClipRRect(
            child: CustomPaint(
              painter: GraphCustomPainter(graph: graphPainter!),
              size: graphPainter?.getSize(),
            ),
          ),
        ),
      ),
    );
  }
}
