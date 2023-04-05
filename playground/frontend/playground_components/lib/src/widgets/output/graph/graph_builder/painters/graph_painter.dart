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

import 'dart:math';

import 'package:flutter/material.dart';

import '../../../../../constants/sizes.dart';
import '../../models/graph.dart';
import '../canvas_drawer.dart';
import 'edge_painter.dart';
import 'node_painter.dart';

class GraphPainter {
  final List<NodeElementPainter> elementsPainter;
  final List<EdgePainter> edges;
  final Axis direction;
  final Map<String, NodeElementPainter> elementsMap = {};
  final Map<int, double> rowSizes = {};
  final Map<int, double> columnSizes = {};
  final Map<int, double> rowStarts = {};
  final Map<int, double> columnStarts = {};

  getSize() {
    final lastColumn = columnStarts.length - 1;
    final lastRow = rowStarts.length - 1;
    final width =
        columnStarts[lastColumn]! + columnSizes[lastColumn]! + 4 * BeamSizes.size16;
    final height = rowStarts[lastRow]! + rowSizes[lastRow]! + 4 * BeamSizes.size16;
    return Size(width, height);
  }

  GraphPainter({
    required this.elementsPainter,
    required this.edges,
    required this.direction,
  }) {
    for (var element in elementsPainter) {
      elementsMap[element.element.name] = element;
    }
    elementsPainter
        .where((painter) => painter.element.type == NodeType.node)
        .cast<NodeElementPainter>()
        .forEach((painter) {
      final currentRowSize = rowSizes[painter.row] ?? 0.0;
      final elementHeight =
          painter.calculateSize(elementsMap, null, null).height;
      rowSizes[painter.row] = max(currentRowSize, elementHeight);
      final elementWidth = painter.calculateSize(elementsMap, null, null).width;
      final currentColumnSize = columnSizes[painter.column] ?? 0.0;
      columnSizes[painter.column] = max(currentColumnSize, elementWidth);
    });
    var left = 0.0;
    var top = 0.0;
    for (var r = 0; r < rowSizes.length; r++) {
      rowStarts[r] = top;
      top = top + rowSizes[r]! + 4 * BeamSizes.size16;
    }
    for (var c = 0; c < columnSizes.length; c++) {
      columnStarts[c] = left;
      left = left + columnSizes[c]! + 4 * BeamSizes.size16;
    }
  }

  paint(CanvasDrawer drawer) {
    elementsPainter
        .where((painter) => painter.element.type == NodeType.node)
        .cast<NodeElementPainter>()
        .forEach((element) {
      element.paint(drawer, rowStarts, columnStarts);
    });
    for (var element in edges) {
      element.paint(
        drawer,
        elementsMap,
        rowStarts,
        columnStarts,
        rowSizes,
        columnSizes,
        direction,
      );
    }
  }
}
