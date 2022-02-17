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
import 'package:collection/collection.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/graph/graph_builder/canvas_drawer.dart';
import 'package:playground/modules/graph/graph_builder/painters/node_painter.dart';
import 'package:playground/modules/graph/models/graph.dart';

class EdgePainter {
  final Edge edge;

  EdgePainter(this.edge);

  void paint(
      CanvasDrawer drawer,
      Map<String, NodeElementPainter> elementsMap,
      Map<int, double> rowStarts,
      Map<int, double> columnStarts,
      Map<int, double> rowSizes,
      Map<int, double> columnSizes,
      ) {
    final startNode = elementsMap[edge.startId]!;
    final endNode = elementsMap[edge.endId]!;
    final startColumn = startNode.column;
    final endColumn = endNode.column;
    final endRow = endNode.row;

    final List<Point<double>> movePoints = [];

    var x = startNode.left! + startNode.size!.width;
    var y = startNode.top! + startNode.size!.height / 2;
    drawer.drawCircle(x, y, 4.0);
    movePoints.add(Point(x, y));
    // 1. Go to the closest border
    x = columnStarts[startColumn]! + columnSizes[startColumn]! + 2 * kXlSpacing;
    movePoints.add(Point(x, y));

    // 2. Go to the correct row
    y = rowStarts[endRow]! + rowSizes[endRow]! + 2 * kXlSpacing;
    movePoints.add(Point(x, y));

    // 3. Go to the correct column
    x = columnStarts[endColumn]! - 2 * kXlSpacing;
    movePoints.add(Point(x, y));

    // 4. Go to the middle of the row
    y = rowStarts[endRow]! + rowSizes[endRow]! / 2;
    movePoints.add(Point(x, y));

    // 5. Go to the element
    x = columnStarts[endColumn]!;
    movePoints.add(Point(x, y));
    drawer.drawCircle(x, y, 4.0);

    List<Point<double>> optimizedMovePoints = [];

    movePoints.forEachIndexed((index, element) {
      if (index == 0 || index == movePoints.length - 1) {
        optimizedMovePoints.add(element);
        return;
      }
      if (movePoints[index - 1].x == movePoints[index].x &&
          movePoints[index].x == movePoints[index + 1].x) {
        return;
      }
      if (movePoints[index - 1].y == movePoints[index].y &&
          movePoints[index].y == movePoints[index + 1].y) {
        return;
      }
      optimizedMovePoints.add(element);
    });

    drawer.drawArrow(
        optimizedMovePoints[0].x + kXlSpacing, optimizedMovePoints[0].y);

    optimizedMovePoints.forEachIndexed((index, point) {
      if (index == 0) {
        return;
      }
      final prevPoint = optimizedMovePoints[index - 1];
      if (edge.isPrimary) {
        drawer.drawLine(prevPoint.x, prevPoint.y, point.x, point.y);
      } else {
        drawer.drawDashedLine(prevPoint.x, prevPoint.y, point.x, point.y);
      }
    });
  }
}
