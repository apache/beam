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
import 'package:flutter/widgets.dart';

import '../../../../../constants/sizes.dart';
import '../../models/graph.dart';
import '../canvas_drawer.dart';
import 'node_painter.dart';

const kEdgeSpacing = 2 * BeamSizes.size16;

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
    Axis direction,
  ) {
    if (direction == Axis.vertical) {
      _drawVertical(
          drawer, elementsMap, rowStarts, columnStarts, rowSizes, columnSizes);
    } else {
      _drawHorizontal(
          drawer, elementsMap, rowStarts, columnStarts, rowSizes, columnSizes);
    }
  }

  _drawHorizontal(
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
    // 1. Go to the closest border (right)
    x = columnStarts[startColumn]! + columnSizes[startColumn]! + kEdgeSpacing;
    movePoints.add(Point(x, y));

    // 2. Go to the correct row
    y = rowStarts[endRow]! + rowSizes[endRow]! + kEdgeSpacing;
    movePoints.add(Point(x, y));

    // 3. Go to the correct column
    x = columnStarts[endColumn]! - kEdgeSpacing;
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

    drawer.drawRightArrow(
        optimizedMovePoints[0].x + BeamSizes.size16, optimizedMovePoints[0].y);

    _drawLine(drawer, optimizedMovePoints);
  }

  _drawVertical(
    CanvasDrawer drawer,
    Map<String, NodeElementPainter> elementsMap,
    Map<int, double> rowStarts,
    Map<int, double> columnStarts,
    Map<int, double> rowSizes,
    Map<int, double> columnSizes,
  ) {
    final startNode = elementsMap[edge.startId]!;
    final endNode = elementsMap[edge.endId]!;
    final startRow = startNode.row;
    final endColumn = endNode.column;
    final endRow = endNode.row;

    final List<Point<double>> movePoints = [];

    var x = startNode.left! + startNode.size!.width / 2;
    var y = startNode.top! + rowSizes[startRow]!;
    drawer.drawCircle(x, y, 4.0);
    movePoints.add(Point(x, y));
    // 1. Go to the closest border (bottom)
    y = rowStarts[startRow]! + rowSizes[startRow]! + kEdgeSpacing;
    movePoints.add(Point(x, y));

    // 2. Go to the correct column
    x = columnStarts[endColumn]! + columnSizes[endColumn]! + kEdgeSpacing;
    movePoints.add(Point(x, y));

    // 3. Go to the correct row
    y = rowStarts[endRow]! - kEdgeSpacing;
    movePoints.add(Point(x, y));

    // 4. Go to the middle of the column
    x = columnStarts[endColumn]! + endNode.size!.width / 2;
    movePoints.add(Point(x, y));

    // 5. Go to the element
    y = rowStarts[endRow]!;
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

    drawer.drawBottomArrow(
        optimizedMovePoints[0].x, optimizedMovePoints[0].y + BeamSizes.size16);

    _drawLine(drawer, optimizedMovePoints);
  }

  _drawLine(CanvasDrawer drawer, List<Point<double>> points) {
    points.forEachIndexed((index, point) {
      if (index == 0) {
        return;
      }
      final prevPoint = points[index - 1];
      if (edge.isPrimary) {
        drawer.drawLine(prevPoint.x, prevPoint.y, point.x, point.y);
      } else {
        drawer.drawDashedLine(prevPoint.x, prevPoint.y, point.x, point.y);
      }
    });
  }
}
