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

class NodeElementPainter {
  int row;
  int column;
  final Node element;
  Size? size;
  double? left;
  double? top;

  NodeElementPainter(
      {required this.element, required this.row, required this.column});

  void paint(
      CanvasDrawer drawer,
      Map<int, double> rowStarts,
      Map<int, double> columnStarts,
      ) {
    final rowNum = row;
    final columnNum = column;
    left = columnStarts[columnNum];
    top = rowStarts[rowNum];
    drawer.drawRect(left!, top!, size!.width, size!.height, size!.height * 0.2);
    if (parentLabel.isNotEmpty) {
      drawer.drawText(
        parentLabel,
        maxTextWidth,
        Offset(left! + BeamSizes.size16, top! + BeamSizes.size12),
      );
      drawer.drawSecondaryText(
        element.label,
        maxTextWidth,
        Offset(left! + BeamSizes.size16, top! + BeamSizes.size12 + BeamSizes.size8 + 10.0),
      );
    } else {
      drawer.drawText(
        element.label,
        maxTextWidth,
        Offset(left! + BeamSizes.size16, top! + (56 / 2 - 5)),
      );
    }
  }

  String get parentLabel {
    return element.parent?.label ?? '';
  }

  Size calculateSize(
      Map<String, NodeElementPainter> elementsMap,
      Map<int, double>? rowSizes,
      Map<int, double>? columnSizes,
      ) {
    if (size != null) {
      return size!;
    }
    final fullWidth = maxTextWidth + BeamSizes.size16 * 2;
    size = Size(fullWidth, BeamSizes.size12 * 2 + BeamSizes.size8 + 10.0 * 2);
    return size!;
  }

  double get maxTextWidth {
    var textWidth = getTextSize(element.label, 10.0).width;
    if (parentLabel.isNotEmpty) {
      final parentLabelSize = getTextSize(parentLabel, 10.0).width;
      return max(parentLabelSize, textWidth);
    }
    return textWidth;
  }
}
