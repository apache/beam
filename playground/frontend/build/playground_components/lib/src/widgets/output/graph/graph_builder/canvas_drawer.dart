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

import 'dart:ui';
import 'dart:ui' as ui;

import 'package:flutter/material.dart';

import '../../../../constants/colors.dart';

const kDashSize = 4;
const kArrowSize = 4;
const kDefaultFontSize = 10.0;

class CanvasDrawer {
  final Canvas canvas;

  CanvasDrawer(this.canvas);

  final borderPaint = Paint()
    ..color = BeamGraphColors.border
    ..strokeWidth = 1
    ..isAntiAlias = true
    ..style = PaintingStyle.fill;

  final linePaint = Paint()
    ..color = BeamGraphColors.edge
    ..strokeWidth = 2
    ..isAntiAlias = true
    ..style = PaintingStyle.fill;

  Paragraph createParagraph(String text, double width,
      {double fontSize = kDefaultFontSize, Color color = Colors.white}) {
    final textStyle = ui.TextStyle(
      color: color,
      fontSize: fontSize,
    );
    final paragraphStyle = ParagraphStyle(
      textDirection: TextDirection.ltr,
      textAlign: TextAlign.center,
    );
    final paragraphBuilder = ParagraphBuilder(paragraphStyle)
      ..pushStyle(textStyle)
      ..addText(text);
    final constraints = ParagraphConstraints(width: width);
    final paragraph = paragraphBuilder.build();
    paragraph.layout(constraints);
    return paragraph;
  }

  drawText(String text, double width, Offset offset) {
    canvas.drawParagraph(createParagraph(text, width), offset);
  }

  drawSecondaryText(String text, double width, Offset offset) {
    canvas.drawParagraph(
      createParagraph(
        text,
        width,
        color: BeamGraphColors.node,
      ),
      offset,
    );
  }

  drawLine(double x1, double y1, double x2, double y2) {
    canvas.drawLine(Offset(x1, y1), Offset(x2, y2), linePaint);
  }

  drawDashedLine(double x1, double y1, double x2, double y2) {
    double startX = x1;
    double startY = y1;

    while (startX < x2 || startY < y2) {
      double endX = x1 == x2 ? x1 : startX + kDashSize;
      double endY = y1 == y2 ? y1 : startY + kDashSize;
      canvas.drawLine(
        Offset(startX, startY),
        Offset(endX, endY),
        linePaint,
      );

      startX = x1 == x2 ? x1 : endX + kDashSize;
      startY = y1 == y2 ? y1 : endY + kDashSize;
    }
  }

  drawCircle(double x1, double y1, double radius) {
    canvas.drawCircle(Offset(x1, y1), radius, linePaint);
  }

  drawRightArrow(double x1, double y1) {
    final path = Path();
    path.moveTo(x1 - kArrowSize, y1 - kArrowSize);
    path.lineTo(x1 + kArrowSize, y1);
    path.lineTo(x1 - kArrowSize, y1 + kArrowSize);
    path.lineTo(x1 - kArrowSize, y1 - kArrowSize);
    canvas.drawPath(path, linePaint);
  }

  drawBottomArrow(double x1, double y1) {
    final path = Path();
    path.moveTo(x1 - kArrowSize, y1);
    path.lineTo(x1, y1 + kArrowSize);
    path.lineTo(x1 + kArrowSize, y1);
    path.lineTo(x1 - kArrowSize, y1);
    canvas.drawPath(path, linePaint);
  }

  drawRect(
    double left,
    double top,
    double width,
    double height,
    double radius,
  ) {
    final borderRadius = Radius.circular(radius);
    final rect = Rect.fromLTWH(left, top, width, height);
    final rRect = RRect.fromRectAndRadius(rect, borderRadius);
    canvas.drawRRect(rRect, borderPaint);
  }
}
