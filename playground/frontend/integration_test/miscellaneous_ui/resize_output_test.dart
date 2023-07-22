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

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/modules/output/models/output_placement.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import '../common/common_finders.dart';

Future<void> checkResizeOutput(WidgetTester wt) async {
  final dragHandleStartPosition = wt.getCenter(find.dragHandle());
  Future<void> resetSplitViewRatio() async {
    final currentPosition = wt.getCenter(find.dragHandle());
    final offset = dragHandleStartPosition - currentPosition;
    await wt.drag(find.dragHandle(), offset);
    await wt.pumpAndSettle();
  }

  await _checkDragVertically(wt);
  await resetSplitViewRatio();

  await _checkExcessivelyDragVertically(wt);
  await resetSplitViewRatio();

  await wt.tapAndSettle(find.byKey(const ValueKey(OutputPlacement.left)));

  await _checkDragHorizontally(wt);
  await resetSplitViewRatio();

  await _checkExcessivelyDragHorizontally(wt);
  await resetSplitViewRatio();
}

Future<void> _checkDragVertically(WidgetTester wt) async {
  final height = wt.getSize(find.splitView()).height;
  var dragHandlePosition = wt.getCenter(find.dragHandle());

  await wt.drag(find.dragHandle(), Offset(0, height * 0.1));
  await wt.pumpAndSettle();

  var newPosition = wt.getCenter(find.dragHandle());
  expectSimilar(newPosition.dy, dragHandlePosition.dy + height * 0.1);
}

Future<void> _checkExcessivelyDragVertically(WidgetTester wt) async {
  final height = wt.getSize(find.splitView()).height;
  final dragHandlePosition = wt.getCenter(find.dragHandle());

  await wt.drag(find.dragHandle(), Offset(0, height * 0.9));
  await wt.pumpAndSettle();

  final newPosition = wt.getCenter(find.dragHandle());
  final maxDy = height * (maxRatio - defaultRatio);
  expectSimilar(
    newPosition.dy,
    dragHandlePosition.dy + maxDy,
  );
}

Future<void> _checkDragHorizontally(WidgetTester wt) async {
  final width = wt.getSize(find.splitView()).width;
  final dragHandlePosition = wt.getCenter(find.dragHandle());

  await wt.drag(find.dragHandle(), Offset(width * 0.1, 0));
  await wt.pumpAndSettle();

  final newPosition = wt.getCenter(find.dragHandle());
  expectSimilar(newPosition.dx, dragHandlePosition.dx + width * 0.1);
}

Future<void> _checkExcessivelyDragHorizontally(WidgetTester wt) async {
  final width = wt.getSize(find.splitView()).width;
  final dragHandlePosition = wt.getCenter(find.dragHandle());

  await wt.drag(find.dragHandle(), Offset(width * 0.9, 0));
  await wt.pumpAndSettle();

  final newPosition = wt.getCenter(find.dragHandle());
  final maxDx = width * (maxRatio - defaultRatio);
  expectSimilar(
    newPosition.dx,
    dragHandlePosition.dx + maxDx,
  );
}
