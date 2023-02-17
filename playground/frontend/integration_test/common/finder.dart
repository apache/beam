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

import 'package:flutter/painting.dart';
import 'package:flutter_test/flutter_test.dart';

extension FinderExtension on Finder {
  Finder getChildrenByType(Type childType) {
    final finders = evaluate();
    final childElements = finders
        .map((e) => collectAllElementsFrom(e, skipOffstage: true))
        .expand((e) => e)
        .where((e) => e.widget.runtimeType == childType);

    return find.byElementPredicate(
      (element) => childElements.contains(element),
    );
  }

  Finder horizontallyAt(int index, WidgetTester wt) =>
      _atIndexOnAxis(index, Axis.horizontal, wt);

  Finder verticallyAt(int index, WidgetTester wt) =>
      _atIndexOnAxis(index, Axis.vertical, wt);

  Finder _atIndexOnAxis(int index, Axis axis, WidgetTester wt) {
    final finders = evaluate();

    if (index > finders.length - 1) {
      throw IndexError(index, finders);
    }

    final offsets = <_IndexAndOffset>[];

    for (int i = 0; i < finders.length; i++) {
      offsets.add(_IndexAndOffset(i, wt.getCenter(at(i))));
    }

    offsets.sort(
      (a, b) => axis == Axis.vertical
          ? _compareDoubles(a.offset.dy, b.offset.dy)
          : _compareDoubles(a.offset.dx, b.offset.dx),
    );

    return at(offsets[index].index);
  }

  int _compareDoubles(double a, double b) {
    return (a - b).sign.toInt();
  }
}

class _IndexAndOffset {
  final int index;
  final Offset offset;

  _IndexAndOffset(this.index, this.offset);
}
