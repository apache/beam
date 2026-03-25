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

/// A temporary widget while Flutter team is working on
/// their TwoDimensionalScrollable.
///
/// See https://github.com/apache/beam/issues/25118#issuecomment-1457703666
class Scroll2DWidget extends StatefulWidget {
  const Scroll2DWidget({
    required this.child,
  });

  final Widget child;

  @override
  State<Scroll2DWidget> createState() => _Scroll2DWidgetState();
}

class _Scroll2DWidgetState extends State<Scroll2DWidget> {
  final _verticalScrollController = ScrollController();
  final _horizontalScrollController = ScrollController();
  bool _isPanning = false;

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      cursor:
          _isPanning ? SystemMouseCursors.grabbing : SystemMouseCursors.grab,
      child: GestureDetector(
        onPanDown: (details) {
          _setIsPanning(true);
        },
        onPanStart: (details) {
          _setIsPanning(true);
        },
        onPanEnd: (details) {
          _setIsPanning(false);
        },
        onPanCancel: () {
          _setIsPanning(false);
        },
        onPanUpdate: (details) {
          _verticalScrollController.position.jumpTo(
            _verticalScrollController.position.pixels - details.delta.dy,
          );
          _horizontalScrollController.position.jumpTo(
            _horizontalScrollController.position.pixels - details.delta.dx,
          );
        },
        child: SingleChildScrollView(
          controller: _verticalScrollController,
          child: SingleChildScrollView(
            controller: _horizontalScrollController,
            scrollDirection: Axis.horizontal,
            child: widget.child,
          ),
        ),
      ),
    );
  }

  void _setIsPanning(bool newValue) {
    setState(() {
      _isPanning = newValue;
    });
  }
}
