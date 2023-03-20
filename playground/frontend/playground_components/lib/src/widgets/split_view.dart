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

import '../constants/sizes.dart';
import 'drag_handle.dart';

const minRatio = 0.3;
const maxRatio = 0.7;
const defaultRatio = 0.5;

class SplitView extends StatefulWidget {
  final Widget first;
  final Widget second;
  final Axis direction;
  final double initialRatio;
  final Key? dragHandleKey;

  const SplitView({
    super.key,
    required this.first,
    required this.second,
    required this.direction,
    this.dragHandleKey,
    this.initialRatio = defaultRatio,
  });

  @override
  State<SplitView> createState() => _SplitViewState();
}

class _SplitViewState extends State<SplitView> {
  //from minRatio to maxRatio
  double _ratio = defaultRatio;
  double _maxSize = 0;

  int get _sizeFirst => (_ratio * _maxSize).toInt();

  int get _sizeSecond => ((1 - _ratio) * _maxSize).toInt();

  get _isHorizontal => widget.direction == Axis.horizontal;

  get _isVertical => widget.direction == Axis.vertical;

  @override
  void initState() {
    super.initState();
    _ratio = widget.initialRatio;
  }

  @override
  Widget build(BuildContext widgetContext) {
    return LayoutBuilder(builder: (context, BoxConstraints constraints) {
      _updateMaxSize(constraints);
      return _isHorizontal
          ? _buildHorizontalLayout(context, constraints)
          : _buildVerticalLayout(context, constraints);
    });
  }

  Widget _buildHorizontalLayout(
      BuildContext context, BoxConstraints constraints) {
    return SizedBox(
      width: constraints.maxWidth,
      child: Row(
        children: <Widget>[
          Expanded(
            flex: _sizeFirst,
            child: widget.first,
          ),
          _buildSeparator(context),
          Expanded(
            flex: _sizeSecond,
            child: widget.second,
          ),
        ],
      ),
    );
  }

  Widget _buildVerticalLayout(
      BuildContext context, BoxConstraints constraints) {
    return SizedBox(
      height: constraints.maxHeight,
      child: Column(
        children: <Widget>[
          Expanded(
            flex: _sizeFirst,
            child: widget.first,
          ),
          _buildSeparator(context),
          Expanded(
            flex: _sizeSecond,
            child: widget.second,
          ),
        ],
      ),
    );
  }

  Widget _buildSeparator(BuildContext context) {
    return MouseRegion(
      cursor: _isHorizontal
          ? SystemMouseCursors.resizeLeftRight
          : SystemMouseCursors.resizeUpDown,
      child: GestureDetector(
        behavior: HitTestBehavior.translucent,
        child: Container(
          width: _isHorizontal ? BeamSizes.splitViewSeparator : double.infinity,
          height: _isVertical ? BeamSizes.splitViewSeparator : double.infinity,
          color: Theme.of(context).dividerColor,
          child: Center(
            child: DragHandle(
              direction: widget.direction,
              key: widget.dragHandleKey,
            ),
          ),
        ),
        onPanUpdate: (DragUpdateDetails details) {
          setState(() {
            _updateRatio(details);
          });
        },
      ),
    );
  }

  void _updateRatio(DragUpdateDetails details) {
    if (_maxSize == 0) {
      return;
    }
    if (_isHorizontal) {
      _ratio += details.delta.dx / _maxSize;
    } else {
      _ratio += details.delta.dy / _maxSize;
    }
    _checkRatioSafe();
  }

  void _checkRatioSafe() {
    if (_ratio > maxRatio) {
      _ratio = maxRatio;
    }
    if (_ratio < minRatio) {
      _ratio = minRatio;
    }
  }

  void _updateMaxSize(BoxConstraints constraints) {
    _calculateMaxSize(
        _isHorizontal ? constraints.maxWidth : constraints.maxHeight);
  }

  void _calculateMaxSize(double maxSize) {
    if (_maxSize != maxSize) {
      _maxSize = maxSize - BeamSizes.splitViewSeparator;
    }
  }
}
