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
import 'package:flutter_svg/svg.dart';
import 'package:provider/provider.dart';

import '../constants/assets.dart';
import '../constants/sizes.dart';

enum SplitViewDirection {
  vertical,
  horizontal,
}

const _minRatio = 0.3;
const _maxRatio = 0.7;
const _defaultRatio = 0.5;
const _dividerSize = BeamSizes.size8;

class SplitView extends StatefulWidget {
  final ThemeData themeData;
  final Widget first;
  final Widget second;
  final SplitViewDirection direction;
  final double ratio;

  const SplitView({
    required this.themeData,
    required this.first,
    required this.second,
    required this.direction,
    this.ratio = _defaultRatio,
  });

  @override
  State<SplitView> createState() => _SplitViewState();
}

class _SplitViewState extends State<SplitView> {
  double _ratio = _defaultRatio;
  double _maxSize = 0;

  double get _sizeFirst => _ratio * _maxSize;
  double get _sizeSecond => (1 - _ratio) * _maxSize;
  bool get _isHorizontal => widget.direction == SplitViewDirection.horizontal;
  bool get _isVertical => widget.direction == SplitViewDirection.vertical;

  @override
  void initState() {
    super.initState();
    _ratio = widget.ratio;
  }

  @override
  Widget build(BuildContext widgetContext) {
    return Provider(
      create: (_) => widget.themeData,
      child: LayoutBuilder(
        builder: (context, BoxConstraints constraints) {
          _updateMaxSize(constraints);
          return _isHorizontal
              ? _HorizontalLayout(
                  isHorizontal: _isHorizontal,
                  isVertical: _isVertical,
                  constraints: constraints,
                  firstChild: widget.first,
                  secondChild: widget.second,
                  sizeFirst: _sizeFirst,
                  sizeSecond: _sizeSecond,
                  onPanUpdate: _onPanUpdate,
                )
              : _buildVerticalLayout(context, constraints);
        },
      ),
    );
  }

  void _onPanUpdate(DragUpdateDetails details) {
    setState(() {
      _updateRatio(details);
    });
  }

  // TODO(nausharipov): extract as widgets
  Widget _buildVerticalLayout(
    BuildContext context,
    BoxConstraints constraints,
  ) {
    return SizedBox(
      height: constraints.maxHeight,
      child: Column(
        children: <Widget>[
          SizedBox(
            height: _sizeFirst,
            child: widget.first,
          ),
          _Separator(
            isHorizontal: _isHorizontal,
            isVertical: _isVertical,
            onPanUpdate: _onPanUpdate,
          ),
          SizedBox(
            height: _sizeSecond,
            child: widget.second,
          ),
        ],
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
    if (_ratio > _maxRatio) {
      _ratio = _maxRatio;
    }
    if (_ratio < _minRatio) {
      _ratio = _minRatio;
    }
  }

  void _updateMaxSize(BoxConstraints constraints) {
    _calculateMaxSize(
      _isHorizontal ? constraints.maxWidth : constraints.maxHeight,
    );
  }

  void _calculateMaxSize(double maxSize) {
    if (_maxSize != maxSize) {
      _maxSize = maxSize - _dividerSize;
    }
  }
}

class _HorizontalLayout extends StatelessWidget {
  final BoxConstraints constraints;
  final bool isHorizontal;
  final bool isVertical;
  final void Function(DragUpdateDetails)? onPanUpdate;
  final Widget firstChild;
  final Widget secondChild;
  final double sizeFirst;
  final double sizeSecond;

  const _HorizontalLayout({
    required this.constraints,
    required this.onPanUpdate,
    required this.isHorizontal,
    required this.isVertical,
    required this.firstChild,
    required this.secondChild,
    required this.sizeFirst,
    required this.sizeSecond,
  });

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: constraints.maxWidth,
      child: Row(
        children: <Widget>[
          SizedBox(
            width: sizeFirst,
            child: firstChild,
          ),
          _Separator(
            isHorizontal: isHorizontal,
            isVertical: isVertical,
            onPanUpdate: onPanUpdate,
          ),
          SizedBox(
            width: sizeSecond,
            child: secondChild,
          ),
        ],
      ),
    );
  }
}

class _Separator extends StatelessWidget {
  final bool isHorizontal;
  final bool isVertical;
  final void Function(DragUpdateDetails)? onPanUpdate;

  const _Separator({
    required this.isHorizontal,
    required this.isVertical,
    required this.onPanUpdate,
  });

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      cursor: isHorizontal
          ? SystemMouseCursors.resizeLeftRight
          : SystemMouseCursors.resizeUpDown,
      child: GestureDetector(
        behavior: HitTestBehavior.translucent,
        onPanUpdate: onPanUpdate,
        child: Container(
          padding: const EdgeInsets.all(2),
          width: isHorizontal ? _dividerSize : double.infinity,
          height: isVertical ? _dividerSize : double.infinity,
          color: context.watch<ThemeData>().dividerColor,
          child: SvgPicture.asset(
            isHorizontal ? BeamAssets.dragHorizontal : BeamAssets.dragVertical,
            package: 'playground_components',
          ),
        ),
      ),
    );
  }
}
