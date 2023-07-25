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
import 'package:playground/constants/colors.dart';

const defaultRatio = 0.65;
const kContainerTopBorder = Border(
  top: BorderSide(
    width: 0.5,
    color: kLightGrey1,
  ),
);

class EmbeddedSplitView extends StatefulWidget {
  final Widget first;
  final Widget second;

  const EmbeddedSplitView({
    Key? key,
    required this.first,
    required this.second,
  }) : super(key: key);

  @override
  State<EmbeddedSplitView> createState() => _EmbeddedSplitViewState();
}

class _EmbeddedSplitViewState extends State<EmbeddedSplitView> {
  double _maxSize = 0;

  get _sizeFirst => defaultRatio * _maxSize;

  get _sizeSecond => (1 - defaultRatio) * _maxSize;

  @override
  Widget build(BuildContext widgetContext) {
    return LayoutBuilder(
      builder: (context, BoxConstraints constraints) {
        _updateMaxSize(constraints);
        return _buildVerticalLayout(context, constraints);
      },
    );
  }

  Widget _buildVerticalLayout(
    BuildContext context,
    BoxConstraints constraints,
  ) {
    return SizedBox(
      height: constraints.maxHeight,
      width: double.infinity,
      child: Column(
        children: <Widget>[
          SizedBox(
            height: _sizeFirst,
            child: widget.first,
          ),
          Container(
            height: _sizeSecond,
            width: double.infinity,
            decoration: const BoxDecoration(border: kContainerTopBorder),
            child: widget.second,
          ),
        ],
      ),
    );
  }

  void _updateMaxSize(BoxConstraints constraints) {
    if (_maxSize != constraints.maxHeight) {
      _maxSize = constraints.maxHeight;
    }
  }
}
