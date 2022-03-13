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
import 'package:playground/config/theme.dart';
import 'package:playground/constants/sizes.dart';

class OutputTab extends StatefulWidget {
  final String name;
  final bool isSelected;
  final String value;

  const OutputTab({
    Key? key,
    required this.name,
    required this.isSelected,
    required this.value,
  }) : super(key: key);

  @override
  State<OutputTab> createState() => _OutputTabState();
}

class _OutputTabState extends State<OutputTab> {
  bool hasNewContent = false;

  @override
  void didUpdateWidget(OutputTab oldWidget) {
    if (widget.isSelected && hasNewContent) {
      setState(() {
        hasNewContent = false;
      });
    } else if (!widget.isSelected &&
        widget.value.isNotEmpty &&
        oldWidget.value != widget.value) {
      setState(() {
        hasNewContent = true;
      });
    }
    super.didUpdateWidget(oldWidget);
  }

  @override
  Widget build(BuildContext context) {
    return Tab(
      child: Wrap(
        direction: Axis.horizontal,
        alignment: WrapAlignment.center,
        spacing: 8.0,
        children: [
          Text(widget.name),
          if (hasNewContent)
            Container(
              width: kIconSizeXs,
              height: kIconSizeXs,
              decoration: BoxDecoration(
                color: ThemeColors.of(context).primary,
                shape: BoxShape.circle,
              ),
            ),
        ],
      ),
    );
  }
}
