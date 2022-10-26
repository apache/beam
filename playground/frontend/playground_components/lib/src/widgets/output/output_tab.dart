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

import 'package:aligned_dialog/aligned_dialog.dart';
import 'package:flutter/material.dart';

import '../../constants/sizes.dart';
import '../../controllers/playground_controller.dart';
import 'result_filter_popover.dart';

class OutputTab extends StatefulWidget {
  final PlaygroundController playgroundController;
  final String name;
  final bool isSelected;
  final String value;
  final bool hasFilter;

  const OutputTab({
    super.key,
    required this.playgroundController,
    required this.name,
    required this.isSelected,
    required this.value,
    this.hasFilter = false,
  });

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
    final themeData = Theme.of(context);

    return Tab(
      child: Wrap(
        direction: Axis.horizontal,
        alignment: WrapAlignment.center,
        spacing: BeamSizes.size8,
        children: [
          Text(widget.name),
          widget.hasFilter
              ? GestureDetector(
                  onTap: () {
                    showAlignedDialog(
                      context: context,
                      builder: (dialogContext) => ResultFilterPopover(
                        playgroundController: widget.playgroundController,
                      ),
                      followerAnchor: Alignment.topLeft,
                      targetAnchor: Alignment.topLeft,
                      barrierColor: Colors.transparent,
                    );
                  },
                  child: Icon(
                    Icons.filter_alt_outlined,
                    size: BeamIconSizes.small,
                    color: themeData.primaryColor,
                  ),
                )
              : const SizedBox(),
          if (hasNewContent)
            Container(
              width: BeamIconSizes.xs,
              height: BeamIconSizes.xs,
              decoration: BoxDecoration(
                color: themeData.primaryColor,
                shape: BoxShape.circle,
              ),
            ),
        ],
      ),
    );
  }
}
