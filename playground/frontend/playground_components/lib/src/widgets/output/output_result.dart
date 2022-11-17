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

import '../../constants/sizes.dart';
import '../../theme/theme.dart';

class OutputResult extends StatefulWidget {
  final String text;
  final bool isSelected;

  const OutputResult({Key? key, required this.text, required this.isSelected})
      : super(key: key);

  @override
  State<OutputResult> createState() => _OutputResultState();
}

class _OutputResultState extends State<OutputResult> {
  final ScrollController _scrollController = ScrollController();

  @override
  void didUpdateWidget(OutputResult oldWidget) {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients &&
          !widget.isSelected &&
          oldWidget.text != widget.text) {
        _scrollController.jumpTo(_scrollController.position.maxScrollExtent);
      }
    });
    super.didUpdateWidget(oldWidget);
  }

  @override
  Widget build(BuildContext context) {
    final ext = Theme.of(context).extension<BeamThemeExtension>()!;
    return SingleChildScrollView(
      controller: _scrollController,
      child: Scrollbar(
        thumbVisibility: true,
        trackVisibility: true,
        controller: _scrollController,
        child: Padding(
          padding: const EdgeInsets.all(BeamSizes.size16),
          child: SelectableText(
            widget.text,
            style: ext.codeRootStyle,
          ),
        ),
      ),
    );
  }
}
