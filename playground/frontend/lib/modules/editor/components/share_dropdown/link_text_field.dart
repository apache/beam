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
import 'package:flutter/services.dart';
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground_components/playground_components.dart';

const _kTextFieldMaxHeight = 45.0;

class LinkTextField extends StatefulWidget {
  final String text;

  const LinkTextField({super.key, required this.text});

  @override
  State<LinkTextField> createState() => _LinkTextFieldState();
}

class _LinkTextFieldState extends State<LinkTextField> {
  final textEditingController = TextEditingController();
  bool _isPressed = false;

  @override
  initState() {
    super.initState();
    textEditingController.text = widget.text;
  }

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);

    return Container(
      decoration: BoxDecoration(
        color: themeData.extension<BeamThemeExtension>()?.borderColor,
        borderRadius: BorderRadius.circular(kSmBorderRadius),
      ),
      child: Container(
        margin: const EdgeInsets.symmetric(horizontal: kMdSpacing),
        child: Center(
          child: TextFormField(
            controller: textEditingController,
            decoration: InputDecoration(
              constraints: const BoxConstraints(
                maxHeight: _kTextFieldMaxHeight,
              ),
              border: InputBorder.none,
              suffixIcon: _buildCopyButton(),
            ),
            readOnly: true,
            style: TextStyle(
              fontSize: kLabelFontSize,
              fontWeight: kNormalWeight,
              color: themeData.primaryColor,
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildCopyButton() {
    return MouseRegion(
      cursor: SystemMouseCursors.click,
      child: GestureDetector(
        onTap: () async {
          await _copyLinkText();
          setState(() {
            _isPressed = true;
          });
        },
        child: _isPressed
            ? const Icon(
                Icons.check,
                size: kIconSizeMd,
              )
            : const Icon(
                Icons.file_copy_outlined,
                size: kIconSizeSm,
              ),
      ),
    );
  }

  Future<void> _copyLinkText() async {
    await Clipboard.setData(ClipboardData(text: widget.text));
  }
}
