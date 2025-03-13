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
import 'package:playground_components/playground_components.dart';

import '../../../../constants/font_weight.dart';
import '../../../../constants/sizes.dart';
import '../../../../services/analytics/events/shareable_copied.dart';
import 'share_tabs/share_format_enum.dart';

const _kTextFieldMaxHeight = 45.0;

class LinkTextField extends StatefulWidget {
  final EventSnippetContext eventSnippetContext;
  final ShareFormat shareFormat;
  final String text;

  const LinkTextField({
    super.key,
    required this.eventSnippetContext,
    required this.shareFormat,
    required this.text,
  });

  @override
  State<LinkTextField> createState() => _LinkTextFieldState();
}

class _LinkTextFieldState extends State<LinkTextField> {
  final textEditingController = TextEditingController();

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
              suffixIcon: CopyButton(
                beforePressed: _submitEvent,
                text: widget.text,
              ),
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

  void _submitEvent() {
    PlaygroundComponents.analyticsService.sendUnawaited(
      ShareableCopiedAnalyticsEvent(
        shareFormat: widget.shareFormat,
        snippetContext: widget.eventSnippetContext,
      ),
    );
  }
}

class CopyButton extends StatefulWidget {
  const CopyButton({
    required this.beforePressed,
    required this.text,
  });

  final VoidCallback beforePressed;
  final String text;

  @override
  State<CopyButton> createState() => _CopyButtonState();
}

class _CopyButtonState extends State<CopyButton> {
  bool _isPressed = false;

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      cursor: SystemMouseCursors.click,
      child: GestureDetector(
        onTap: () async {
          widget.beforePressed();
          await _copyText();
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

  Future<void> _copyText() async {
    try {
      await Clipboard.setData(ClipboardData(text: widget.text));
    } on Exception catch (ex) {
      print('Copy to clipboard failed: ${widget.text}'); // ignore: avoid_print
      print(ex); // ignore: avoid_print
    }
  }
}
