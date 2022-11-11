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

// TODO(alexeyinkin): Refactor this, merge into snippet_editor.dart

import 'package:flutter/material.dart';
import 'package:flutter_code_editor/flutter_code_editor.dart';

import '../models/example.dart';
import '../models/sdk.dart';
import '../theme/theme.dart';

const kJavaRegExp = r'import\s[A-z.0-9]*\;\n\n[(\/\*\*)|(public)|(class)]';
const kPythonRegExp = r'[^\S\r\n](import|as)[^\S\r\n][A-z]*\n\n';
const kGoRegExp = r'[^\S\r\n]+\'
    r'"'
    r'.*'
    r'"'
    r'\n\)\n\n';
const kAdditionalLinesForScrolling = 4;

class EditorTextArea extends StatefulWidget {
  final CodeController codeController;
  final Sdk sdk;
  final Example? example;
  final bool enabled;
  final bool isEditable;
  final bool goToContextLine;

  const EditorTextArea({
    super.key,
    required this.codeController,
    required this.sdk,
    this.example,
    required this.enabled,
    required this.isEditable,
    required this.goToContextLine,
  });

  @override
  State<EditorTextArea> createState() => _EditorTextAreaState();
}

class _EditorTextAreaState extends State<EditorTextArea> {
  var focusNode = FocusNode();
  final GlobalKey _sizeKey = LabeledGlobalKey('CodeFieldKey');

  @override
  void dispose() {
    super.dispose();
    focusNode.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (widget.goToContextLine) {
      WidgetsBinding.instance.addPostFrameCallback((_) => _setTextScrolling());
    }

    final ext = Theme.of(context).extension<BeamThemeExtension>()!;

    return Semantics(
      container: true,
      textField: true,
      multiline: true,
      enabled: widget.enabled,
      readOnly: widget.enabled,
      label: 'widgets.codeEditor.label',
      child: FocusScope(
        key: _sizeKey,
        node: FocusScopeNode(canRequestFocus: widget.isEditable),
        child: CodeTheme(
          data: ext.codeTheme,
          child: Container(
            color: ext.codeTheme.styles['root']?.backgroundColor,
            child: SingleChildScrollView(
              child: CodeField(
                key: ValueKey(widget.codeController),
                focusNode: focusNode,
                enabled: widget.enabled,
                controller: widget.codeController,
                textStyle: ext.codeRootStyle,
              ),
            ),
          ),
        ),
      ),
    );
  }

  void _setTextScrolling() {
    focusNode.requestFocus();
    if (widget.codeController.text.isNotEmpty) {
      widget.codeController.selection = TextSelection.fromPosition(
        TextPosition(
          offset: _getOffset(),
        ),
      );
    }
  }

  int _getOffset() {
    int contextLine = _getIndexOfContextLine();
    String pattern = _getPattern(_getQntOfStringsOnScreen());
    if (pattern == '' || pattern == '}') {
      return widget.codeController.text.lastIndexOf(pattern);
    }

    return widget.codeController.text.indexOf(
      pattern,
      contextLine,
    );
  }

  String _getPattern(int qntOfStrings) {
    int contextLineIndex = _getIndexOfContextLine();
    List<String> stringsAfterContextLine =
        widget.codeController.text.substring(contextLineIndex).split('\n');

    String result =
        stringsAfterContextLine.length + kAdditionalLinesForScrolling >
                qntOfStrings
            ? _getResultSubstring(stringsAfterContextLine, qntOfStrings)
            : stringsAfterContextLine.last;

    return result;
  }

  int _getQntOfStringsOnScreen() {
    final renderBox = _sizeKey.currentContext!.findRenderObject()! as RenderBox;
    final height = renderBox.size.height * .75;

    return height ~/ codeFontSize;
  }

  int _getIndexOfContextLine() {
    int ctxLineNumber = widget.example!.contextLine;
    String contextLine = widget.codeController.text.split('\n')[ctxLineNumber];

    while (contextLine == '') {
      ctxLineNumber -= 1;
      contextLine = widget.codeController.text.split('\n')[ctxLineNumber];
    }

    return widget.codeController.text.indexOf(contextLine);
  }

  // This function made for more accuracy in the process of finding an exact line.
  String _getResultSubstring(
    List<String> stringsAfterContextLine,
    int qntOfStrings,
  ) {
    StringBuffer result = StringBuffer();

    for (int i = qntOfStrings - kAdditionalLinesForScrolling;
        i < qntOfStrings + kAdditionalLinesForScrolling;
        i++) {
      if (i == stringsAfterContextLine.length - 1) {
        return result.toString();
      }
      result.write(stringsAfterContextLine[i] + '\n');
    }

    return result.toString();
  }
}
