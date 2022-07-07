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

import 'package:code_text_field/code_text_field.dart';
import 'package:flutter/material.dart';
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:highlight/languages/go.dart';
import 'package:highlight/languages/java.dart';
import 'package:highlight/languages/python.dart';
import 'package:highlight/languages/scala.dart';
import 'package:playground/config/theme.dart';
import 'package:playground/constants/fonts.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/editor/components/editor_themes.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:provider/provider.dart';

const kJavaRegExp = r'import\s[A-z.0-9]*\;\n\n[(\/\*\*)|(public)|(class)]';
const kPythonRegExp = r'[^\S\r\n](import|as)[^\S\r\n][A-z]*\n\n';
const kGoRegExp = r'[^\S\r\n]+\'
    r'"'
    r'.*'
    r'"'
    r'\n\)\n\n';
const kAdditionalLinesForScrolling = 4;

class EditorTextArea extends StatefulWidget {
  final SDK sdk;
  final ExampleModel? example;
  final bool enabled;
  final void Function(String)? onSourceChange;
  final bool isEditable;
  final bool isEmbedded;

  const EditorTextArea({
    Key? key,
    required this.sdk,
    this.example,
    this.onSourceChange,
    required this.enabled,
    required this.isEditable,
    this.isEmbedded = false,
  }) : super(key: key);

  @override
  State<EditorTextArea> createState() => _EditorTextAreaState();
}

class _EditorTextAreaState extends State<EditorTextArea> {
  CodeController? _codeController;
  var focusNode = FocusNode();
  final GlobalKey codeFieldKey = LabeledGlobalKey('CodeFieldKey');

  @override
  void initState() {
    super.initState();
  }

  @override
  void didChangeDependencies() {
    final themeProvider = Provider.of<ThemeProvider>(context, listen: true);
    _codeController = CodeController(
      text: _codeController?.text ?? widget.example?.source ?? '',
      language: _getLanguageFromSdk(),
      theme: themeProvider.isDarkMode ? kDarkCodeTheme : kLightCodeTheme,
      onChange: (newSource) {
        if (widget.onSourceChange != null) {
          widget.onSourceChange!(newSource);
        }
      },
      webSpaceFix: false,
    );

    super.didChangeDependencies();
  }

  @override
  void dispose() {
    super.dispose();
    _codeController?.dispose();
    focusNode.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (!widget.isEmbedded) {
      WidgetsBinding.instance.addPostFrameCallback((_) => _setTextScrolling());
    }

    return Semantics(
      container: true,
      textField: true,
      multiline: true,
      enabled: widget.enabled,
      readOnly: widget.enabled,
      label: AppLocalizations.of(context)!.codeTextArea,
      child: FocusScope(
        node: FocusScopeNode(canRequestFocus: widget.isEditable),
        child: CodeField(
          key: codeFieldKey,
          focusNode: focusNode,
          enabled: widget.enabled,
          controller: _codeController!,
          textStyle: getCodeFontStyle(
            textStyle: const TextStyle(fontSize: kCodeFontSize),
          ),
          expands: true,
          lineNumberStyle: LineNumberStyle(
            textStyle: TextStyle(
              color: ThemeColors.of(context).grey1Color,
            ),
          ),
        ),
      ),
    );
  }

  _setTextScrolling() {
    focusNode.requestFocus();
    if (_codeController!.text.isNotEmpty) {
      _codeController!.selection = TextSelection.fromPosition(
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
      return _codeController!.text.lastIndexOf(pattern);
    }

    return _codeController!.text.indexOf(
      pattern,
      contextLine,
    );
  }

  String _getPattern(int qntOfStrings) {
    int contextLineIndex = _getIndexOfContextLine();
    List<String> stringsAfterContextLine =
        _codeController!.text.substring(contextLineIndex).split('\n');

    String result =
        stringsAfterContextLine.length + kAdditionalLinesForScrolling >
                qntOfStrings
            ? _getResultSubstring(stringsAfterContextLine, qntOfStrings)
            : stringsAfterContextLine.last;

    return result;
  }

  int _getQntOfStringsOnScreen() {
    RenderBox rBox =
        codeFieldKey.currentContext?.findRenderObject() as RenderBox;
    double height = rBox.size.height * .75;

    return height ~/ kCodeFontSize;
  }

  int _getIndexOfContextLine() {
    int ctxLineNumber = widget.example!.contextLine;
    String contextLine = _codeController!.text.split('\n')[ctxLineNumber];

    while (contextLine == '') {
      ctxLineNumber -= 1;
      contextLine = _codeController!.text.split('\n')[ctxLineNumber];
    }

    return _codeController!.text.indexOf(contextLine);
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

  _getLanguageFromSdk() {
    switch (widget.sdk) {
      case SDK.java:
        return java;
      case SDK.go:
        return go;
      case SDK.python:
        return python;
      case SDK.scio:
        return scala;
    }
  }
}
