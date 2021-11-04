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

const kCodeAreaSemantics = 'Code textarea';

class EditorTextArea extends StatefulWidget {
  final SDK sdk;
  final ExampleModel? example;
  final void Function(String) onSourceChange;

  const EditorTextArea({
    Key? key,
    required this.sdk,
    this.example,
    required this.onSourceChange,
  }) : super(key: key);

  @override
  _EditorTextAreaState createState() => _EditorTextAreaState();
}

class _EditorTextAreaState extends State<EditorTextArea> {
  CodeController? _codeController;

  @override
  void initState() {
    super.initState();
  }

  @override
  void didChangeDependencies() {
    final themeProvider = Provider.of<ThemeProvider>(context, listen: true);
    _codeController = CodeController(
      text: _codeController?.text ?? widget.example?.sources[widget.sdk] ?? '',
      language: _getLanguageFromSdk(),
      theme: themeProvider.isDarkMode ? kDarkCodeTheme : kLightCodeTheme,
      onChange: (newSource) => widget.onSourceChange(newSource),
      webSpaceFix: false,
    );
    super.didChangeDependencies();
  }

  @override
  void dispose() {
    super.dispose();
    _codeController?.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Semantics(
      container: true,
      textField: true,
      multiline: true,
      enabled: true,
      readOnly: false,
      label: kCodeAreaSemantics,
      child: CodeField(
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
    );
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
