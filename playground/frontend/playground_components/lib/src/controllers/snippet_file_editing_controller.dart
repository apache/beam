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

import 'dart:math';

import 'package:flutter/widgets.dart';
import 'package:flutter_code_editor/flutter_code_editor.dart';
import 'package:get_it/get_it.dart';

import '../constants/constants.dart';
import '../models/example_view_options.dart';
import '../models/sdk.dart';
import '../models/snippet_file.dart';
import '../services/symbols/symbols_notifier.dart';

/// The main state object for a file in a snippet.
class SnippetFileEditingController extends ChangeNotifier {
  final CodeController codeController;
  final SnippetFile savedFile;
  final Sdk sdk;

  bool _isChanged = false;

  final _symbolsNotifier = GetIt.instance.get<SymbolsNotifier>();

  Map<String, dynamic> defaultEventParams = const {};

  SnippetFileEditingController({
    required this.savedFile,
    required this.sdk,
    required ExampleViewOptions viewOptions,
    int? contextLine1Based,
  }) : codeController = CodeController(
          // ignore: avoid_redundant_argument_values
          params: const EditorParams(tabSpaces: spaceCount),
          language: sdk.highlightMode,
          namedSectionParser: const BracketsStartEndNamedSectionParser(),
          text: savedFile.content,
        ) {
    _applyViewOptions(viewOptions);

    // TODO(alexeyinkin): Scroll to a comment instead of index,
    //  then remove the parameter, https://github.com/apache/beam/issues/23774
    if (contextLine1Based != null) {
      _toStartOfFullLine(max(contextLine1Based - 1, 0));
    }

    codeController.addListener(_onCodeControllerChanged);
    _symbolsNotifier.addListener(_onSymbolsNotifierChanged);
    _onSymbolsNotifierChanged();
  }

  void _applyViewOptions(ExampleViewOptions options) {
    codeController.readOnlySectionNames = options.readOnlySectionNames.toSet();
    codeController.visibleSectionNames = options.showSectionNames.toSet();

    if (options.foldCommentAtLineZero) {
      codeController.foldCommentAtLineZero();
    }

    if (options.foldImports) {
      codeController.foldImports();
    }

    final unfolded = options.unfoldSectionNames;
    if (unfolded.isNotEmpty) {
      codeController.foldOutsideSections(unfolded);
    }
  }

  void _toStartOfFullLine(int line) {
    if (line >= codeController.code.lines.length) {
      return;
    }

    final fullPosition = codeController.code.lines.lines[line].textRange.start;
    final visiblePosition = codeController.code.hiddenRanges.cutPosition(
      fullPosition,
    );

    codeController.selection = TextSelection.collapsed(
      offset: visiblePosition,
    );
  }

  void _onCodeControllerChanged() {
    if (!_isChanged) {
      if (_isCodeChanged()) {
        _isChanged = true;
        notifyListeners();
      }
    } else {
      _updateIsChanged();
      if (!_isChanged) {
        notifyListeners();
      }
    }
  }

  bool get isChanged => _isChanged;

  bool _isCodeChanged() {
    return savedFile.content.tabsToSpaces(spaceCount) !=
        codeController.fullText;
  }

  void _updateIsChanged() {
    _isChanged = _isCodeChanged();
  }

  void reset() {
    codeController.fullText = savedFile.content;
  }

  void _onSymbolsNotifierChanged() {
    final mode = sdk.highlightMode;
    if (mode == null) {
      return;
    }

    final dictionary = _symbolsNotifier.getDictionary(mode);
    if (dictionary == null) {
      return;
    }

    codeController.autocompleter.setCustomWords(dictionary.symbols);
  }

  SnippetFile getFile() => SnippetFile(
        content: codeController.fullText,
        isMain: savedFile.isMain,
        name: savedFile.name,
      );

  @override
  void dispose() {
    _symbolsNotifier.removeListener(
      _onSymbolsNotifierChanged,
    );
    super.dispose();
  }
}
