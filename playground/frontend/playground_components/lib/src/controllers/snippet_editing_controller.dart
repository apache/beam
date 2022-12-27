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

import '../models/example.dart';
import '../models/example_loading_descriptors/content_example_loading_descriptor.dart';
import '../models/example_loading_descriptors/empty_example_loading_descriptor.dart';
import '../models/example_loading_descriptors/example_loading_descriptor.dart';
import '../models/example_view_options.dart';
import '../models/sdk.dart';
import '../services/symbols/symbols_notifier.dart';

/// The main state object for a single [sdk].
class SnippetEditingController extends ChangeNotifier {
  final Sdk sdk;
  final CodeController codeController;
  final _symbolsNotifier = GetIt.instance.get<SymbolsNotifier>();
  Example? _selectedExample;
  ExampleLoadingDescriptor? _descriptor;
  String _pipelineOptions = '';
  bool _isChanged = false;

  SnippetEditingController({
    required this.sdk,
  }) : codeController = CodeController(
          language: sdk.highlightMode,
          namedSectionParser: const BracketsStartEndNamedSectionParser(),
        ) {
    codeController.addListener(_onCodeControllerChanged);
    _symbolsNotifier.addListener(_onSymbolsNotifierChanged);
    _onSymbolsNotifierChanged();
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

  void setExample(
    Example example, {
    ExampleLoadingDescriptor? descriptor,
  }) {
    _descriptor = descriptor;
    _selectedExample = example;
    _pipelineOptions = example.pipelineOptions;
    _isChanged = false;

    final viewOptions = example.viewOptions;

    codeController.removeListener(_onCodeControllerChanged);
    setSource(example.source);
    _applyViewOptions(viewOptions);
    _toStartOfContextLineIfAny();
    codeController.addListener(_onCodeControllerChanged);

    notifyListeners();
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

  void _toStartOfContextLineIfAny() {
    final contextLine1Based = selectedExample?.contextLine;

    if (contextLine1Based == null) {
      return;
    }

    _toStartOfFullLine(max(contextLine1Based - 1, 0));
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

  Example? get selectedExample => _selectedExample;

  ExampleLoadingDescriptor? get descriptor => _descriptor;

  set pipelineOptions(String value) {
    if (value == _pipelineOptions) {
      return;
    }
    _pipelineOptions = value;

    if (!_isChanged) {
      if (_arePipelineOptionsChanged()) {
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

  String get pipelineOptions => _pipelineOptions;

  bool get isChanged => _isChanged;

  void _updateIsChanged() {
    _isChanged = _isCodeChanged() || _arePipelineOptionsChanged();
  }

  bool _isCodeChanged() {
    return _selectedExample?.source != codeController.fullText;
  }

  bool _arePipelineOptionsChanged() {
    return _pipelineOptions != (_selectedExample?.pipelineOptions ?? '');
  }

  void reset() {
    codeController.text = _selectedExample?.source ?? '';
    _pipelineOptions = _selectedExample?.pipelineOptions ?? '';
  }

  /// Creates an [ExampleLoadingDescriptor] that can recover the
  /// current content.
  ExampleLoadingDescriptor getLoadingDescriptor() {
    final example = selectedExample;
    if (example == null) {
      return EmptyExampleLoadingDescriptor(sdk: sdk);
    }

    if (!isChanged && _descriptor != null) {
      return _descriptor!;
    }

    return ContentExampleLoadingDescriptor(
      complexity: example.complexity,
      content: codeController.fullText,
      name: example.name,
      sdk: sdk,
    );
  }

  void setSource(String source) {
    codeController.readOnlySectionNames = const {};
    codeController.visibleSectionNames = const {};

    codeController.fullText = source;
    codeController.historyController.deleteHistory();
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

  @override
  void dispose() {
    _symbolsNotifier.removeListener(
      _onSymbolsNotifierChanged,
    );
    super.dispose();
  }
}
