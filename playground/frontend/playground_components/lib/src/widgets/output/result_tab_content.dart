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
import 'package:flutter_code_editor/flutter_code_editor.dart';

import '../../constants/sizes.dart';
import '../../controllers/playground_controller.dart';
import '../../enums/result_filter.dart';
import '../../enums/unread_entry.dart';
import '../../theme/theme.dart';
import '../unread/clearer.dart';

// TODO(alexeyinkin): Show the full text when fixed: https://github.com/flutter/flutter/issues/128575
const _maxFirstCharacters = 1000;
const _maxLastCharacters = 10000;
const _cutTemplate = 'Showing the first $_maxFirstCharacters '
    'and the last $_maxLastCharacters characters:\n';

class ResultTabContent extends StatefulWidget {
  const ResultTabContent({
    required this.playgroundController,
  });

  final PlaygroundController playgroundController;

  @override
  State<ResultTabContent> createState() => _ResultTabContentState();
}

class _ResultTabContentState extends State<ResultTabContent> {
  final ScrollController _scrollController = ScrollController();
  final CodeController _codeController = CodeController(
    readOnly: true,
  );

  @override
  void initState() {
    super.initState();
    widget.playgroundController.codeRunner.addListener(_updateText);
    widget.playgroundController.resultFilterController.addListener(
      _updateText,
    );
    _updateText();
  }

  void _updateText() {
    _codeController.fullText = _getText();
  }

  @override
  void dispose() {
    _codeController.dispose();
    widget.playgroundController.resultFilterController.removeListener(
      _updateText,
    );
    widget.playgroundController.codeRunner.removeListener(_updateText);
    super.dispose();
  }

  String _getText() {
    final fullText = _getFullText();

    if (fullText.length <= _maxFirstCharacters + _maxLastCharacters) {
      return fullText;
    }

    // ignore: prefer_interpolation_to_compose_strings
    return _cutTemplate +
        fullText.substring(0, _maxFirstCharacters) +
        '\n\n...\n\n' +
        fullText.substring(fullText.length - _maxLastCharacters);
  }

  String _getFullText() {
    final filter = widget.playgroundController.resultFilterController.value;

    switch (filter) {
      case ResultFilterEnum.log:
        return widget.playgroundController.codeRunner.resultLog;
      case ResultFilterEnum.output:
        return widget.playgroundController.codeRunner.resultOutput;
      case ResultFilterEnum.all:
        return widget.playgroundController.codeRunner.resultLogOutput;
    }
  }

  @override
  Widget build(BuildContext context) {
    final ext = Theme.of(context).extension<BeamThemeExtension>()!;

    return UnreadClearer(
      controller: widget.playgroundController.codeRunner.unreadController,
      unreadKey: UnreadEntryEnum.result,
      child: ColoredBox(
        // TODO(alexeyinkin): Migrate to Material 3: https://github.com/apache/beam/issues/24610
        color: Theme.of(context).backgroundColor,
        child: AnimatedBuilder(
          animation: widget.playgroundController.codeRunner,
          builder: (context, child) => SingleChildScrollView(
            controller: _scrollController,
            child: Scrollbar(
              thumbVisibility: true,
              trackVisibility: true,
              controller: _scrollController,
              child: Padding(
                padding: const EdgeInsets.all(BeamSizes.size16),
                child: AnimatedBuilder(
                  animation: widget.playgroundController.resultFilterController,
                  builder: (context, child) {
                    return CodeTheme(
                      data: ext.codeTheme,
                      child: CodeField(
                        controller: _codeController,
                        gutterStyle: GutterStyle.none,
                        textStyle: ext.codeRootStyle,
                      ),
                    );
                  },
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}
