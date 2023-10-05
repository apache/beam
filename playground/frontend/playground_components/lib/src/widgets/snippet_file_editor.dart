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

import 'package:flutter/material.dart';
import 'package:flutter/scheduler.dart';
import 'package:flutter_code_editor/flutter_code_editor.dart';

import '../controllers/snippet_file_editing_controller.dart';
import '../models/event_snippet_context.dart';
import '../services/analytics/analytics_service.dart';
import '../services/analytics/events/snippet_modified.dart';
import '../theme/theme.dart';

class SnippetFileEditor extends StatefulWidget {
  SnippetFileEditor({
    required this.autofocus,
    required this.controller,
    required this.eventSnippetContext,
    required this.isEditable,
  }) : super(
          // When the example is changed, will scroll to the context line again.
          key: ValueKey(controller.savedFile),
        );

  final bool autofocus;
  final SnippetFileEditingController controller;
  final EventSnippetContext eventSnippetContext;
  final bool isEditable;

  @override
  State<SnippetFileEditor> createState() => _SnippetFileEditorState();
}

class _SnippetFileEditorState extends State<SnippetFileEditor> {
  bool _didAutoFocus = false;
  final _focusNode = FocusNode();
  bool _isEverModified = false;
  final _scrollController = ScrollController();

  @override
  void initState() {
    super.initState();
    widget.controller.addListener(_onChanged);
  }

  @override
  void didUpdateWidget(SnippetFileEditor oldWidget) {
    super.didUpdateWidget(oldWidget);
    oldWidget.controller.removeListener(_onChanged);
    widget.controller.addListener(_onChanged);
  }

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();

    if (widget.autofocus && !_didAutoFocus) {
      _didAutoFocus = true;
      SchedulerBinding.instance.addPostFrameCallback((_) {
        if (mounted) {
          _scrollSoCursorIsOnTop();
        }
      });
    }
  }

  void _scrollSoCursorIsOnTop() {
    _focusNode.requestFocus();

    final position = max(widget.controller.codeController.selection.start, 0);
    final characterOffset = _getLastCharacterOffset(
      style: kLightTheme.extension<BeamThemeExtension>()!.codeRootStyle,
      text: widget.controller.codeController.text.substring(0, position),
    );

    _scrollController.jumpTo(
      min(
        characterOffset.dy,
        _scrollController.position.maxScrollExtent,
      ),
    );
  }

  @override
  void dispose() {
    _focusNode.dispose();
    widget.controller.removeListener(_onChanged);
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final ext = Theme.of(context).extension<BeamThemeExtension>()!;

    return Semantics(
      container: true,
      enabled: widget.isEditable,
      label: 'widgets.codeEditor.label',
      multiline: true,
      readOnly: !widget.isEditable,
      textField: true,
      child: FocusScope(
        node: FocusScopeNode(canRequestFocus: widget.isEditable),
        child: CodeTheme(
          data: ext.codeTheme,
          child: Container(
            color: ext.codeTheme.styles['root']?.backgroundColor,
            child: SingleChildScrollView(
              controller: _scrollController,
              child: CodeField(
                key: ValueKey(widget.controller.codeController),
                controller: widget.controller.codeController,
                enabled: widget.isEditable,
                focusNode: _focusNode,
                textStyle: ext.codeRootStyle,
              ),
            ),
          ),
        ),
      ),
    );
  }

  void _onChanged() {
    if (widget.controller.isChanged && !_isEverModified) {
      final snippet = widget.eventSnippetContext.originalSnippet;

      if (snippet != null) {
        BeamAnalyticsService.get().sendUnawaited(
          SnippetModifiedAnalyticsEvent(
            fileName: widget.controller.savedFile.name,
            sdk: widget.controller.sdk,
            snippet: snippet,
            additionalParams: widget.controller.defaultEventParams,
          ),
        );
      }
      _isEverModified = true;
    }
  }
}

Offset _getLastCharacterOffset({
  required TextStyle style,
  required String text,
}) {
  final textPainter = TextPainter(
    textDirection: TextDirection.ltr,
    text: TextSpan(text: text, style: style),
  )..layout();

  return textPainter.getOffsetForCaret(
    TextPosition(offset: text.length),
    Rect.zero,
  );
}
