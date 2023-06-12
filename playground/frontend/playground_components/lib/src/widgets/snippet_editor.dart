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

import '../constants/sizes.dart';
import '../controllers/snippet_editing_controller.dart';
import 'loading_indicator.dart';
import 'snippet_file_editor.dart';
import 'tabbed_snippet_editor.dart';

class SnippetEditor extends StatelessWidget {
  const SnippetEditor({
    required this.autofocus,
    required this.controller,
    required this.isEditable,
    this.actionsWidget,
  });

  final bool autofocus;
  final SnippetEditingController controller;
  final bool isEditable;

  /// A child widget that will be:
  ///  - Hidden if no file is loaded.
  ///  - Shown as an overlay for a single file editor.
  ///  - Built into the tab bar for a multi-file editor.
  final Widget? actionsWidget;

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: controller,
      builder: (context, child) {
        switch (controller.fileControllers.length) {
          case 0:
            return const Center(
              child: LoadingIndicator(),
            );

          case 1:
            return Stack(
              children: [
                Positioned.fill(
                  child: SnippetFileEditor(
                    autofocus: autofocus,
                    controller: controller.fileControllers.first,
                    eventSnippetContext: controller.eventSnippetContext,
                    isEditable: isEditable,
                  ),
                ),
                if (actionsWidget != null)
                  Positioned(
                    right: 0,
                    top: BeamSizes.size10,
                    child: actionsWidget!,
                  ),
              ],
            );

          default:
            return TabbedSnippetEditor(
              autofocus: autofocus,
              controller: controller,
              eventSnippetContext: controller.eventSnippetContext,
              isEditable: isEditable,
              trailing: actionsWidget,
            );
        }
      },
    );
  }
}
