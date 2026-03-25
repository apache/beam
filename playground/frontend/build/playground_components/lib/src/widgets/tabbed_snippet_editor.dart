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

import 'package:collection/collection.dart';
import 'package:flutter/material.dart';
import 'package:flutter/widgets.dart';
import 'package:keyed_collection_widgets/keyed_collection_widgets.dart';

import '../controllers/snippet_editing_controller.dart';
import '../models/event_snippet_context.dart';
import 'snippet_file_editor.dart';
import 'tabs/tab_bar.dart';

class TabbedSnippetEditor extends StatelessWidget {
  const TabbedSnippetEditor({
    required this.autofocus,
    required this.controller,
    required this.eventSnippetContext,
    required this.isEditable,
    this.trailing,
  });

  final bool autofocus;
  final SnippetEditingController controller;
  final EventSnippetContext eventSnippetContext;
  final bool isEditable;
  final Widget? trailing;

  @override
  Widget build(BuildContext context) {
    final files = controller.fileControllers.map((c) => c.getFile());
    final keys = files.map((f) => f.name).toList(growable: false);
    final initialKey = files.firstWhereOrNull((f) => f.isMain)?.name;

    // TODO(nausharipov): fork keyed_collection_widgets and put prints.
    return DefaultKeyedTabController<String>.fromKeys(
      animationDuration: Duration.zero,
      initialKey: initialKey,
      keys: keys,
      onChanged: (key) {
        if (key != null) {
          controller.activateFileControllerByName(key);
        }
      },
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(
                child:
                    BeamTabBar(tabs: {for (final key in keys) key: Text(key)}),
              ),
              if (trailing != null) trailing!,
            ],
          ),
          Expanded(
            child: KeyedTabBarView.withDefaultController(
              children: {
                for (final key in keys)
                  key: SnippetFileEditor(
                    autofocus: autofocus,
                    controller: controller.requireFileControllerByName(key),
                    eventSnippetContext: eventSnippetContext,
                    isEditable: isEditable,
                  ),
              },
            ),
          ),
        ],
      ),
    );
  }
}
