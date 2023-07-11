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

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/shortcuts/components/shortcut_row.dart';
import 'package:playground/modules/shortcuts/constants/global_shortcuts.dart';
import 'package:playground_components/playground_components.dart';

class ShortcutsDialogContent extends StatelessWidget {
  static const _kModalMaxWidth = 400.0;
  static const _kShortcutsMaxWidth = 200.0;

  const ShortcutsDialogContent({
    required this.playgroundController,
  });

  final PlaygroundController playgroundController;

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: _kModalMaxWidth,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        mainAxisSize: MainAxisSize.min,
        children: [
          ...[
            ...playgroundController.shortcuts,
            BeamMainRunShortcut(onInvoke: () {}),
            ...globalShortcuts,
          ]
              .map(
                (shortcut) => Row(
                  crossAxisAlignment: CrossAxisAlignment.center,
                  children: [
                    ConstrainedBox(
                      constraints:
                          const BoxConstraints(maxWidth: _kShortcutsMaxWidth),
                      child: ShortcutRow(shortcut: shortcut),
                    ),
                    const SizedBox(width: kMdSpacing),
                    Expanded(
                      child: Text(
                        shortcut.actionIntent.slug.tr(),
                        style: const TextStyle(fontWeight: kBoldWeight),
                      ),
                    ),
                  ],
                ),
              )
              .alternateWith(
                const SizedBox(
                  height: kXlSpacing,
                ),
              ),
        ],
      ),
    );
  }
}
