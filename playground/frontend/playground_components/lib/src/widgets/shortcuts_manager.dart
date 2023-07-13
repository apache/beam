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

import '../models/shortcut.dart';

/// Makes [shortcuts] available to the tree beneath.
class ShortcutsManager extends StatelessWidget {
  final Widget child;
  final List<BeamShortcut> shortcuts;

  const ShortcutsManager({
    super.key,
    required this.child,
    required this.shortcuts,
  });

  @override
  Widget build(BuildContext context) {
    return FocusableActionDetector(
      autofocus: true,
      shortcuts: _shortcutsMap,
      actions: _getActions(context),
      child: child,
    );
  }

  Map<LogicalKeySet, Intent> get _shortcutsMap => {
        for (var shortcut in shortcuts)
          shortcut.keySet: shortcut.actionIntent
      };

  Map<Type, Action<Intent>> _getActions(BuildContext context) => {
        for (var shortcut in shortcuts)
          shortcut.actionIntent.runtimeType: shortcut.createAction(context)
      };
}
