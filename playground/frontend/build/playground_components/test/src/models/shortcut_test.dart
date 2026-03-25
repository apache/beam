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

import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/playground_components.dart';

void main() {
  group(
    'BeamShortcut test.',
    () {
      test(
        'Title builds correctly',
        () {
          const meta = 'Command';
          final shortcutsAndTitles = {
            //
            _buildShortcut([
              LogicalKeyboardKey.meta,
              LogicalKeyboardKey.enter,
            ]): '$meta + Enter',

            _buildShortcut([
              LogicalKeyboardKey.enter,
              LogicalKeyboardKey.meta,
            ]): 'Enter + $meta',

            _buildShortcut([
              LogicalKeyboardKey.meta,
              LogicalKeyboardKey.shift,
              LogicalKeyboardKey.keyS,
            ]): '$meta + Shift + S',

            _buildShortcut([
              LogicalKeyboardKey.meta,
              LogicalKeyboardKey.alt,
              LogicalKeyboardKey.shift,
              LogicalKeyboardKey.keyS,
            ]): '$meta + Alt + Shift + S',

            _buildShortcut([
              LogicalKeyboardKey.control,
              LogicalKeyboardKey.alt,
              LogicalKeyboardKey.shift,
              LogicalKeyboardKey.keyS,
            ]): 'Control + Alt + Shift + S',
          };

          for (final entry in shortcutsAndTitles.entries) {
            expect(entry.key.title, entry.value);
          }
        },
      );
    },
  );
}

BeamShortcut _buildShortcut(List<LogicalKeyboardKey> keys) {
  return BeamShortcut(
    keys: keys,
    actionIntent: const BeamIntent(
      slug: 'slug',
    ),
    createAction: (_) => CallbackAction(
      onInvoke: (_) {
        return null;
      },
    ),
  );
}
