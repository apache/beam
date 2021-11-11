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
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/shortcuts/components/shortcut_tooltip.dart';
import 'package:playground/modules/shortcuts/constants/global_shortcuts.dart';

const kRunText = 'Run';

class RunButton extends StatelessWidget {
  final bool isRunning;
  final VoidCallback runCode;

  const RunButton({Key? key, required this.isRunning, required this.runCode})
      : super(key: key);

  @override
  Widget build(BuildContext context) {
    return ShortcutTooltip(
      shortcut: kRunShortcut,
      child: ElevatedButton.icon(
        icon: isRunning
            ? SizedBox(
                width: kIconSizeSm,
                height: kIconSizeSm,
                child: CircularProgressIndicator(
                  color: Theme.of(context).primaryColor,
                ),
              )
            : const Icon(Icons.play_arrow),
        label: const Text(kRunText),
        onPressed: !isRunning ? runCode : null,
      ),
    );
  }
}
