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

import '../constants/sizes.dart';
import '../controllers/playground_controller.dart';
import '../theme/theme.dart';
import 'shortcut_tooltip.dart';

const kMsToSec = 1000;
const kSecondsFractions = 1;

const _width = 150.0;

class RunButton extends StatelessWidget {
  final PlaygroundController playgroundController;
  final bool isRunning;
  final VoidCallback runCode;
  final VoidCallback cancelRun;
  final bool disabled;

  const RunButton({
    super.key,
    required this.playgroundController,
    required this.isRunning,
    required this.runCode,
    required this.cancelRun,
    this.disabled = false,
  });

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: _width,
      height: BeamSizes.buttonHeight,
      child: ShortcutTooltip(
        shortcut: playgroundController.runShortcut,
        child: ElevatedButton.icon(
          icon: isRunning
              ? SizedBox(
                  width: BeamIconSizes.small,
                  height: BeamIconSizes.small,
                  child: CircularProgressIndicator(
                    color: Theme.of(context).extension<BeamThemeExtension>()!.primaryBackgroundTextColor,
                  ),
                )
              : const Icon(Icons.play_arrow),
          label: StreamBuilder(
              stream: playgroundController.executionTime,
              builder: (context, AsyncSnapshot<int> state) {
                final seconds = (state.data ?? 0) / kMsToSec;
                final runText = 'widgets.runOrCancelButton.titles.run'.tr();
                final cancelText = 'widgets.runOrCancelButton.titles.cancel'.tr();
                final buttonText = isRunning ? cancelText : runText;
                if (seconds > 0) {
                  return Text(
                    '$buttonText (${seconds.toStringAsFixed(kSecondsFractions)} s)',
                  );
                }
                return Text(buttonText);
              }),
          onPressed: onPressHandler(),
        ),
      ),
    );
  }

  onPressHandler() {
    if (disabled) {
      return null;
    }
    return !isRunning ? runCode : cancelRun;
  }
}
