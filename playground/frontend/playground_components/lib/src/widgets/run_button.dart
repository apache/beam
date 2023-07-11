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
import '../models/run_shortcut.dart';
import '../theme/theme.dart';
import 'periodic_builder.dart';
import 'shortcut_tooltip.dart';

class RunButton extends StatelessWidget {
  final PlaygroundController playgroundController;
  final VoidCallback runCode;
  final VoidCallback cancelRun;
  final bool isEnabled;

  const RunButton({
    super.key,
    required this.playgroundController,
    required this.runCode,
    required this.cancelRun,
    this.isEnabled = true,
  });

  static const _buttonTextRebuildInterval = 100;
  static const _width = 150.0;

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: playgroundController.codeRunner,
      builder: (context, child) {
        final isRunning = playgroundController.codeRunner.isCodeRunning;
        final runStartDate = playgroundController.codeRunner.runStartDate;
        final runStopDate = playgroundController.codeRunner.runStopDate;

        return SizedBox(
          width: _width,
          height: BeamSizes.buttonHeight,
          child: ShortcutTooltip(
            shortcut: BeamMainRunShortcut(
              onInvoke: () {}, // Only the tooltip is used.
            ),
            child: ElevatedButton.icon(
              style: const ButtonStyle(
                padding: MaterialStatePropertyAll(EdgeInsets.zero),
              ),
              icon: isRunning
                  ? SizedBox(
                      width: BeamIconSizes.small,
                      height: BeamIconSizes.small,
                      child: CircularProgressIndicator(
                        color: Theme.of(context)
                            .extension<BeamThemeExtension>()!
                            .primaryBackgroundTextColor,
                      ),
                    )
                  : const Icon(Icons.play_arrow),
              label: isRunning
                  // TODO(nausharipov): fix bug
                  // It is also rebuilt on every codeRunner notification
                  ? PeriodicBuilderWidget(
                      interval: const Duration(
                        milliseconds: _buttonTextRebuildInterval,
                      ),
                      builder: () {
                        return _ButtonText(
                          isRunning: isRunning,
                          runStartDate: runStartDate,
                          runStopDate: runStopDate,
                        );
                      },
                    )
                  : _ButtonText(
                      isRunning: isRunning,
                      runStartDate: runStartDate,
                      runStopDate: runStopDate,
                    ),
              onPressed: isEnabled ? _onPressed() : null,
            ),
          ),
        );
      },
    );
  }

  VoidCallback _onPressed() {
    return playgroundController.codeRunner.isCodeRunning ? cancelRun : runCode;
  }
}

class _ButtonText extends StatelessWidget {
  final bool isRunning;
  final DateTime? runStartDate;
  final DateTime? runStopDate;

  const _ButtonText({
    required this.isRunning,
    required this.runStartDate,
    required this.runStopDate,
  });

  static const _msToSec = 1000;
  static const _secondsFractionDigits = 1;

  @override
  Widget build(BuildContext context) {
    final runText = 'widgets.runOrCancelButton.titles.run'.tr();
    final cancelText = 'widgets.runOrCancelButton.titles.cancel'.tr();
    final buttonText = isRunning ? cancelText : runText;
    final runStopDateOrNow = runStopDate ?? DateTime.now();

    final elapsedDuration =
        runStopDateOrNow.difference(runStartDate ?? DateTime.now());

    if (elapsedDuration.inMilliseconds > 0) {
      final seconds = elapsedDuration.inMilliseconds / _msToSec;
      return Text(
        '$buttonText (${seconds.toStringAsFixed(_secondsFractionDigits)} s)',
      );
    }
    return Text(buttonText);
  }
}
