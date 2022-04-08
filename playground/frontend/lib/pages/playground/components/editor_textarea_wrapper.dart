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
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/modules/editor/components/editor_textarea.dart';
import 'package:playground/modules/editor/components/run_button.dart';
import 'package:playground/modules/examples/components/description_popover/description_popover_button.dart';
import 'package:playground/modules/examples/components/multifile_popover/multifile_popover_button.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/notifications/components/notification.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:playground/utils/analytics_utils.dart';
import 'package:provider/provider.dart';

class CodeTextAreaWrapper extends StatelessWidget {
  const CodeTextAreaWrapper({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundState>(builder: (context, state, child) {
      if (state.result?.errorMessage?.isNotEmpty ?? false) {
        WidgetsBinding.instance?.addPostFrameCallback((_) {
          _handleError(context, state);
        });
      }
      return Column(
        key: ValueKey(EditorKeyObject(
          state.sdk,
          state.selectedExample,
          state.resetKey,
        )),
        children: [
          Expanded(
            child: Stack(
              children: [
                Positioned.fill(
                  child: EditorTextArea(
                    enabled: !(state.selectedExample?.isMultiFile ?? false),
                    example: state.selectedExample,
                    sdk: state.sdk,
                    onSourceChange: state.setSource,
                    isEditable: true,
                  ),
                ),
                Positioned(
                  right: kXlSpacing,
                  top: kXlSpacing,
                  height: kButtonHeight,
                  child: Row(
                    children: [
                      if (state.selectedExample != null) ...[
                        if (state.selectedExample?.isMultiFile ?? false)
                          Semantics(
                            container: true,
                            child: MultifilePopoverButton(
                              example: state.selectedExample!,
                              followerAnchor: Alignment.topRight,
                              targetAnchor: Alignment.bottomRight,
                            ),
                          ),
                        Semantics(
                          container: true,
                          child: DescriptionPopoverButton(
                            example: state.selectedExample!,
                            followerAnchor: Alignment.topRight,
                            targetAnchor: Alignment.bottomRight,
                          ),
                        ),
                      ],
                      Semantics(
                        container: true,
                        child: RunButton(
                          disabled: state.selectedExample?.isMultiFile ?? false,
                          isRunning: state.isCodeRunning,
                          cancelRun: () {
                            state.cancelRun().catchError(
                                  (_) => NotificationManager.showError(
                                    context,
                                    AppLocalizations.of(context)!.runCode,
                                    AppLocalizations.of(context)!.cancelExecution,
                                  ),
                                );
                          },
                          runCode: () {
                            AnalyticsService analyticsService =
                                AnalyticsService.get(context);
                            final stopwatch = Stopwatch()..start();
                            final exampleName = getAnalyticsExampleName(
                              state.selectedExample,
                              state.isExampleChanged,
                              state.sdk,
                            );
                            state.runCode(
                              onFinish: () {
                                analyticsService.trackRunTimeEvent(
                                  exampleName,
                                  stopwatch.elapsedMilliseconds,
                                );
                              },
                            );
                            AnalyticsService.get(context)
                                .trackClickRunEvent(exampleName);
                          },
                        ),
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ],
      );
    });
  }

  _handleError(BuildContext context, PlaygroundState state) {
    NotificationManager.showError(
      context,
      AppLocalizations.of(context)!.runCode,
      state.result?.errorMessage ?? '',
    );
    state.resetError();
  }
}

class EditorKeyObject {
  final SDK sdk;
  final ExampleModel? example;
  final DateTime? resetKey;

  const EditorKeyObject(this.sdk, this.example, this.resetKey);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is EditorKeyObject &&
          runtimeType == other.runtimeType &&
          sdk == other.sdk &&
          example == other.example &&
          resetKey == other.resetKey;

  @override
  int get hashCode => hashValues(sdk, example, resetKey);
}
