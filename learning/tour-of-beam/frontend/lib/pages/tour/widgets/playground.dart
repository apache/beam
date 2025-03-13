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
import 'package:flutter/widgets.dart';
import 'package:playground_components/playground_components.dart';

import '../state.dart';

class PlaygroundWidget extends StatelessWidget {
  final TourNotifier tourNotifier;

  const PlaygroundWidget({
    required this.tourNotifier,
  });

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: tourNotifier.playgroundController,
      builder: _buildOnChange,
    );
  }

  Widget _buildOnChange(BuildContext context, Widget? child) {
    final playgroundController = tourNotifier.playgroundController;
    if (playgroundController.codeRunner.result?.errorMessage?.isNotEmpty ??
        false) {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        _handleError(context, playgroundController);
      });
    }

    final snippetController = playgroundController.snippetEditingController;
    if (snippetController == null) {
      return const LoadingIndicator();
    }

    return Stack(
      children: [
        SplitView(
          direction: Axis.vertical,
          first: SnippetEditor(
            autofocus: true,
            controller: snippetController,
            isEditable: true,
          ),
          second: OutputWidget(
            playgroundController: playgroundController,
            graphDirection: Axis.horizontal,
          ),
        ),
        Positioned(
          top: 30,
          right: 30,
          child: Row(
            children: [
              if (playgroundController.codeRunner.canRun)
                RunOrCancelButton(
                  playgroundController: playgroundController,
                  beforeCancel: (runner) {
                    PlaygroundComponents.analyticsService.sendUnawaited(
                      RunCancelledAnalyticsEvent(
                        snippetContext: runner.eventSnippetContext!,
                        duration: runner.elapsed!,
                        trigger: EventTrigger.click,
                        additionalParams: tourNotifier.tobEventContext.toJson(),
                      ),
                    );
                  },
                  beforeRun: () {
                    PlaygroundComponents.analyticsService.sendUnawaited(
                      RunStartedAnalyticsEvent(
                        snippetContext: tourNotifier.playgroundController
                            .codeRunner.eventSnippetContext,
                        trigger: EventTrigger.click,
                        additionalParams: tourNotifier.tobEventContext.toJson(),
                      ),
                    );
                  },
                  onComplete: (runner) {
                    PlaygroundComponents.analyticsService.sendUnawaited(
                      RunFinishedAnalyticsEvent(
                        snippetContext: runner.eventSnippetContext!,
                        duration: runner.elapsed!,
                        trigger: EventTrigger.click,
                        additionalParams: tourNotifier.tobEventContext.toJson(),
                      ),
                    );
                  },
                ),
            ],
          ),
        ),
      ],
    );
  }

  void _handleError(BuildContext context, PlaygroundController controller) {
    //TODO(alexeyinkin): A better trigger instead of resetErrorMessageText, https://github.com/apache/beam/issues/26319
    PlaygroundComponents.toastNotifier.add(
      Toast(
        description: controller.codeRunner.result?.errorMessage ?? '',
        title: 'intents.playground.run'.tr(),
        type: ToastType.error,
      ),
    );
    controller.resetErrorMessageText();
  }
}
