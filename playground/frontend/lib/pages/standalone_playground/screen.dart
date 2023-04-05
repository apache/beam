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
import 'package:playground_components/playground_components.dart';

import '../../components/logo/logo_component.dart';
import '../../constants/sizes.dart';
import '../../modules/actions/components/new_example_action.dart';
import '../../modules/actions/components/reset_action.dart';
import '../../modules/analytics/analytics_service.dart';
import '../../modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown.dart';
import '../../modules/examples/example_selector.dart';
import '../../modules/sdk/components/sdk_selector.dart';
import '../../modules/shortcuts/components/shortcuts_manager.dart';
import 'state.dart';
import 'widgets/close_listener_nonweb.dart'
    if (dart.library.html) 'widgets/close_listener.dart';
import 'widgets/more_actions.dart';
import 'widgets/playground_page_body.dart';
import 'widgets/playground_page_footer.dart';
import 'widgets/playground_page_providers.dart';

class StandalonePlaygroundScreen extends StatelessWidget {
  // TODO: calculate sum of widths of all app bar buttons at the first frame.
  // https://github.com/apache/beam/issues/25524
  // To get value manually print window width and check when app bar buttons
  // will span 2 lines
  static const kAppBarButtonsWidth = 1105;
  final StandalonePlaygroundNotifier notifier;

  const StandalonePlaygroundScreen(this.notifier);

  @override
  Widget build(BuildContext context) {
    return CloseListener(
      child: PlaygroundPageProviders(
        playgroundController: notifier.playgroundController,
        child: PlaygroundShortcutsManager(
          playgroundController: notifier.playgroundController,
          child: AnimatedBuilder(
            animation: notifier.playgroundController,
            builder: (context, child) {
              final snippetController =
                  notifier.playgroundController.snippetEditingController;

              return Scaffold(
                appBar: AppBar(
                  toolbarHeight:
                      MediaQuery.of(context).size.width > kAppBarButtonsWidth
                          ? kToolbarHeight
                          : 2 * kToolbarHeight,
                  automaticallyImplyLeading: false,
                  title: Wrap(
                    crossAxisAlignment: WrapCrossAlignment.center,
                    spacing: kXlSpacing,
                    children: [
                      const Logo(),
                      AnimatedBuilder(
                        animation: notifier.playgroundController.exampleCache,
                        builder: (context, child) => ExampleSelector(
                          isSelectorOpened: notifier.playgroundController
                              .exampleCache.isSelectorOpened,
                          playgroundController: notifier.playgroundController,
                        ),
                      ),
                      SDKSelector(
                        value: notifier.playgroundController.sdk,
                        onChanged: (newSdk) {
                          AnalyticsService.get(context).trackSelectSdk(
                              notifier.playgroundController.sdk, newSdk);
                          notifier.playgroundController.setSdk(newSdk);
                        },
                      ),
                      if (snippetController != null)
                        PipelineOptionsDropdown(
                          pipelineOptions: snippetController.pipelineOptions,
                          setPipelineOptions:
                              notifier.playgroundController.setPipelineOptions,
                        ),
                      const NewExampleAction(),
                      const ResetAction(),
                    ],
                  ),
                  actions: [
                    const ToggleThemeButton(),
                    MoreActions(
                      playgroundController: notifier.playgroundController,
                    ),
                  ],
                ),
                body: Column(
                  children: [
                    const Expanded(child: PlaygroundPageBody()),
                    Semantics(
                      container: true,
                      child: const PlaygroundPageFooter(),
                    ),
                  ],
                ),
              );
            },
          ),
        ),
      ),
    );
  }
}
