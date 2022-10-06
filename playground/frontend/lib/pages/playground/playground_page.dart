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
import 'package:playground/components/logo/logo_component.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/actions/components/new_example_action.dart';
import 'package:playground/modules/actions/components/reset_action.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown.dart';
import 'package:playground/modules/examples/example_selector.dart';
import 'package:playground/modules/sdk/components/sdk_selector.dart';
import 'package:playground/modules/shortcuts/components/shortcuts_manager.dart';
import 'package:playground/modules/shortcuts/constants/global_shortcuts.dart';
import 'package:playground/pages/playground/components/close_listener_nonweb.dart'
    if (dart.library.html) 'package:playground/pages/playground/components/close_listener.dart';
import 'package:playground/pages/playground/components/more_actions.dart';
import 'package:playground/pages/playground/components/playground_page_body.dart';
import 'package:playground/pages/playground/components/playground_page_footer.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

class PlaygroundPage extends StatelessWidget {
  const PlaygroundPage({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return CloseListener(
      child: Consumer<PlaygroundController>(
        builder: (context, controller, child) {
          final snippetController = controller.snippetEditingController;

          return ShortcutsManager(
            shortcuts: [
              ...controller.shortcuts,
              ...globalShortcuts,
            ],
            child: Scaffold(
              appBar: AppBar(
                automaticallyImplyLeading: false,
                title: Wrap(
                  crossAxisAlignment: WrapCrossAlignment.center,
                  spacing: kXlSpacing,
                  children: [
                    const Logo(),
                    ExampleSelector(
                      changeSelectorVisibility:
                          controller.exampleCache.changeSelectorVisibility,
                      isSelectorOpened:
                          controller.exampleCache.isSelectorOpened,
                    ),
                    SDKSelector(
                      value: controller.sdk,
                      onChanged: (newSdk) {
                        AnalyticsService.get(context)
                            .trackSelectSdk(controller.sdk, newSdk);
                        controller.setSdk(newSdk);
                      },
                    ),
                    if (snippetController != null)
                      PipelineOptionsDropdown(
                        pipelineOptions: snippetController.pipelineOptions,
                        setPipelineOptions: controller.setPipelineOptions,
                      ),
                    const NewExampleAction(),
                    const ResetAction(),
                  ],
                ),
                actions: [
                  const ToggleThemeButton(),
                  MoreActions(
                    playgroundController: controller,
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
            ),
          );
        },
      ),
    );
  }
}
