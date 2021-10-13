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
import 'package:playground/components/toggle_theme_button/toggle_theme_button.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/pages/playground/components/playground_page_body.dart';
import 'package:playground/pages/playground/components/playground_page_footer.dart';
import 'package:playground/pages/playground/components/playground_page_providers.dart';
import 'package:playground/modules/actions/components/new_example_action.dart';
import 'package:playground/modules/actions/components/reset_action.dart';
import 'package:playground/pages/playground/components/more_actions.dart';
import 'package:provider/provider.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:playground/modules/sdk/components/sdk_selector.dart';
import 'package:playground/components/logo/logo_component.dart';

class PlaygroundPage extends StatelessWidget {
  const PlaygroundPage({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return PlaygroundPageProviders(
      child: Scaffold(
        appBar: AppBar(
          title: Consumer<PlaygroundState>(
            builder: (context, state, child) {
              return Wrap(
                crossAxisAlignment: WrapCrossAlignment.center,
                spacing: kLgSpacing,
                children: [
                  const Logo(),
                  SDKSelector(sdk: state.sdk, setSdk: state.setSdk),
                  const NewExampleAction(),
                  ResetAction(reset: state.reset),
                ],
              );
            },
          ),
          actions: const [ToggleThemeButton(), MoreActions()],
        ),
        body: Column(
          children: const [
            Expanded(child: PlaygroundPageBody()),
            PlaygroundPageFooter(),
          ],
        ),
      ),
    );
  }
}
