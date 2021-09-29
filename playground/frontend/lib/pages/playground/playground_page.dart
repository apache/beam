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
import 'package:provider/provider.dart';
import 'package:playground/pages/playground/components/editor_textarea_wrapper.dart';
import 'package:playground/modules/output/components/output_area.dart';
import 'package:playground/pages/playground/playground_state.dart';
import 'package:playground/modules/sdk/components/sdk_selector.dart';
import 'package:playground/components/logo/logo_component.dart';

class PlaygroundPage extends StatelessWidget {
  const PlaygroundPage({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return ChangeNotifierProvider<PlaygroundState>(
      create: (context) => PlaygroundState(),
      child: Scaffold(
        appBar: AppBar(
          title: Wrap(
            crossAxisAlignment: WrapCrossAlignment.center,
            spacing: 16.0,
            children: [
              const Logo(),
              Consumer<PlaygroundState>(
                builder: (context, state, child) {
                  return SDKSelector(
                    sdk: state.sdk,
                    setSdk: state.setSdk,
                  );
                },
              ),
            ],
          ),
          actions: const [ToggleThemeButton()],
        ),
        body: Column(
          children: [
            const Expanded(child: CodeTextAreaWrapper()),
            Container(height: 16.0, color: Theme.of(context).backgroundColor),
            const Expanded(child: OutputArea()),
          ],
        ),
      ),
    );
  }
}
