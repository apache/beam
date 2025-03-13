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

import '../../modules/shortcuts/components/shortcuts_manager.dart';
import '../standalone_playground/widgets/playground_page_providers.dart';
import 'state.dart';
import 'widgets/embedded_actions.dart';
import 'widgets/embedded_appbar_title.dart';
import 'widgets/embedded_editor.dart';
import 'widgets/embedded_split_view.dart';

const kActionsWidth = 300.0;
const kActionsHeight = 40.0;

class EmbeddedPlaygroundScreen extends StatelessWidget {
  final EmbeddedPlaygroundNotifier notifier;

  const EmbeddedPlaygroundScreen(this.notifier);

  @override
  Widget build(BuildContext context) {
    return PlaygroundPageProviders(
      playgroundController: notifier.playgroundController,
      child: PlaygroundShortcutsManager(
        playgroundController: notifier.playgroundController,
        child: Scaffold(
          appBar: AppBar(
            automaticallyImplyLeading: false,
            title: const EmbeddedAppBarTitle(),
            actions: const [EmbeddedActions()],
          ),
          body: EmbeddedSplitView(
            first: EmbeddedEditor(isEditable: notifier.isEditable),
            second: Container(
              color: Theme.of(context).backgroundColor,
              child: OutputWidget(
                playgroundController: notifier.playgroundController,
                graphDirection: Axis.horizontal,
              ),
            ),
          ),
        ),
      ),
    );
  }
}
