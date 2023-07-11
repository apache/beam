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
import 'package:provider/provider.dart';

import 'example_share_tabs.dart';
import 'snippet_save_and_share_tabs.dart';

/// Checks if the playground code is saved yet and shows the specific
/// content widget accordingly.
// TODO(alexeyinkin): Refactor code sharing, https://github.com/apache/beam/issues/24637
class ShareTabs extends StatelessWidget {
  const ShareTabs({
    super.key,
    required this.eventSnippetContext,
    required this.onError,
    required this.tabController,
  });

  /// The [EventSnippetContext] at the time when the sharing button was clicked.
  final EventSnippetContext eventSnippetContext;

  final VoidCallback onError;
  final TabController tabController;

  @override
  Widget build(BuildContext context) {
    return Container(
      color: Theme.of(context).backgroundColor,
      child: Consumer<PlaygroundController>(
        builder: (context, playgroundController, _) {
          final controller =
              playgroundController.requireSnippetEditingController();

          if (controller.shouldSaveBeforeSharing()) {
            return SnippetSaveAndShareTabs(
              eventSnippetContext: eventSnippetContext,
              onError: onError,
              playgroundController: playgroundController,
              sdk: controller.sdk,
              tabController: tabController,
            );
          }

          return ExampleShareTabs(
            descriptor: controller.descriptor!,
            eventSnippetContext: eventSnippetContext,
            sdk: controller.sdk,
            tabController: tabController,
          );
        },
      ),
    );
  }
}
