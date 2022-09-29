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
import 'package:playground/modules/editor/components/share_dropdown/share_tabs/example_share_tabs.dart';
import 'package:playground_components/playground_components.dart';

class SnippetSaveAndShareTabs extends StatelessWidget {
  final PlaygroundController playgroundController;
  final TabController tabController;

  const SnippetSaveAndShareTabs({
    super.key,
    required this.playgroundController,
    required this.tabController,
  });

  @override
  Widget build(BuildContext context) {
    return FutureBuilder(
      future: playgroundController.getSnippetId(),
      builder: (context, snapshot) {
        if (!snapshot.hasData) {
          return const LoadingIndicator();
        }

        return ExampleShareTabs(
          examplePath: snapshot.data.toString(),
          tabController: tabController,
        );
      },
    );
  }
}
