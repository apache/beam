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
import 'package:playground/components/loading_indicator/loading_indicator.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/editor/components/share_dropdown/share_tabs/example_share_tabs.dart';
import 'package:playground/modules/examples/repositories/models/shared_file_model.dart';
import 'package:playground/pages/playground/states/playground_state.dart';

class SnippetSaveAndShareTabs extends StatelessWidget {
  final PlaygroundState playgroundState;
  final TabController tabController;

  const SnippetSaveAndShareTabs({
    super.key,
    required this.playgroundState,
    required this.tabController,
  });

  @override
  Widget build(BuildContext context) {
    return FutureBuilder(
      future: playgroundState.exampleState.getSnippetId(
        files: [SharedFile(code: playgroundState.source, isMain: true)],
        sdk: playgroundState.sdk,
        pipelineOptions: playgroundState.pipelineOptions,
      ),
      builder: (context, snapshot) {
        if (!snapshot.hasData) {
          return const LoadingIndicator(size: kLgLoadingIndicatorSize);
        }

        return ExampleShareTabs(
          examplePath: snapshot.data.toString(),
          tabController: tabController,
        );
      },
    );
  }
}
