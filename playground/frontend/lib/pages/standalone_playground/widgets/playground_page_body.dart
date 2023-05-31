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

import '../../../modules/output/components/output_header/output_placements.dart';
import '../../../modules/output/models/output_placement.dart';
import '../../../modules/output/models/output_placement_state.dart';
import 'editor_textarea_wrapper.dart';

class PlaygroundPageBody extends StatelessWidget {
  const PlaygroundPageBody({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer2<OutputPlacementState, PlaygroundController>(
        builder: (context, outputState, controller, child) {
      final snippetController = controller.snippetEditingController;

      if (snippetController == null || snippetController.isLoading) {
        return const LoadingIndicator();
      }

      final output = OutputWidget(
        graphDirection: outputState.placement.graphDirection,
        playgroundController: controller,
        trailing: const OutputPlacements(),
      );

      final codeTextArea = CodeTextAreaWrapper(
        playgroundController: controller,
      );

      switch (outputState.placement) {
        case OutputPlacement.bottom:
          return SplitView(
            direction: Axis.vertical,
            first: codeTextArea,
            second: output,
          );

        case OutputPlacement.left:
          return SplitView(
            direction: Axis.horizontal,
            first: output,
            second: codeTextArea,
          );

        case OutputPlacement.right:
          return SplitView(
            direction: Axis.horizontal,
            first: codeTextArea,
            second: output,
          );
      }
    });
  }
}
