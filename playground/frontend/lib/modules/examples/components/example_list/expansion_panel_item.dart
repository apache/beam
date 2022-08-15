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
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/modules/examples/components/example_list/example_item_actions.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/pages/playground/states/examples_state.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

class ExpansionPanelItem extends StatelessWidget {
  final ExampleModel example;
  final ExampleModel selectedExample;
  final AnimationController animationController;
  final OverlayEntry? dropdown;

  const ExpansionPanelItem({
    Key? key,
    required this.example,
    required this.selectedExample,
    required this.animationController,
    required this.dropdown,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundState>(
      builder: (context, playgroundState, child) => MouseRegion(
        cursor: SystemMouseCursors.click,
        child: GestureDetector(
          onTap: () async {
            if (playgroundState.selectedExample != example) {
              _closeDropdown(playgroundState.exampleState);
              AnalyticsService.get(context).trackSelectExample(example);
              final exampleWithInfo =
                  await playgroundState.exampleState.loadExampleInfo(example);
              playgroundState.setExample(exampleWithInfo);
            }
          },
          child: Container(
            color: Colors.transparent,
            margin: const EdgeInsets.only(left: kXxlSpacing),
            height: kContainerHeight,
            child: Padding(
              padding: const EdgeInsets.only(right: kLgSpacing),
              child: Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  // Wrapped with Row for better user interaction and positioning
                  Text(
                    example.name,
                    style: example == selectedExample
                        ? const TextStyle(fontWeight: FontWeight.bold)
                        : const TextStyle(),
                  ),
                  ExampleItemActions(parentContext: context, example: example),
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }

  void _closeDropdown(ExampleState exampleState) {
    animationController.reverse();
    dropdown?.remove();
    exampleState.changeSelectorVisibility();
  }
}
