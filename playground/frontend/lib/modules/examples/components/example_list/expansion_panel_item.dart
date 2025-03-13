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

import '../../../../constants/sizes.dart';
import '../../../../services/analytics/events/snippet_selected.dart';
import 'example_item_actions.dart';

/// An [example] in the example dropdown.
class ExpansionPanelItem extends StatelessWidget {
  final ExampleBase example;
  final VoidCallback onSelected;
  final ExampleBase? selectedExample;

  const ExpansionPanelItem({
    Key? key,
    required this.example,
    required this.onSelected,
    required this.selectedExample,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundController>(
      builder: (context, controller, child) => MouseRegion(
        cursor: SystemMouseCursors.click,
        child: GestureDetector(
          onTap: () {
            if (controller.selectedExample != example) {
              _closeDropdown(controller.exampleCache);
              PlaygroundComponents.analyticsService.sendUnawaited(
                SnippetSelectedAnalyticsEvent(
                  sdk: example.sdk,
                  snippet: example.path,
                ),
              );
              controller.setExampleBase(example);
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
                  Text(
                    example.name,
                    style: example.path == selectedExample?.path
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

  void _closeDropdown(ExampleCache exampleCache) {
    exampleCache.setSelectorOpened(false);
    onSelected();
  }
}
