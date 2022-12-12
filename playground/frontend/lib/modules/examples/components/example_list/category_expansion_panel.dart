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

import 'dart:math' as math;

import 'package:expansion_widget/expansion_widget.dart';
import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';

import '../../../../constants/sizes.dart';
import 'expansion_panel_item.dart';

/// An expandable list of [examples] for the given [categoryName]
/// to be shown in the example dropdown.
class CategoryExpansionPanel extends StatelessWidget {
  final String categoryName;
  final List<ExampleBase> examples;
  final VoidCallback onSelected;
  final ExampleBase? selectedExample;

  const CategoryExpansionPanel({
    required this.categoryName,
    required this.examples,
    required this.onSelected,
    required this.selectedExample,
  });

  @override
  Widget build(BuildContext context) {
    return ExpansionWidget(
      initiallyExpanded: examples.any((e) => e.path == selectedExample?.path),
      maintainState: true,
      titleBuilder: (animationValue, _, isExpanded, toggleFunction) => InkWell(
        onTap: () => toggleFunction(animated: true),
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: kLgSpacing),
          child: Row(
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Transform.rotate(
                angle: -math.pi * animationValue,
                alignment: Alignment.center,
                child: const Icon(Icons.keyboard_arrow_down),
              ),
              SizedBox(
                height: kContainerHeight,
                child: Align(
                  alignment: Alignment.centerLeft,
                  child: Text(categoryName),
                ),
              ),
            ],
          ),
        ),
      ),
      content: Column(
        children: buildItems(),
      ),
    );
  }

  List<Widget> buildItems() {
    List<Widget> items = [];
    for (var example in examples) {
      items.add(
        ExpansionPanelItem(
          example: example,
          onSelected: onSelected,
          selectedExample: selectedExample,
        ),
      );
    }
    return items;
  }
}
